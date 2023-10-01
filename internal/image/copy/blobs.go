package copy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/registry"
	"go.alexhamlin.co/magic-mirror/internal/log"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

// blobCopier handles requests to copy blob content between repositories.
type blobCopier struct {
	*work.Queue[blobCopyRequest, work.NoValue]

	sourceMap   map[digest.Digest]mapset.Set[image.Repository]
	sourceMapMu sync.Mutex

	copyMu work.KeyMutex[digest.Digest]
}

type blobCopyRequest struct {
	Digest digest.Digest
	Dst    image.Repository
}

func newBlobCopier(concurrency int) *blobCopier {
	c := &blobCopier{sourceMap: make(map[digest.Digest]mapset.Set[image.Repository])}
	c.Queue = work.NewQueue(concurrency, work.NoValueHandler(c.copyOneBlob))
	return c
}

// RegisterSource informs the copier that the provided source repository
// contains the blob with the provided digest. The copier may use this
// information to optimize its copy attempts, by skipping copies of blobs that
// are known to exist at a destination or attempting cross-repository mounts of
// blobs within the same registry.
func (c *blobCopier) RegisterSource(dgst digest.Digest, src image.Repository) {
	c.sources(dgst).Add(src)
}

// CopyAll ensures that all of the referenced blobs from the source repository
// exist in the destination repository.
//
// The source repository will be registered as a source for the provided blobs.
// Note that the copier may source the blobs for this operation from another
// repository, and may use the provided repository as a source for future
// unrelated copies.
func (c *blobCopier) CopyAll(src, dst image.Repository, dgsts ...digest.Digest) error {
	requests := make([]blobCopyRequest, len(dgsts))
	for i, dgst := range dgsts {
		c.RegisterSource(dgst, src)
		requests[i] = blobCopyRequest{Digest: dgst, Dst: dst}
	}
	_, err := c.Queue.GetAll(requests...)
	return err
}

func (c *blobCopier) sources(dgst digest.Digest) mapset.Set[image.Repository] {
	c.sourceMapMu.Lock()
	defer c.sourceMapMu.Unlock()
	if set, ok := c.sourceMap[dgst]; ok {
		return set
	}
	set := mapset.NewSet[image.Repository]()
	c.sourceMap[dgst] = set
	return set
}

func (c *blobCopier) copyOneBlob(qh *work.QueueHandle, req blobCopyRequest) (err error) {
	// If another handler is copying this same blob, wait for that handler to
	// finish. If it's copying to the same registry that we are, we'll be able to
	// mount the blob instead of pulling it again from the source.
	c.copyMu.LockDetached(qh, req.Digest)
	defer c.copyMu.Unlock(req.Digest)

	srcSet := c.sources(req.Digest)
	if srcSet.Contains(req.Dst) {
		log.Verbosef("[blob]\tknown %s@%s", req.Dst, req.Digest)
		return nil
	}

	defer func() {
		if err == nil {
			c.RegisterSource(req.Digest, req.Dst)
		}
	}()

	hasBlob, err := checkForExistingBlob(req.Dst, req.Digest)
	if err != nil {
		return err
	}
	if hasBlob {
		log.Verbosef("[blob]\tfound %s@%s", req.Dst, req.Digest)
		return nil
	}

	var mountRepo image.Repository
	sources := srcSet.ToSlice()
	for _, src := range sources {
		if src.Registry == req.Dst.Registry {
			mountRepo = src
			break
		}
	}

	uploadURL, mounted, err := requestBlobUploadURL(req.Dst, req.Digest, mountRepo.Namespace)
	if err != nil {
		return err
	}
	if mounted {
		log.Verbosef("[blob]\tmounted %s@%s to %s", mountRepo, req.Digest, req.Dst)
		return nil
	}

	blob, size, err := downloadBlob(sources[0], req.Digest)
	if err != nil {
		// TODO: DELETE the upload request to cancel it.
		return err
	}
	defer blob.Close()

	query, err := url.ParseQuery(uploadURL.RawQuery)
	if err != nil {
		return err
	}
	query.Add("digest", req.Digest.String())
	uploadURL.RawQuery = query.Encode()

	uploadReq, err := http.NewRequest(http.MethodPut, uploadURL.String(), blob)
	if err != nil {
		return err
	}
	uploadReq.Header.Add("Content-Type", "application/octet-stream")
	uploadReq.Header.Add("Content-Length", strconv.FormatInt(size, 10))

	client, err := registry.GetClient(req.Dst, registry.PushScope)
	if err != nil {
		return err
	}

	uploadResp, err := client.Do(uploadReq)
	if err != nil {
		return err
	}
	defer uploadResp.Body.Close()
	if err := registry.CheckResponse(uploadResp, http.StatusCreated); err != nil {
		return err
	}

	log.Verbosef("[blob]\tcopied %s@%s to %s", sources[0], req.Digest, req.Dst)
	return nil
}

func checkForExistingBlob(repo image.Repository, dgst digest.Digest) (bool, error) {
	client, err := registry.GetClient(repo, registry.PullScope)
	if err != nil {
		return false, err
	}

	u := repo.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/blobs/%s", repo.Namespace, dgst)
	req, err := http.NewRequest(http.MethodHead, u.String(), nil)
	if err != nil {
		return false, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, registry.CheckResponse(resp, http.StatusOK, http.StatusNotFound)
}

func downloadBlob(repo image.Repository, dgst digest.Digest) (r io.ReadCloser, size int64, err error) {
	client, err := registry.GetClient(repo, registry.PullScope)
	if err != nil {
		return nil, 0, err
	}

	u := repo.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/blobs/%s", repo.Namespace, dgst)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if err := registry.CheckResponse(resp, http.StatusOK); err != nil {
		resp.Body.Close()
		return nil, 0, err
	}

	r = resp.Body
	size, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return
}

func requestBlobUploadURL(repo image.Repository, dgst digest.Digest, mountNamespace string) (upload *url.URL, mounted bool, err error) {
	client, err := registry.GetClient(repo, registry.PushScope)
	if err != nil {
		return nil, false, err
	}

	query := make(url.Values)
	if mountNamespace != "" {
		query.Add("mount", dgst.String())
		query.Add("from", mountNamespace)
	} else {
		query.Add("digest", dgst.String())
	}

	u := repo.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/blobs/uploads/", repo.Namespace)
	u.RawQuery = query.Encode()
	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, false, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if err := registry.CheckResponse(resp, http.StatusCreated, http.StatusAccepted); err != nil {
		return nil, false, err
	}

	if resp.StatusCode == http.StatusCreated {
		// The mount was successful.
		return nil, true, nil
	}

	// The mount was not successful, and we need to provide a regular upload URL.
	upload, err = u.Parse(resp.Header.Get("Location"))
	return
}
