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

	"github.com/featherbread/magic-mirror/internal/image"
	"github.com/featherbread/magic-mirror/internal/image/registry"
	"github.com/featherbread/magic-mirror/internal/log"
	"github.com/featherbread/magic-mirror/internal/parka"
)

// blobCopier handles requests to copy blob content between repositories.
type blobCopier struct {
	parka.Set[blobCopyKey]
	copyMu parka.KeyMutex[blobMutexKey]

	sourceMap   map[digest.Digest]mapset.Set[image.Repository]
	sourceMapMu sync.Mutex
}

type blobCopyKey struct {
	Digest digest.Digest
	Dst    image.Repository
}

type blobMutexKey struct {
	Digest   digest.Digest
	Registry image.Registry
}

func newBlobCopier(concurrency int) *blobCopier {
	c := &blobCopier{sourceMap: make(map[digest.Digest]mapset.Set[image.Repository])}
	c.Set = parka.NewSet(c.copyBlob)
	c.Set.Limit(concurrency)
	return c
}

// RegisterSource informs the copier that the provided source repository
// contains the blob with the provided digest. The copier may use this
// information to optimize its copy attempts, by skipping copies of blobs that
// are known to exist at a destination or attempting cross-repository mounts of
// blobs in the same registry.
func (c *blobCopier) RegisterSource(dgst digest.Digest, src image.Repository) {
	c.sources(dgst).Add(src)
}

// CopyAll ensures that all referenced blobs from the source repository exist
// in the destination repository.
//
// The source repository will be registered as a source for the provided blobs.
// Note that the copier may source the blobs for this operation from another
// repository, and may use the provided repository as a source for future
// unrelated copies.
func (c *blobCopier) CopyAll(src, dst image.Repository, dgsts ...digest.Digest) error {
	keys := make([]blobCopyKey, len(dgsts))
	for i, dgst := range dgsts {
		c.RegisterSource(dgst, src)
		keys[i] = blobCopyKey{Digest: dgst, Dst: dst}
	}
	return c.Set.Collect(keys...)
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

func (c *blobCopier) copyBlob(ph *parka.Handle, req blobCopyKey) (err error) {
	// If another handler is copying this blob to the same registry, wait for it
	// to finish so we can do a cross-repository mount instead of pulling from the
	// source again.
	key := blobMutexKey{Digest: req.Digest, Registry: req.Dst.Registry}
	c.copyMu.LockDetached(ph, key)
	defer c.copyMu.Unlock(key)

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

	// Since the source set is a Go map under the hood, we expect Go's random map
	// iteration order to balance the use of different sources when the same blob
	// is copied to multiple destinations.
	allSources := srcSet.ToSlice()
	source := allSources[0]

	// If we have access to another repository on the same destination registry
	// that we know contains this blob, it's cheaper for the registry to copy it
	// locally ("mounting" it) than for us to send it.
	var mountRepo image.Repository
	for _, src := range allSources {
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

	blob, size, err := downloadBlob(source, req.Digest)
	if err != nil {
		return err // TODO: DELETE the upload request to cancel it.
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

	_, err = client.DoExpectingNoBody(uploadReq, http.StatusCreated)
	if err != nil {
		return err
	}

	log.Verbosef("[blob]\tcopied %s@%s to %s", source, req.Digest, req.Dst)
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

	resp, err := client.DoExpectingNoBody(req, http.StatusOK, http.StatusNotFound)
	ok := (err == nil && resp.StatusCode == http.StatusOK)
	return ok, err
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

	resp, err := client.DoExpecting(req, http.StatusOK)
	if err != nil {
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

	resp, err := client.DoExpectingNoBody(req, http.StatusCreated, http.StatusAccepted)
	if err != nil {
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
