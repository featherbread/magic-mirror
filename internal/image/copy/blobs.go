package copy

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/registry"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type blobCopier struct {
	*work.Queue[blobCopyRequest, work.NoValue]

	sourcesMap map[digest.Digest]mapset.Set[image.Repository]
	sourcesMu  sync.Mutex
}

type blobCopyRequest struct {
	Digest digest.Digest
	To     image.Repository
}

func newBlobCopier(workers int) *blobCopier {
	c := &blobCopier{
		sourcesMap: make(map[digest.Digest]mapset.Set[image.Repository]),
	}
	c.Queue = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *blobCopier) RegisterSource(dgst digest.Digest, from image.Repository) {
	c.sources(dgst).Add(from)
}

func (c *blobCopier) CopyAll(from, to image.Repository, dgsts ...digest.Digest) error {
	requests := make([]blobCopyRequest, len(dgsts))
	for i, dgst := range dgsts {
		c.RegisterSource(dgst, from)
		requests[i] = blobCopyRequest{Digest: dgst, To: to}
	}
	_, err := c.Queue.GetOrSubmitAll(requests...).WaitAll()
	return err
}

func (c *blobCopier) sources(dgst digest.Digest) mapset.Set[image.Repository] {
	c.sourcesMu.Lock()
	defer c.sourcesMu.Unlock()
	if set, ok := c.sourcesMap[dgst]; ok {
		return set
	}
	set := mapset.NewSet[image.Repository]()
	c.sourcesMap[dgst] = set
	return set
}

func (c *blobCopier) handleRequest(req blobCopyRequest) (err error) {
	sourceSet := c.sources(req.Digest)
	if sourceSet.Contains(req.To) {
		log.Printf("[blob]\tknown %s@%s", req.To, req.Digest)
		return nil
	}

	defer func() {
		if err == nil {
			c.RegisterSource(req.Digest, req.To)
		}
	}()

	hasBlob, err := checkForExistingBlob(req.To, req.Digest)
	if err != nil {
		return err
	}
	if hasBlob {
		log.Printf("[blob]\tfound %s@%s", req.To, req.Digest)
		return nil
	}

	var mountSource image.Repository
	sources := sourceSet.ToSlice()
	for _, source := range sources {
		if source.Registry == req.To.Registry {
			mountSource = source
			break
		}
	}

	uploadURL, mounted, err := requestBlobUploadURL(req.To, req.Digest, mountSource.Namespace)
	if err != nil {
		return err
	}
	if mounted {
		log.Printf("[blob]\tmounted %s@%s to %s", mountSource, req.Digest, req.To)
		return nil
	}

	blob, size, err := downloadBlob(sources[0], req.Digest)
	if err != nil {
		return err
	}

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

	client, err := registry.GetClient(req.To, registry.PushScope)
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

	log.Printf("[blob]\tcopied %s@%s to %s", sources[0], req.Digest, req.To)
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
