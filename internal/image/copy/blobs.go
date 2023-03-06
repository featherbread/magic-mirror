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

	srcsMap map[digest.Digest]mapset.Set[image.Repository]
	srcsMu  sync.Mutex
}

type blobCopyRequest struct {
	Digest digest.Digest
	Dst    image.Repository
}

func newBlobCopier(workers int) *blobCopier {
	c := &blobCopier{
		srcsMap: make(map[digest.Digest]mapset.Set[image.Repository]),
	}
	c.Queue = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *blobCopier) RegisterSource(dgst digest.Digest, src image.Repository) {
	c.sources(dgst).Add(src)
}

func (c *blobCopier) CopyAll(src, dst image.Repository, dgsts ...digest.Digest) error {
	requests := make([]blobCopyRequest, len(dgsts))
	for i, dgst := range dgsts {
		c.RegisterSource(dgst, src)
		requests[i] = blobCopyRequest{Digest: dgst, Dst: dst}
	}
	_, err := c.Queue.GetOrSubmitAll(requests...).WaitAll()
	return err
}

func (c *blobCopier) sources(dgst digest.Digest) mapset.Set[image.Repository] {
	c.srcsMu.Lock()
	defer c.srcsMu.Unlock()
	if set, ok := c.srcsMap[dgst]; ok {
		return set
	}
	set := mapset.NewSet[image.Repository]()
	c.srcsMap[dgst] = set
	return set
}

func (c *blobCopier) handleRequest(req blobCopyRequest) (err error) {
	srcSet := c.sources(req.Digest)
	if srcSet.Contains(req.Dst) {
		log.Printf("[blob]\tknown %s@%s", req.Dst, req.Digest)
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
		log.Printf("[blob]\tfound %s@%s", req.Dst, req.Digest)
		return nil
	}

	var mountSrc image.Repository
	srcs := srcSet.ToSlice()
	for _, src := range srcs {
		if src.Registry == req.Dst.Registry {
			mountSrc = src
			break
		}
	}

	uploadURL, mounted, err := requestBlobUploadURL(req.Dst, req.Digest, mountSrc.Namespace)
	if err != nil {
		return err
	}
	if mounted {
		log.Printf("[blob]\tmounted %s@%s to %s", mountSrc, req.Digest, req.Dst)
		return nil
	}

	blob, size, err := downloadBlob(srcs[0], req.Digest)
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

	log.Printf("[blob]\tcopied %s@%s to %s", srcs[0], req.Digest, req.Dst)
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
