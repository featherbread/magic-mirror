package blob

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/registry"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type Copier struct {
	engine *work.Queue[Request, work.NoValue]

	sourcesMap map[image.Digest]mapset.Set[image.Repository]
	sourcesMu  sync.Mutex
}

type Request struct {
	Digest image.Digest
	To     image.Repository
}

func NewCopier(workers int) *Copier {
	c := &Copier{
		sourcesMap: make(map[image.Digest]mapset.Set[image.Repository]),
	}
	c.engine = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *Copier) RequestCopy(dgst image.Digest, from, to image.Repository) CopyTask {
	c.sources(dgst).Add(from)
	return CopyTask{c.engine.GetOrSubmit(Request{Digest: dgst, To: to})}
}

type CopyTask struct {
	*work.Task[work.NoValue]
}

func (t CopyTask) Wait() error {
	_, err := t.Task.Wait()
	return err
}

func (c *Copier) Close() {
	c.engine.Close()
}

func (c *Copier) sources(dgst image.Digest) mapset.Set[image.Repository] {
	c.sourcesMu.Lock()
	defer c.sourcesMu.Unlock()
	if set, ok := c.sourcesMap[dgst]; ok {
		return set
	}
	set := mapset.NewSet[image.Repository]()
	c.sourcesMap[dgst] = set
	return set
}

func (c *Copier) handleRequest(req Request) (err error) {
	sourceSet := c.sources(req.Digest)
	if sourceSet.Contains(req.To) {
		log.Printf("[blob]\tknown %s@%s", req.To, req.Digest)
		return nil
	}

	defer func() {
		if err == nil {
			c.sources(req.Digest).Add(req.To)
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
	query.Add("digest", string(req.Digest))
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
	if err := transport.CheckError(uploadResp, http.StatusCreated); err != nil {
		return err
	}

	log.Printf("[blob]\tcopied %s@%s to %s", sources[0], req.Digest, req.To)
	return nil
}

func checkForExistingBlob(repo image.Repository, dgst image.Digest) (bool, error) {
	client, err := registry.GetClient(repo, registry.PullScope)
	if err != nil {
		return false, err
	}

	u := registry.GetBaseURL(repo.Registry)
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
	return resp.StatusCode == http.StatusOK, transport.CheckError(resp, http.StatusOK, http.StatusNotFound)
}

func downloadBlob(repo image.Repository, dgst image.Digest) (r io.ReadCloser, size int64, err error) {
	client, err := registry.GetClient(repo, registry.PullScope)
	if err != nil {
		return nil, 0, err
	}

	u := registry.GetBaseURL(repo.Registry)
	u.Path = fmt.Sprintf("/v2/%s/blobs/%s", repo.Namespace, dgst)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if err := transport.CheckError(resp, http.StatusOK); err != nil {
		resp.Body.Close()
		return nil, 0, err
	}

	r = resp.Body
	size, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return
}

func requestBlobUploadURL(repo image.Repository, dgst image.Digest, mountNamespace string) (upload *url.URL, mounted bool, err error) {
	client, err := registry.GetClient(repo, registry.PushScope)
	if err != nil {
		return nil, false, err
	}

	query := make(url.Values)
	if mountNamespace != "" {
		query.Add("mount", string(dgst))
		query.Add("from", mountNamespace)
	} else {
		query.Add("digest", string(dgst))
	}

	u := registry.GetBaseURL(repo.Registry)
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
	if err := transport.CheckError(resp, http.StatusCreated, http.StatusAccepted); err != nil {
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
