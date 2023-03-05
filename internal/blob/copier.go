package blob

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/engine"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

type Copier struct {
	engine *engine.Engine[Request, engine.NoValue]

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
	c.engine = engine.NewEngine(workers, engine.NoValueFunc(c.handleRequest))
	return c
}

func (c *Copier) RequestCopy(dgst image.Digest, from, to image.Repository) CopyTask {
	c.sources(dgst).Add(from)
	return CopyTask{c.engine.GetOrSubmit(Request{Digest: dgst, To: to})}
}

type CopyTask struct {
	*engine.Task[engine.NoValue]
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

func (c *Copier) handleRequest(req Request) error {
	sourceSet := c.sources(req.Digest)
	if sourceSet.Contains(req.To) {
		log.Printf("[blob] %s known to contain %s", req.To, req.Digest)
		return nil
	}

	hasBlob, err := checkForExistingBlob(req.To, req.Digest)
	if err != nil {
		return err
	}
	if hasBlob {
		log.Printf("[blob] %s found to contain %s", req.To, req.Digest)
		return nil
	}

	var mountSource image.Repository
	sources := sourceSet.ToSlice()
	for _, source := range sources {
		if source.Registry == req.To.Registry {
			log.Printf("[blob] %s found mount candidate %s for %s", req.To, source, req.Digest)
			mountSource = source
			break
		}
	}

	uploadURL, mounted, err := requestBlobUploadURL(req.To, req.Digest, mountSource.Namespace)
	if err != nil {
		return err
	}
	if mounted {
		log.Printf("[blob] %s successfully mounted %s from %s", req.To, req.Digest, mountSource)
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

	client, err := getRegistryClient(req.To.Registry, transport.PushScope)
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

	log.Printf("[blob] %s successfully copied %s from %s", req.To, req.Digest, sources[0])
	c.sources(req.Digest).Add(req.To)
	return nil
}

func checkForExistingBlob(repo image.Repository, dgst image.Digest) (bool, error) {
	client, err := getRegistryClient(repo.Registry, transport.PullScope)
	if err != nil {
		return false, err
	}

	u := getBaseURL(repo.Registry)
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
	client, err := getRegistryClient(repo.Registry, transport.PullScope)
	if err != nil {
		return nil, 0, err
	}

	u := getBaseURL(repo.Registry)
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
	client, err := getRegistryClient(repo.Registry, transport.PushScope)
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

	u := getBaseURL(repo.Registry)
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

func getBaseURL(reg image.Registry) *url.URL {
	// TODO: Use go-containerregistry logic for this.
	scheme := "https"
	if strings.HasPrefix(string(reg), "localhost:") {
		scheme = "http"
	}
	return &url.URL{
		Scheme: scheme,
		Host:   string(reg),
	}
}

func getRegistryClient(registry image.Registry, scope string) (http.Client, error) {
	transport, err := getRegistryTransport(registry, scope)
	client := http.Client{Transport: transport}
	return client, err
}

func getRegistryTransport(registry image.Registry, scope string) (http.RoundTripper, error) {
	gRegistry, err := name.NewRegistry(string(registry))
	if err != nil {
		return nil, err
	}
	authenticator, err := authn.DefaultKeychain.Resolve(gRegistry)
	if err != nil {
		authenticator = authn.Anonymous
	}
	return transport.NewWithContext(
		context.TODO(),
		gRegistry,
		authenticator,
		http.DefaultTransport,
		[]string{gRegistry.Scope(scope)},
	)
}
