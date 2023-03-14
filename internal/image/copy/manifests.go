package copy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/registry"
	"go.alexhamlin.co/magic-mirror/internal/log"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

func uploadManifest(img image.Image, manifest image.ManifestKind) error {
	client, err := registry.GetClient(img.Repository, registry.PushScope)
	if err != nil {
		return err
	}

	reference := img.Tag
	if reference == "" {
		reference = img.Digest.String()
	}
	if reference == "" {
		reference = manifest.Descriptor().Digest.String()
	}

	u := img.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", img.Namespace, reference)
	req, err := http.NewRequest(http.MethodPut, u.String(), bytes.NewReader(manifest.Encoded()))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", manifest.Descriptor().MediaType)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return registry.CheckResponse(resp, http.StatusCreated)
}

type manifestCache struct {
	*work.Queue[image.Image, image.ManifestKind]
}

func newManifestCache(concurrency int) *manifestCache {
	d := &manifestCache{}
	d.Queue = work.NewQueue(concurrency, d.handleRequest)
	return d
}

func (d *manifestCache) Get(img image.Image) (image.ManifestKind, error) {
	return d.Queue.GetOrSubmit(img).Wait()
}

func (d *manifestCache) handleRequest(_ context.Context, img image.Image) (resp image.ManifestKind, err error) {
	reference := img.Digest.String()
	if reference == "" {
		reference = img.Tag
	}

	log.Verbosef("[manifest]\tdownloading %s", img)

	client, err := registry.GetClient(img.Repository, registry.PullScope)
	if err != nil {
		return
	}

	u := img.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", img.Namespace, reference)
	downloadReq, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return
	}
	downloadReq.Header.Add("Accept", strings.Join(image.AllManifestMediaTypes, ","))

	downloadResp, err := client.Do(downloadReq)
	if err != nil {
		return
	}
	defer downloadResp.Body.Close()
	err = registry.CheckResponse(downloadResp, http.StatusOK)
	if err != nil {
		return
	}

	contentType := image.MediaType(downloadResp.Header.Get("Content-Type"))
	body, err := io.ReadAll(downloadResp.Body)
	switch {
	case contentType.IsIndex():
		var index image.RawIndex
		err = json.Unmarshal(body, &index)
		resp = index
	case contentType.IsManifest():
		var manifest image.RawManifest
		err = json.Unmarshal(body, &manifest)
		resp = manifest
	default:
		err = fmt.Errorf("unknown manifest type for %s: %s", img, contentType)
	}
	return
}
