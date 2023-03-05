package copy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/registry"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type manifest struct {
	ContentType string
	Body        json.RawMessage
}

func uploadManifest(img image.Image, manifest manifest) error {
	client, err := registry.GetClient(img.Repository, registry.PushScope)
	if err != nil {
		return err
	}

	reference := string(img.Digest)
	if reference == "" {
		reference = img.Tag
	}

	u := img.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", img.Namespace, reference)
	req, err := http.NewRequest(http.MethodPut, u.String(), bytes.NewReader(manifest.Body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", manifest.ContentType)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return transport.CheckError(resp, http.StatusCreated)
}

type manifestCache struct {
	*work.Queue[image.Image, manifest]
}

func newManifestCache(workers int) *manifestCache {
	d := &manifestCache{}
	d.Queue = work.NewQueue(workers, d.handleRequest)
	return d
}

func (d *manifestCache) Get(img image.Image) (manifest, error) {
	return d.Queue.GetOrSubmit(img).Wait()
}

var supportedManifestMediaTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

func (d *manifestCache) handleRequest(img image.Image) (resp manifest, err error) {
	reference := string(img.Digest)
	if reference == "" {
		reference = img.Tag
	}

	log.Printf("[manifest]\tdownloading %s", img)

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
	downloadReq.Header.Add("Accept", strings.Join(supportedManifestMediaTypes, ","))

	downloadResp, err := client.Do(downloadReq)
	if err != nil {
		return
	}
	defer downloadResp.Body.Close()
	err = transport.CheckError(downloadResp, http.StatusOK)
	if err != nil {
		return
	}

	contentType := downloadResp.Header.Get("Content-Type")
	body, err := io.ReadAll(downloadResp.Body)
	return manifest{ContentType: contentType, Body: body}, err
}
