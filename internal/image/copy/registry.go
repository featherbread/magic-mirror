package copy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/registry"
)

var supportedManifestMediaTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

func uploadManifest(img image.Image, contentType string, body json.RawMessage) error {
	client, err := registry.GetClient(img.Repository, registry.PushScope)
	if err != nil {
		return err
	}

	reference := string(img.Digest)
	if reference == "" {
		reference = img.Tag
	}

	u := registry.GetBaseURL(img.Registry)
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", img.Namespace, reference)
	req, err := http.NewRequest(http.MethodPut, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", contentType)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return transport.CheckError(resp, http.StatusCreated)
}
