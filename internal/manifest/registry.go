package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

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

func downloadManifest(repo image.Repository, reference string) (contentType string, body json.RawMessage, err error) {
	client, err := registry.GetClient(repo.Registry, registry.PullScope)
	if err != nil {
		return
	}

	u := registry.GetBaseURL(repo.Registry)
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", repo.Namespace, reference)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return
	}
	req.Header.Add("Accept", strings.Join(supportedManifestMediaTypes, ","))

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	err = transport.CheckError(resp, http.StatusOK)
	if err != nil {
		return
	}

	contentType = resp.Header.Get("Content-Type")
	body, err = io.ReadAll(resp.Body)
	return
}

func uploadManifest(repo image.Repository, reference string, contentType string, body json.RawMessage) error {
	client, err := registry.GetClient(repo.Registry, registry.PushScope)
	if err != nil {
		return err
	}

	u := registry.GetBaseURL(repo.Registry)
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", repo.Namespace, reference)
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
