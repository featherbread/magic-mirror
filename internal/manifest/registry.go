package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

var supportedManifestMediaTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

func downloadManifest(repo image.Repository, reference string) (contentType string, body json.RawMessage, err error) {
	client, err := getRegistryClient(repo.Registry, transport.PullScope)
	if err != nil {
		return
	}

	u := getBaseURL(repo.Registry)
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
	client, err := getRegistryClient(repo.Registry, transport.PushScope)
	if err != nil {
		return err
	}

	u := getBaseURL(repo.Registry)
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
