package blobmirror

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/registry"
)

func transfer(dgst image.Digest, from []image.Repository, to image.Repository) error {
	if len(from) < 1 {
		return fmt.Errorf("no sources for blob %s", dgst)
	}

	log.Printf("[blobmirror] Transferring %s from %s to %s", dgst, from[0], to)

	fromTransport, err := newTransport(from[0].Registry, transport.PullScope)
	if err != nil {
		return err
	}
	fromClient := &http.Client{Transport: fromTransport}

	fromURL := from[0].Registry.BaseURL()
	fromURL.Path = fmt.Sprintf("/v2/%s/blobs/%s", from[0].Path, dgst)
	fromReq, err := http.NewRequest(http.MethodGet, fromURL.String(), nil)
	if err != nil {
		return err
	}
	fromResp, err := fromClient.Do(fromReq)
	if err != nil {
		return err
	}
	defer fromResp.Body.Close()
	if err := transport.CheckError(fromResp, http.StatusOK); err != nil {
		return err
	}
	size, err := strconv.ParseInt(fromResp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return err
	}

	return registry.UploadBlob(to, dgst, size, fromResp.Body)
}

func newTransport(registry image.Registry, scopes ...string) (http.RoundTripper, error) {
	gRegistry, err := name.NewRegistry(string(registry))
	if err != nil {
		return nil, err
	}

	authenticator, err := authn.DefaultKeychain.Resolve(gRegistry)
	if err != nil {
		authenticator = authn.Anonymous
	}

	imgScopes := make([]string, len(scopes))
	for i, scope := range scopes {
		imgScopes[i] = gRegistry.Scope(scope)
	}

	return transport.NewWithContext(
		context.Background(),
		gRegistry,
		authenticator,
		http.DefaultTransport,
		imgScopes,
	)
}
