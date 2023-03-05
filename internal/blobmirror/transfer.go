package blobmirror

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
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

	toTransport, err := newTransport(to.Registry, transport.PushScope)
	if err != nil {
		return err
	}
	toClient := &http.Client{Transport: toTransport}

	requestUploadURL := to.Registry.BaseURL()
	requestUploadURL.Path = fmt.Sprintf("/v2/%s/blobs/uploads/", to.Path)
	requestUploadReq, err := http.NewRequest(http.MethodPost, requestUploadURL.String(), nil)
	if err != nil {
		return err
	}
	requestUploadResp, err := toClient.Do(requestUploadReq)
	if err != nil {
		return err
	}
	defer requestUploadResp.Body.Close()
	if err := transport.CheckError(requestUploadResp, http.StatusAccepted); err != nil {
		return err
	}

	doUploadURL, err := requestUploadURL.Parse(requestUploadResp.Header.Get("Location"))
	if err != nil {
		return err
	}
	doUploadQuery, err := url.ParseQuery(doUploadURL.RawQuery)
	if err != nil {
		return err
	}
	doUploadQuery.Add("digest", string(dgst))
	doUploadURL.RawQuery = doUploadQuery.Encode()
	doUploadReq, err := http.NewRequest(http.MethodPut, doUploadURL.String(), fromResp.Body)
	if err != nil {
		return err
	}
	doUploadReq.Header.Set("Content-Type", "application/octet-stream")
	doUploadReq.Header.Set("Content-Length", fromResp.Header.Get("Content-Length"))
	doUploadResp, err := toClient.Do(doUploadReq)
	if err != nil {
		return err
	}
	defer doUploadResp.Body.Close()
	if err := transport.CheckError(doUploadResp, http.StatusCreated); err != nil {
		return err
	}

	return nil
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
