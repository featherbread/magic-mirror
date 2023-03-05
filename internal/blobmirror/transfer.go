package blobmirror

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/blobmirror/registry"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

func transfer(dgst image.Digest, from []image.Repository, to image.Repository) error {
	if len(from) < 1 {
		return fmt.Errorf("no sources for blob %s", dgst)
	}

	log.Printf("[blobmirror] Transferring %s from %s to %s", dgst, from[0], to)

	blob, size, err := requestBlob(from[0], dgst)
	if err != nil {
		return err
	}
	defer blob.Close()
	return registry.UploadBlob(to, dgst, size, blob)
}

func requestBlob(from image.Repository, dgst image.Digest) (r io.ReadCloser, size int64, err error) {
	client, err := registry.NewClient(from.Registry, transport.PullScope)
	if err != nil {
		return nil, 0, err
	}

	scheme := "https"
	if strings.HasPrefix(string(from.Registry), "localhost:") {
		scheme = "http"
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   string(from.Registry),
		Path:   fmt.Sprintf("/v2/%s/blobs/%s", from.Path, dgst),
	}
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

	size, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return resp.Body, size, err
}
