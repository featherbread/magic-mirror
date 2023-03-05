package blobmirror

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/registry"
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

	u := fmt.Sprintf("%s/v2/%s/blobs/%s", client.BaseURL().String(), from.Path, dgst)
	req, err := http.NewRequest(http.MethodGet, u, nil)
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
