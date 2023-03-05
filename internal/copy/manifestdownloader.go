package copy

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/registry"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type ManifestDownloader struct {
	*work.Queue[image.Image, DownloadResponse]
}

type DownloadRequest struct {
	From      image.Repository
	Reference string
}

type DownloadResponse struct {
	ContentType string
	Body        json.RawMessage
}

func NewDownloader(workers int) *ManifestDownloader {
	d := &ManifestDownloader{}
	d.Queue = work.NewQueue(workers, d.handleRequest)
	return d
}

func (d *ManifestDownloader) Get(img image.Image) (DownloadResponse, error) {
	return d.Queue.GetOrSubmit(img).Wait()
}

func (d *ManifestDownloader) handleRequest(img image.Image) (resp DownloadResponse, err error) {
	reference := img.Digest
	if reference == "" {
		reference = img.Tag
	}

	log.Printf("[manifest]\tdownloading %s", img)

	client, err := registry.GetClient(img.Repository, registry.PullScope)
	if err != nil {
		return
	}

	u := registry.GetBaseURL(img.Registry)
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
	return DownloadResponse{ContentType: contentType, Body: body}, err
}
