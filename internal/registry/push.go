package registry

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

func UploadBlob(repo image.Repository, dgst image.Digest, size int64, r io.Reader) error {
	transport, err := getTransport(repo.Registry, transport.PushScope)
	if err != nil {
		return err
	}
	client := http.Client{Transport: transport}
	p := pusher{repo, client}
	return p.uploadBlob(dgst, size, r)
}

type pusher struct {
	repository image.Repository
	client     http.Client
}

func (p *pusher) uploadBlob(dgst image.Digest, size int64, r io.Reader) error {
	if p.canSkipBlobUpload(dgst) {
		return nil
	}

	uploadURL, err := p.getBlobUploadURL()
	if err != nil {
		return err
	}

	query, err := url.ParseQuery(uploadURL.RawQuery)
	if err != nil {
		return err
	}
	query.Add("digest", string(dgst))
	uploadURL.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodPut, uploadURL.String(), r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.FormatInt(size, 10))

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return transport.CheckError(resp, http.StatusCreated)
}

func (p *pusher) canSkipBlobUpload(dgst image.Digest) (ok bool) {
	req, err := http.NewRequest(http.MethodHead, p.url("/blobs/%s", dgst).String(), nil)
	if err != nil {
		return false
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func (p *pusher) getBlobUploadURL() (u *url.URL, err error) {
	uploadURL := p.url("/blobs/uploads/")
	req, err := http.NewRequest(http.MethodPost, uploadURL.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := transport.CheckError(resp, http.StatusAccepted); err != nil {
		return nil, err
	}

	return uploadURL.Parse(resp.Header.Get("Location"))
}

func (p *pusher) url(format string, v ...interface{}) *url.URL {
	u := p.repository.BaseURL()
	u.Path = "/v2/" + p.repository.Path + fmt.Sprintf(format, v...)
	return u
}
