package image

import (
	"fmt"
	"net/url"
	"strings"
)

type Digest string

type Registry string

func (r Registry) BaseURL() *url.URL {
	scheme := "https"
	if strings.HasPrefix(string(r), "localhost:") {
		scheme = "http"
	}
	u, err := url.Parse(fmt.Sprintf("%s://%s", scheme, r))
	if err != nil {
		panic(err) // TODO: Not this.
	}
	return u
}

type Repository struct {
	Registry
	Path string
}

type Image struct {
	Repository
	Tag string
}
