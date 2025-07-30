package copy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/featherbread/magic-mirror/internal/image"
	"github.com/featherbread/magic-mirror/internal/image/registry"
	"github.com/featherbread/magic-mirror/internal/log"
	"github.com/featherbread/magic-mirror/internal/parka"
)

func uploadManifest(img image.Image, manifest image.ManifestKind) error {
	client, err := registry.GetClient(img.Repository, registry.PushScope)
	if err != nil {
		return err
	}

	reference := img.Tag
	if reference == "" {
		reference = img.Digest.String()
	}
	if reference == "" {
		reference = manifest.Descriptor().Digest.String()
	}

	u := img.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", img.Namespace, reference)
	req, err := http.NewRequest(http.MethodPut, u.String(), bytes.NewReader(manifest.Encoded()))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", manifest.Descriptor().MediaType)

	_, err = client.DoExpectingNoBody(req, http.StatusCreated)
	return err
}

type manifestCache struct {
	*parka.Map[image.Image, image.ManifestKind]
}

func newManifestCache(concurrency int) *manifestCache {
	cache := manifestCache{parka.NewMap(getManifest)}
	cache.Map.Limit(concurrency)
	return &cache
}

func getManifest(_ *parka.Handle, img image.Image) (image.ManifestKind, error) {
	reference := img.Digest.String()
	if reference == "" {
		reference = img.Tag
	}

	log.Verbosef("[manifest]\tdownloading %s", img)

	client, err := registry.GetClient(img.Repository, registry.PullScope)
	if err != nil {
		return nil, err
	}

	u := img.Registry.APIBaseURL()
	u.Path = fmt.Sprintf("/v2/%s/manifests/%s", img.Namespace, reference)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", strings.Join(image.AllManifestMediaTypes, ","))

	resp, err := client.DoExpecting(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	if img.Digest != "" {
		verifier := img.Digest.Verifier()
		io.Copy(verifier, bytes.NewReader(body))
		if !verifier.Verified() {
			return nil, fmt.Errorf("content of %s does not match specified digest", img)
		}
	}

	var result image.ManifestKind
	contentType := image.MediaType(resp.Header.Get("Content-Type"))
	switch {
	case contentType.IsIndex():
		var index image.RawIndex
		err = json.Unmarshal(body, &index)
		result = index
	case contentType.IsManifest():
		var manifest image.RawManifest
		err = json.Unmarshal(body, &manifest)
		result = manifest
	default:
		err = fmt.Errorf("unknown manifest type for %s: %s", img, contentType)
	}
	return result, err
}
