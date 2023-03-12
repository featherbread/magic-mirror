package image

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opencontainers/go-digest"
)

type Registry string

func (r Registry) APIBaseURL() *url.URL {
	scheme := "https"
	if nr, err := name.NewRegistry(string(r)); err == nil {
		scheme = nr.Scheme()
	}
	return &url.URL{
		Scheme: scheme,
		Host:   string(r),
	}
}

type Repository struct {
	Registry
	Namespace string
}

func (r Repository) String() string {
	return fmt.Sprintf("%s/%s", r.Registry, r.Namespace)
}

// TODO: Rename this to something like Ref?
type Image struct {
	Repository
	Tag    string
	Digest digest.Digest
}

const defaultRegistry = "docker.io"

// TODO: This may not be the ideal way to do this.
var imageRegexp = regexp.MustCompile(`^(?:(?P<registry>[^/]+[.:][^/]+)/)?(?P<namespace>[^:@]+)(?::(?P<tag>[a-zA-Z0-9-_.]{1,128}))?(?:@(?P<digest>.+))?$`)

func Parse(s string) (Image, error) {
	match := imageRegexp.FindStringSubmatch(s)
	if len(match) == 0 {
		return Image{}, fmt.Errorf("image reference %q does not match expected format", s)
	}

	var (
		registry  = match[1]
		namespace = match[2]
		tag       = match[3]
		rawDigest = match[4]
	)
	if registry == "" {
		registry = defaultRegistry
	}
	if registry == defaultRegistry && !strings.Contains(namespace, "/") {
		namespace = "library/" + namespace
	}
	if tag == "" && rawDigest == "" {
		tag = "latest"
	}

	img := Image{
		Repository: Repository{
			Registry:  Registry(registry),
			Namespace: namespace,
		},
		Tag: tag,
	}

	if rawDigest != "" {
		img.Digest = digest.Digest(rawDigest)
		if err := img.Digest.Validate(); err != nil {
			return Image{}, fmt.Errorf("invalid digest in %q: %w", s, err)
		}
	}

	return img, nil
}

func (i Image) String() string {
	result := i.Repository.String()
	if i.Tag != "" {
		result += ":" + i.Tag
	}
	if digest := i.Digest.String(); digest != "" {
		result += "@" + digest
	}
	return result
}

func (i Image) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

func (i *Image) UnmarshalText(b []byte) error {
	result, err := Parse(string(b))
	if err != nil {
		return err
	}
	*i = result
	return nil
}
