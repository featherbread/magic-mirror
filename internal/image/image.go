package image

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
)

type Digest string

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

type Image struct {
	Repository
	Tag    string
	Digest Digest
}

const defaultRegistry = "docker.io"

// TODO: This may not be the ideal way to do this.
var imageRegexp = regexp.MustCompile(`^(?:(?P<registry>[^/]+[.:][^/]+)/)?(?P<namespace>[^:@]+)(?::(?P<tag>[a-zA-Z0-9-_.]{1,128}))?(?:@(?P<digest>.+))?$`)

func Parse(s string) (Image, error) {
	m := imageRegexp.FindStringSubmatch(s)
	if len(m) == 0 {
		return Image{}, fmt.Errorf("cannot parse image reference: %s", s)
	}

	var (
		registry  = m[1]
		namespace = m[2]
		tag       = m[3]
		digest    = Digest(m[4])
	)
	if registry == "" {
		registry = defaultRegistry
	}
	if registry == defaultRegistry && !strings.Contains(namespace, "/") {
		namespace = "library/" + namespace
	}
	if tag == "" && digest == "" {
		tag = "latest"
	}

	return Image{
		Repository: Repository{
			Registry:  Registry(registry),
			Namespace: namespace,
		},
		Tag:    tag,
		Digest: digest,
	}, nil
}

func (i Image) String() string {
	s := i.Repository.String()
	if i.Tag != "" {
		s += ":" + i.Tag
	}
	if i.Digest != "" {
		s += "@" + string(i.Digest)
	}
	return s
}
