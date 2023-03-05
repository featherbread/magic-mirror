package registry

import (
	"context"
	"net/http"
	"net/url"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

func GetBaseURL(reg image.Registry) *url.URL {
	scheme := "https"
	if nr, err := name.NewRegistry(string(reg)); err == nil {
		scheme = nr.Scheme()
	}
	return &url.URL{
		Scheme: scheme,
		Host:   string(reg),
	}
}

type Scope string

const (
	PullScope = Scope(transport.PullScope)
	PushScope = Scope(transport.PushScope)
)

func GetClient(repo image.Repository, scope Scope) (http.Client, error) {
	transport, err := getTransport(repo, string(scope))
	client := http.Client{Transport: transport}
	return client, err
}

type transportKey struct {
	registry image.Repository
	scope    string
}

var (
	transports   = make(map[transportKey]*lockedTransport)
	transportsMu sync.Mutex
)

func getTransport(repo image.Repository, scope string) (http.RoundTripper, error) {
	transportsMu.Lock()
	defer transportsMu.Unlock()

	key := transportKey{repo, scope}
	if transport, ok := transports[key]; ok {
		return transport, nil
	}

	gRepo, err := name.NewRepository(repo.String())
	if err != nil {
		return nil, err
	}
	authenticator, err := authn.DefaultKeychain.Resolve(gRepo)
	if err != nil {
		authenticator = authn.Anonymous
	}
	gTransport, err := transport.NewWithContext(
		context.TODO(),
		gRepo.Registry,
		authenticator,
		http.DefaultTransport,
		[]string{gRepo.Scope(scope)},
	)
	if err != nil {
		return nil, err
	}

	transport := &lockedTransport{RoundTripper: gTransport}
	transports[key] = transport
	return transport, err
}

type lockedTransport struct {
	http.RoundTripper
	mu sync.Mutex
}

func (t *lockedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.RoundTripper.RoundTrip(req)
}
