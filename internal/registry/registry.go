package registry

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

func GetBaseURL(reg image.Registry) *url.URL {
	// TODO: Use go-containerregistry logic for this.
	scheme := "https"
	if strings.HasPrefix(string(reg), "localhost:") {
		scheme = "http"
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

func GetClient(registry image.Registry, scope Scope) (http.Client, error) {
	transport, err := getTransport(registry, string(scope))
	client := http.Client{Transport: transport}
	return client, err
}

type transportKey struct {
	registry image.Registry
	scope    string
}

var (
	transports   = make(map[transportKey]*lockedTransport)
	transportsMu sync.Mutex
)

func getTransport(registry image.Registry, scope string) (http.RoundTripper, error) {
	transportsMu.Lock()
	defer transportsMu.Unlock()

	key := transportKey{registry, scope}
	if transport, ok := transports[key]; ok {
		return transport, nil
	}

	gRegistry, err := name.NewRegistry(string(registry))
	if err != nil {
		return nil, err
	}
	authenticator, err := authn.DefaultKeychain.Resolve(gRegistry)
	if err != nil {
		authenticator = authn.Anonymous
	}
	gTransport, err := transport.NewWithContext(
		context.TODO(),
		gRegistry,
		authenticator,
		http.DefaultTransport,
		[]string{gRegistry.Scope(scope)},
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
