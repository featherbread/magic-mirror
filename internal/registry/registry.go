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

type clientKey struct {
	registry image.Registry
	scope    Scope
}

var (
	clients   = make(map[clientKey]http.Client)
	clientsMu sync.Mutex
)

func GetClient(registry image.Registry, scope Scope) (http.Client, error) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	if client, ok := clients[clientKey{registry, scope}]; ok {
		return client, nil
	}

	transport, err := getTransport(registry, string(scope))
	client := http.Client{Transport: transport}
	if err == nil {
		clients[clientKey{registry, scope}] = client
	}
	return client, err
}

func getTransport(registry image.Registry, scope string) (http.RoundTripper, error) {
	gRegistry, err := name.NewRegistry(string(registry))
	if err != nil {
		return nil, err
	}
	authenticator, err := authn.DefaultKeychain.Resolve(gRegistry)
	if err != nil {
		authenticator = authn.Anonymous
	}
	return transport.NewWithContext(
		context.TODO(),
		gRegistry,
		authenticator,
		http.DefaultTransport,
		[]string{gRegistry.Scope(scope)},
	)
}
