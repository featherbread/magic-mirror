package registry

import (
	"context"
	"net/http"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

type Client struct {
	image.Registry
	http.Client
}

func NewClient(registry image.Registry, scope string) (*Client, error) {
	transport, err := getTransport(registry, scope)
	if err != nil {
		return nil, err
	}
	return &Client{
		Registry: registry,
		Client:   http.Client{Transport: transport},
	}, nil
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
		context.Background(),
		gRegistry,
		authenticator,
		http.DefaultTransport,
		[]string{gRegistry.Scope(scope)},
	)
}
