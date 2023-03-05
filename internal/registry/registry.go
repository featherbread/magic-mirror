package registry

import (
	"context"
	"net/http"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

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
