package registry

import (
	"context"
	"net/http"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

type Scope string

const (
	PullScope = Scope(transport.PullScope)
	PushScope = Scope(transport.PushScope)
)

type clientKey struct {
	repo  image.Repository
	scope Scope
}

var (
	clients   = make(map[clientKey]http.Client)
	clientsMu sync.Mutex
)

func GetClient(repo image.Repository, scope Scope) (http.Client, error) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	key := clientKey{repo, scope}
	if client, ok := clients[key]; ok {
		return client, nil
	}

	transport, err := getTransport(repo, string(scope))
	client := http.Client{Transport: transport}
	if err == nil {
		clients[key] = client
	}
	return client, err
}

func getTransport(repo image.Repository, scope string) (http.RoundTripper, error) {
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
	return &lockedTransport{RoundTripper: gTransport}, err
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
