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

// Scope represents a permission on a particular image repository.
type Scope string

const (
	// PullScope represents the permission to pull images from a specific
	// repository.
	PullScope = Scope(transport.PullScope)

	// PushScope represents the permission to push images to a specific
	// repository.
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

// GetClient returns an HTTP client that transparently authenticates API
// requests to the provided image repository with the provided scope. The
// returned client is safe for concurrent use by multiple goroutines, and may be
// shared with other callers.
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

// CheckResponse validates that the response code is within the provided set of
// codes. If it is not, CheckResponse consumes the response body and returns a
// detailed error.
func CheckResponse(resp *http.Response, codes ...int) error {
	return transport.CheckError(resp, codes...)
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
	return newLockedTransport(gTransport), err
}

// lockedTransport works around the non-thread-safety of the underlying
// RoundTripper from go-containerregistry.
type lockedTransport struct {
	http.RoundTripper
	mu ctxLock
}

func newLockedTransport(rt http.RoundTripper) *lockedTransport {
	return &lockedTransport{
		RoundTripper: rt,
	}
}

func (t *lockedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.mu.LockWithContext(req.Context()); err != nil {
		return nil, err
	}
	defer t.mu.Unlock()
	return t.RoundTripper.RoundTrip(req)
}

// ctxLock is a mutex that supports canceling a lock attempt with a
// [context.Context]. The zero value for a ctxLock is an unlocked mutex.
type ctxLock struct {
	ch   chan struct{}
	init sync.Once
}

func (l *ctxLock) ensureInit() {
	l.init.Do(func() {
		l.ch = make(chan struct{}, 1)
		l.ch <- struct{}{}
	})
}

// LockWithContext blocks the calling goroutine until the mutex is available, in
// which case it returns a nil error, or until ctx is canceled, in which case it
// returns ctx.Err().
//
// When LockWithContext returns a non-nil error, the caller must not attempt to
// unlock the mutex or violate any invariant that it protects.
func (l *ctxLock) LockWithContext(ctx context.Context) error {
	l.ensureInit()
	select {
	case <-l.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock unlocks the locked mutex, or panics if the mutex is already unlocked.
func (l *ctxLock) Unlock() {
	l.ensureInit()
	select {
	case l.ch <- struct{}{}:
		return
	default:
		panic("unlock of unlocked ctxLock")
	}
}
