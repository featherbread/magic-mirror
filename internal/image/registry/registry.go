package registry

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"github.com/ahamlinman/magic-mirror/internal/image"
)

// Error is the type of the structured error that [Client.DoExpecting] returns
// when a request yields an unexpected response code.
type Error = transport.Error

// Client is an HTTP client with registry-specific helpers.
type Client struct {
	http.Client
}

// DoExpecting performs an HTTP request, then asserts that the status code of
// the response is among those listed. If it is not, DoExpecting consumes and
// closes resp.Body to return a non-nil error based on the response content,
// along with the remainder of the response value.
func (c Client) DoExpecting(req *http.Request, codes ...int) (resp *http.Response, err error) {
	resp, err = c.Client.Do(req)
	if err != nil {
		return
	}
	err = transport.CheckError(resp, codes...)
	if err != nil {
		resp.Body.Close()
	}
	return
}

// DoExpectingNoBody behaves like [Client.DoExpecting], but always discards and
// closes resp.Body. Body will always be nil in the returned response, which
// differs from [http.Client]'s behavior of always returning a non-nil Body.
func (c Client) DoExpectingNoBody(req *http.Request, codes ...int) (resp *http.Response, err error) {
	resp, err = c.DoExpecting(req, codes...)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	resp.Body = nil
	return
}

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
	clients   = make(map[clientKey]Client)
	clientsMu sync.Mutex
)

// GetClient returns an HTTP client that transparently authenticates API
// requests to the provided image repository with the provided scope. The
// returned client is safe for concurrent use by multiple goroutines, and may be
// shared with other callers.
func GetClient(repo image.Repository, scope Scope) (Client, error) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	key := clientKey{repo, scope}
	if client, ok := clients[key]; ok {
		return client, nil
	}

	transport, err := getTransport(repo, string(scope))
	client := Client{http.Client{Transport: transport}}
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
