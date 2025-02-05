package work

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const timeout = 2 * time.Second

// async runs fn in a new goroutine. After the current test finishes, async
// waits a short time for that goroutine to exit, then panics if it remains
// blocked.
//
// Under the experimental synctest package, tests can instead spawn goroutines
// normally, and rely on synctest to panic if bubbled goroutines remain
// deadlocked at the end of the test.
func async(t *testing.T, fn func()) {
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	assertEventuallyUnblocks(t, done)
}

// assertIdentityResults requests the listed keys from a queue whose handler
// must return its key as its value, and fails the test if it returns incorrect
// results or does not return results within a reasonable amount of time.
//
// Under the experimental synctest package, tests can instead get and check
// results directly from the queue, and rely on synctest to panic if the request
// deadlocks.
func assertIdentityResults[K comparable](t *testing.T, q *Queue[K, K], keys ...K) {
	t.Helper()

	result := make(chan []K, 1)
	go func() {
		got, err := q.GetAll(keys...)
		assert.NoError(t, err)
		result <- got
	}()

	select {
	case got := <-result:
		assert.Equal(t, keys, got)
	case <-time.After(timeout):
		t.Fatalf("did not get result for keys within %v", timeout)
	}
}

// assertReceiveCount fails a test if it cannot receive a given number of values
// from a channel within a reasonable amount of time. This is often used to
// ensure that one or more goroutines have passed a specific point in their
// execution.
//
// Under the experimental synctest package, these awaited channels can often be
// removed entirely from the goroutines in question, and synctest.Wait can
// replace the lingering assertReceiveCount. When assertReceiveCount is used to
// check that a set of goroutines terminates, users can instead rely on synctest
// to panic if bubbled goroutines remain deadlocked at the end of the test.
func assertReceiveCount[T any](t *testing.T, count int, ch <-chan T) {
	t.Helper()
	bail := time.After(timeout)
	for range count {
		select {
		case <-ch:
		case <-bail:
			t.Fatalf("did not finish receiving within %v", timeout)
		}
	}
}

// assertKeyBlocked fails a test if a queue is able to process a given key
// without blocking. It is best-effort; see [forceRuntimeProgress] for details.
//
// Under the experimental synctest package, tests can instead spawn a goroutine
// to get from the queue, call synctest.Wait to force that goroutine to settle,
// and perform a non-blocking receive to check that the goroutine remains
// blocked. This is verbose compared to assertKeyBlocked, but does not require a
// tacit understanding of this custom assertion to be confident in the
// correctness of the test.
func assertKeyBlocked[K comparable, V any](t *testing.T, q *Queue[K, V], key K) {
	t.Helper()

	done := make(chan struct{})
	go func() { defer close(done); q.Get(key) }()
	forceRuntimeProgress()

	select {
	case <-done:
		t.Errorf("computation of key was not blocked")
	default:
		assertEventuallyUnblocks(t, done)
	}
}

// forceRuntimeProgress makes a best-effort attempt to force the Go runtime to
// make progress on all other goroutines in the system, ideally to the point at
// which they will next block if not preempted. It works best if no other
// goroutines are CPU-intensive or change GOMAXPROCS.
//
// Under the experimental synctest package, synctest.Wait guarantees this
// behavior for the goroutines within a bubble.
func forceRuntimeProgress() {
	gomaxprocs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(gomaxprocs)
	for range runtime.NumGoroutine() {
		runtime.Gosched()
	}
}

// assertEventuallyUnblocks registers a test cleanup that panics if done remains
// blocked after a reasonable amount of time, to help assert that tests do not
// leak goroutines.
//
// Under the experimental synctest package, users can rely on synctest to panic
// if bubbled goroutines remain deadlocked at the end of the test.
func assertEventuallyUnblocks(t *testing.T, done <-chan struct{}) {
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(timeout):
			panic("leaked a blocked goroutine from this test")
		}
	})
}
