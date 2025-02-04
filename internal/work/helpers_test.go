package work

import (
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const timeout = 2 * time.Second

func makeIntKeys(n int) (keys []int) {
	keys = make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	return
}

func async(t *testing.T, fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	assertEventuallyUnblocks(t, done)
}

func assertIdentityResults[K comparable](t *testing.T, q *Queue[K, K], keys ...K) {
	t.Helper()

	var (
		done = make(chan struct{})
		got  []K
		err  error
	)
	go func() {
		defer close(done)
		got, err = q.GetAll(keys...)
	}()

	select {
	case <-done:
		if err != nil {
			t.Errorf("unexpected handler error: %v", err)
		}
		if diff := cmp.Diff(keys, got); diff != "" {
			t.Errorf("unexpected handler results (-want +got): %s", diff)
		}

	case <-time.After(timeout):
		t.Fatalf("did not get result for key within %v", timeout)
	}
}

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

func assertDoneCount[K comparable, V any](t *testing.T, q *Queue[K, V], want uint64) {
	t.Helper()
	stats := q.Stats()
	if stats.Done != want {
		t.Errorf("queue reports %d tasks done, want %d", stats.Done, want)
	}
}

func assertSubmittedCount[K comparable, V any](t *testing.T, q *Queue[K, V], want uint64) {
	t.Helper()
	stats := q.Stats()
	if stats.Submitted != want {
		t.Errorf("queue reports %d tasks submitted, want %d", stats.Submitted, want)
	}
}

// assertBlockedAfter fails a test if a queue is able to process a given key
// without blocking after a call to settle().
//
// The settle function should ensure that any goroutines that might incorrectly
// unblock the queue have progressed far enough to do so, ideally to the point
// that they have exited or are durably blocked on another condition.
// [forceRuntimeProgress] makes a best-effort attempt to ensure this in stable
// versions of Go as of writing. Future versions of Go may provide a mechanism
// to robustly guarantee this, like the experimental "testing/synctest" package.
func assertBlockedAfter[K comparable, V any](settle func(), t *testing.T, q *Queue[K, V], key K) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Get(key)
	}()

	settle()

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
func forceRuntimeProgress() {
	gomaxprocs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(gomaxprocs)
	for range runtime.NumGoroutine() {
		runtime.Gosched()
	}
}
