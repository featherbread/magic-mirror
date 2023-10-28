package work

import (
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func makeIntKeys(n int) (keys []int) {
	keys = make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	return
}

func assertSucceedsWithin[K comparable, V any](t *testing.T, timeout time.Duration, q *Queue[K, V], keys []K, want []V) {
	t.Helper()

	var (
		done = make(chan struct{})
		got  []V
		err  error
	)
	go func() {
		defer close(done)
		got, err = q.GetAll(keys...)
	}()

	select {
	case <-done:
		if err != nil {
			t.Errorf("unexpected error from task: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result from task (-want +got): %s", diff)
		}

	case <-time.After(timeout):
		t.Fatalf("did not get result for key within %v", timeout)
	}
}

func assertDoneCount[K comparable, V any](t *testing.T, q *Queue[K, V], want uint64) {
	t.Helper()
	done, _ := q.Stats()
	if done != want {
		t.Errorf("queue reports %d tasks done, want %d", done, want)
	}
}

func assertSubmittedCount[K comparable, V any](t *testing.T, q *Queue[K, V], want uint64) {
	t.Helper()
	_, submitted := q.Stats()
	if submitted != want {
		t.Errorf("queue reports %d tasks submitted, want %d", submitted, want)
	}
}

func assertBlocked[K comparable, V any](t *testing.T, q *Queue[K, V], key K) (cleanup func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Get(key)
	}()

	// Make a best-effort attempt to force the key's handler to be in flight when
	// it should not be.
	forceRuntimeProgress()

	select {
	case <-done:
		t.Errorf("computation of key was not blocked")
	default:
	}

	return func() { <-done }
}

// forceRuntimeProgress attempts to force the Go runtime to make progress on
// every other live goroutine before resuming the current one.
//
// In scenarios where all live goroutines other than the current one are
// expected to eventually block, this function tries to force those goroutines
// to execute up to that eventual state so that assertions can be made about it.
// For example, if an implementation of concurrency limits is broken, or if a
// goroutine can run to completion when it should be blocked, this function
// should force those conditions to occur more quickly and reliably than
// sleeping for a predetermined time.
//
// This function operates on a best-effort basis. It works best when no live
// goroutines in the system are expected to spawn further goroutines or perform
// CPU-intensive work without blocking.
func forceRuntimeProgress() {
	gomaxprocs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(gomaxprocs)
	n := runtime.NumGoroutine()
	for i := 0; i < n; i++ {
		runtime.Gosched()
	}
}
