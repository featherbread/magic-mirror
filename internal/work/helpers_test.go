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

func assertTaskBlocked[T any](t *testing.T, task *task[T]) {
	t.Helper()

	// Make an effort to ensure the task is scheduled.
	runtime.Gosched()

	select {
	case <-task.done:
		// TODO: Can we do this without touching Task internals?
		t.Errorf("task was not blocked")
	default:
	}
}

// forceRuntimeProgress attempts to force the Go runtime to make progress on at
// least n other goroutines before resuming the current one.
//
// This function is intended to help reveal issues with concurrency limits by
// forcing the goroutines subject to those limits to hit some kind of blocking
// condition simultaneously. When the only other live goroutines in a test are
// those spawned by the test itself, this approach appears in practice to work
// just as well as other strategies that may be at least an order of magnitude
// more expensive (e.g. time.Sleep calls). However, there is some inherent
// non-determinism in this strategy that could reveal itself if some other test
// leaks runnable goroutines.
func forceRuntimeProgress(n int) {
	gomaxprocs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(gomaxprocs)
	for i := 0; i < n; i++ {
		runtime.Gosched()
	}
}
