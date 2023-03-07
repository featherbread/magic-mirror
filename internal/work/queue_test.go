package work

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestQueueBasicUnlimited(t *testing.T) {
	q := NewQueue(0, func(_ context.Context, x int) (int, error) { return x, nil })
	defer q.CloseSubmit()

	assertTaskSucceedsWithin[int](t, 2*time.Second, q.GetOrSubmit(42), 42)
}

func TestQueueBasicLimited(t *testing.T) {
	q := NewQueue(1, func(_ context.Context, x int) (int, error) { return x, nil })
	defer q.CloseSubmit()

	assertTaskSucceedsWithin[int](t, 2*time.Second, q.GetOrSubmit(42), 42)
}

func TestQueueDeduplication(t *testing.T) {
	const (
		count = 10
		half  = count / 2
	)

	unblock := make(chan struct{})
	q := NewQueue(0, func(_ context.Context, x int) (int, error) {
		<-unblock
		return x, nil
	})
	defer q.CloseSubmit()

	want := make([]int, count)
	for i := range want {
		want[i] = i
	}

	close(unblock)
	halfTasks := q.GetOrSubmitAll(want[:half]...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, halfTasks, want[:half])

	unblock = make(chan struct{})
	tasks := q.GetOrSubmitAll(want...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks[:half], want[:half])
	assertTaskBlocked(t, tasks[half])

	close(unblock)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, want)

	unblock = make(chan struct{})
	tasksAgain := q.GetOrSubmitAll(want...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasksAgain, want)
}

func TestQueueConcurrencyLimit(t *testing.T) {
	const (
		submitCount = 50
		workerCount = 10
	)

	var (
		inflight atomic.Int32
		breached atomic.Bool
		unblock  = make(chan struct{})
	)
	q := NewQueue(workerCount, func(_ context.Context, x int) (int, error) {
		count := inflight.Add(1)
		defer inflight.Add(-1)
		if count > workerCount {
			breached.Store(true)
		}
		<-unblock
		return x, nil
	})
	defer q.CloseSubmit()

	values := make([]int, submitCount)
	for i := range values {
		values[i] = i
	}
	tasks := q.GetOrSubmitAll(values...)

	// Try to force the Go runtime to make progress on at least the number of
	// tasks that will result in a breach. There's some inherent non-determinism
	// in this strategy that could reveal itself if some other test leaks runnable
	// goroutines, but as of this writing it appears to work just as well as
	// time.Sleep calls that are about an order of magnitude more expensive.
	gomaxprocs := runtime.GOMAXPROCS(1)
	for i := 0; i < workerCount+1; i++ {
		runtime.Gosched()
	}
	runtime.GOMAXPROCS(gomaxprocs)
	close(unblock)

	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, values)

	if breached.Load() {
		t.Errorf("queue breached limit of %d workers in flight", workerCount)
	}
}

type awaitable[T any] interface {
	Wait() (T, error)
}

func assertTaskSucceedsWithin[T any](t *testing.T, d time.Duration, task awaitable[T], want T) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		task.Wait()
	}()

	select {
	case <-done:
		got, err := task.Wait()
		if err != nil {
			t.Errorf("unexpected error from task: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result from task (-want +got): %s", diff)
		}

	case <-time.After(d):
		t.Fatalf("task did not finish within %v", d)
	}
}

func assertTaskBlocked[T any](t *testing.T, task *Task[T]) {
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
