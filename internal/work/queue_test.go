package work

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestQueueBasicUnlimited(t *testing.T) {
	q := NewQueue(0, func(x int) (int, error) { return x, nil })
	defer q.CloseSubmit()

	assertTaskSucceedsWithin[int](t, 2*time.Second, q.GetOrSubmit(42), 42)
}

func TestQueueBasicLimited(t *testing.T) {
	q := NewQueue(1, func(x int) (int, error) { return x, nil })
	defer q.CloseSubmit()

	assertTaskSucceedsWithin[int](t, 2*time.Second, q.GetOrSubmit(42), 42)
}

func TestQueueDeduplication(t *testing.T) {
	const (
		count = 10
		half  = count / 2
	)

	unblock := make(chan struct{})
	q := NewQueue(0, func(x int) (int, error) {
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
	q := NewQueue(workerCount, func(x int) (int, error) {
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

	// Make an effort to queue as many tasks as possible before unblocking them.
	runtime.Gosched()
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
