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
	const submitCount = 10

	var hits atomic.Uint32
	unblock := make(chan struct{})
	q := NewQueue(0, func(x int) (int, error) {
		hits.Add(1)
		<-unblock
		return x, nil
	})
	defer q.CloseSubmit()

	tasks := make(TaskList[int], submitCount)
	want := make([]int, len(tasks))
	for i := range tasks {
		tasks[i] = q.GetOrSubmit(42)
		want[i] = 42
	}
	close(unblock)

	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, want)

	if hits := hits.Load(); hits > 1 {
		t.Errorf("handler saw %d concurrent executions for the same key", hits)
	}
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
