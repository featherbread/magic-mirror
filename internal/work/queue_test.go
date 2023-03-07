package work

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueBasicUnlimited(t *testing.T) {
	q := NewQueue(0, func(x int) (int, error) { return x, nil })
	defer q.CloseSubmit()

	assertTaskSucceedsWithin(t, 2*time.Second, q.GetOrSubmit(42), 42)
}

func TestQueueBasicLimited(t *testing.T) {
	q := NewQueue(1, func(x int) (int, error) { return x, nil })
	defer q.CloseSubmit()

	assertTaskSucceedsWithin(t, 2*time.Second, q.GetOrSubmit(42), 42)
}

func TestQueueDeduplication(t *testing.T) {
	const submitCount = 10

	var hits atomic.Uint32
	unblock := make(chan struct{})
	q := NewQueue(submitCount, func(x int) (int, error) {
		hits.Add(1)
		<-unblock
		return x, nil
	})
	defer q.CloseSubmit()

	tasks := make(TaskList[int], submitCount)
	for i := range tasks {
		tasks[i] = q.GetOrSubmit(42)
	}
	close(unblock)
	for _, task := range tasks {
		assertTaskSucceedsWithin(t, 2*time.Second, task, 42)
	}

	if hits := hits.Load(); hits > 1 {
		t.Errorf("handler saw %d concurrent executions for the same key", hits)
	}
}

func assertTaskSucceedsWithin[T comparable](t *testing.T, d time.Duration, task *Task[T], want T) {
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
		if got != want {
			t.Errorf("unexpected result from task: got %v, want %v", got, 4)
		}

	case <-time.After(d):
		t.Fatalf("task did not finish within %v", d)
	}
}
