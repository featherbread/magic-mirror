package work

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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

	keys := makeIntKeys(count)

	close(unblock)
	halfTasks := q.GetOrSubmitAll(keys[:half]...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, halfTasks, keys[:half])

	unblock = make(chan struct{})
	tasks := q.GetOrSubmitAll(keys...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks[:half], keys[:half])
	assertTaskBlocked(t, tasks[half])

	close(unblock)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, keys)

	unblock = make(chan struct{})
	tasksAgain := q.GetOrSubmitAll(keys...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasksAgain, keys)
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

	keys := makeIntKeys(submitCount)
	tasks := q.GetOrSubmitAll(keys...)

	forceRuntimeProgress(workerCount + 1)
	close(unblock)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, keys)
	if breached.Load() {
		t.Errorf("queue breached limit of %d workers in flight", workerCount)
	}
}

func TestQueueDetachReattachUnlimited(t *testing.T) {
	const submitCount = 50

	q := NewQueue(0, func(ctx context.Context, x int) (int, error) {
		if err := Detach(ctx); err != nil {
			panic(err) // Not ideal, but a very fast way to fail everything.
		}
		if err := Reattach(ctx); err != nil {
			panic(err)
		}
		return x, nil
	})

	keys := makeIntKeys(submitCount)
	tasks := q.GetOrSubmitAll(keys...)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, keys)
}

func TestQueueDetachReattachLimited(t *testing.T) {
	const (
		submitCount = 50
		workerCount = 10
	)

	var (
		awaitDetached      = make(chan struct{})
		countDetached      atomic.Int32
		unblockReattach    = make(chan struct{})
		reattachedInflight atomic.Int32
		breachedReattach   atomic.Bool
		unblockReturn      = make(chan struct{})
	)
	q := NewQueue(workerCount, func(ctx context.Context, x int) (int, error) {
		if err := Detach(ctx); err != nil {
			panic(err)
		}
		countDetached.Add(1)
		<-awaitDetached

		<-unblockReattach
		if err := Reattach(ctx); err != nil {
			panic(err)
		}
		count := reattachedInflight.Add(1)
		defer reattachedInflight.Add(-1)
		if count > workerCount {
			breachedReattach.Store(true)
		}

		<-unblockReturn
		return x, nil
	})

	keys := makeIntKeys(submitCount)
	tasks := q.GetOrSubmitAll(keys...)

	timeout := time.After(2 * time.Second)
	for i := 0; i < submitCount; i++ {
		select {
		case awaitDetached <- struct{}{}:
		case <-timeout:
			t.Fatalf("timed out waiting for tasks to detach: %d of %d ready", countDetached.Load(), submitCount)
		}
	}

	close(unblockReattach)
	forceRuntimeProgress(workerCount + 1)
	close(unblockReturn)
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, keys)

	if breachedReattach.Load() {
		t.Errorf("queue breached limit of %d workers in flight during reattach", workerCount)
	}
}
