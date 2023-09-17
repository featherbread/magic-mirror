package work

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueBasicUnlimited(t *testing.T) {
	q := NewQueue(0, func(_ *QueueHandle, x int) (int, error) { return x, nil })
	assertSucceedsWithin(t, 2*time.Second, q, []int{42}, []int{42})
}

func TestQueueBasicLimited(t *testing.T) {
	q := NewQueue(1, func(_ *QueueHandle, x int) (int, error) { return x, nil })
	assertSucceedsWithin(t, 2*time.Second, q, []int{42}, []int{42})
}

func TestQueueDeduplication(t *testing.T) {
	const (
		count = 10
		half  = count / 2
	)

	unblock := make(chan struct{})
	q := NewQueue(0, func(_ *QueueHandle, x int) (int, error) {
		<-unblock
		return x, nil
	})

	keys := makeIntKeys(count)

	close(unblock)
	assertSucceedsWithin(t, 2*time.Second, q, keys[:half], keys[:half])

	unblock = make(chan struct{})
	assertSucceedsWithin(t, 2*time.Second, q, keys[:half], keys[:half])
	cleanup := assertBlocked(t, q, keys[half])
	defer cleanup()

	close(unblock)
	assertSucceedsWithin(t, 2*time.Second, q, keys, keys)

	unblock = make(chan struct{})
	assertSucceedsWithin(t, 2*time.Second, q, keys, keys)
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
	q := NewQueue(workerCount, func(_ *QueueHandle, x int) (int, error) {
		count := inflight.Add(1)
		defer inflight.Add(-1)
		if count > workerCount {
			breached.Store(true)
		}
		<-unblock
		return x, nil
	})

	keys := makeIntKeys(submitCount)
	go func() { q.GetAll(keys...) }()
	forceRuntimeProgress(workerCount + 1)
	close(unblock)
	assertSucceedsWithin(t, 2*time.Second, q, keys, keys)

	if breached.Load() {
		t.Errorf("queue breached limit of %d workers in flight", workerCount)
	}
}

func TestQueueDetachReattachUnlimited(t *testing.T) {
	const submitCount = 50

	q := NewQueue(0, func(qh *QueueHandle, x int) (int, error) {
		if !qh.Detach() {
			panic("did not actually detach from queue") // Not ideal, but a very fast way to fail everything.
		}
		if !qh.Reattach() {
			panic("did not actually reattach to queue")
		}
		return x, nil
	})

	keys := makeIntKeys(submitCount)
	assertSucceedsWithin(t, 2*time.Second, q, keys, keys)
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
	q := NewQueue(workerCount, func(qh *QueueHandle, x int) (int, error) {
		if !qh.Detach() {
			panic("did not actually detach from queue")
		}
		countDetached.Add(1)
		<-awaitDetached

		<-unblockReattach
		if !qh.Reattach() {
			panic("did not actually reattach to queue")
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
	go func() { q.GetAll(keys...) }()

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
	assertSucceedsWithin(t, 2*time.Second, q, keys, keys)

	if breachedReattach.Load() {
		t.Errorf("queue breached limit of %d workers in flight during reattach", workerCount)
	}
}
