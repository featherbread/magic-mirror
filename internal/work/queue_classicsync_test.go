package work

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueGoexitHandling(t *testing.T) {
	stepGoexit := make(chan struct{})
	q := NewQueue(1, func(_ *QueueHandle, x int) (int, error) {
		if x == 0 {
			for range stepGoexit {
			}
			runtime.Goexit()
		}
		return x, nil
	})

	// Start the handler that will Goexit, and ensure that it's blocked.
	async(t, func() { q.Get(0) })
	stepGoexit <- struct{}{}

	// Force some more handlers to queue up...
	keys := []int{1, 2}
	async(t, func() { q.GetAll(keys...) })
	forceRuntimeProgress()

	// ...then let them through.
	close(stepGoexit)

	// Ensure that the Goexit didn't break the handling of those new keys.
	assertIdentityResults(t, q, keys...)
}

func TestQueueDeduplication(t *testing.T) {
	unblock := make(chan struct{})
	q := NewQueue(0, func(_ *QueueHandle, x int) (int, error) {
		<-unblock
		return x, nil
	})

	const count = 10
	const half = count / 2
	keys := makeIntKeys(count)

	// Handle and cache the first half of the keys.
	close(unblock)
	assertIdentityResults(t, q, keys[:half]...)
	assert.Equal(t, Stats{Done: half, Submitted: half}, q.Stats())

	// Re-block the handler.
	unblock = make(chan struct{})

	// Start handling a fresh key, and ensure it really is blocked.
	assertKeyBlocked(t, q, keys[half])
	assert.Equal(t, Stats{Done: half, Submitted: half + 1}, q.Stats())

	// Ensure that the previous results are cached and available without delay.
	assertIdentityResults(t, q, keys[:half]...)

	// Finish handling the rest of the keys.
	close(unblock)
	assertIdentityResults(t, q, keys...)
	assert.Equal(t, Stats{Done: count, Submitted: count}, q.Stats())
}

func TestQueueConcurrencyLimit(t *testing.T) {
	const workerCount = 5
	const submitCount = workerCount * 10

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

	// Start up as many handlers as possible, and let them check for breaches
	// before they're blocked from returning.
	keys := makeIntKeys(submitCount)
	async(t, func() { q.GetAll(keys...) })
	forceRuntimeProgress()

	// Let them all finish...
	close(unblock)
	assertIdentityResults(t, q, keys...)
	assert.Equal(t, Stats{Done: submitCount, Submitted: submitCount}, q.Stats())

	// ...and ensure they all saw the limit respected.
	if breached.Load() {
		t.Errorf("queue breached limit of %d workers in flight", workerCount)
	}
}

func TestQueueOrdering(t *testing.T) {
	var handledOrder []int
	unblock := make(chan struct{})
	q := NewQueue(1, func(_ *QueueHandle, x int) (int, error) {
		<-unblock
		handledOrder = append(handledOrder, x)
		return x, nil
	})

	// Start a new blocked handler to force the queueing of subsequent keys.
	async(t, func() { q.Get(0) })
	forceRuntimeProgress()

	// Queue up some keys with various priorities.
	async(t, func() { q.GetAll(1, 2) })
	forceRuntimeProgress()
	async(t, func() { q.GetAllUrgent(-1, -2) })
	forceRuntimeProgress()
	async(t, func() { q.Get(3) })
	forceRuntimeProgress()
	async(t, func() { q.GetUrgent(-3) })
	forceRuntimeProgress()

	// Unblock all the handlers...
	close(unblock)
	keys := []int{-3, -2, -1, 0, 1, 2, 3}
	assertIdentityResults(t, q, keys...)

	// ...and ensure that everything was queued in the correct order:
	wantOrder := []int{
		// The initial blocked handler.
		0,
		// The urgent handlers, reversed from their queueing order but with keys in
		// a single GetAllUrgent call queued in the order provided.
		-3,
		-1, -2,
		// The normal handlers, in the order queued.
		1, 2,
		3,
	}
	assert.Equal(t, wantOrder, handledOrder)
}

func TestQueueReattachPriority(t *testing.T) {
	var workers [2]func(*QueueHandle)

	// Create a special handler that will detach as soon as it starts...
	ready0 := make(chan struct{})
	unblock0 := make(chan struct{})
	workers[0] = func(qh *QueueHandle) {
		qh.Detach()
		close(ready0)
		<-unblock0
		qh.Reattach()
	}

	// ...and a handler that will simply block.
	ready1 := make(chan struct{})
	unblock1 := make(chan struct{})
	workers[1] = func(qh *QueueHandle) {
		close(ready1)
		<-unblock1
	}

	var handleOrder []int
	q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
		if x >= 0 && x < len(workers) {
			workers[x](qh)
		}
		handleOrder = append(handleOrder, x)
		return x, nil
	})

	// Start the handler for 0 that will detach itself from the queue...
	async(t, func() { q.Get(0) })
	assertReceiveCount(t, 1, ready0)

	// ..and ensure that unrelated handlers are, in fact, unblocked.
	assertIdentityResults(t, q, -1)

	// Start the handler for 1 that will simply block, and queue up some extra
	// keys behind it.
	async(t, func() { q.GetAll(1, 2, 3) })
	assertReceiveCount(t, 1, ready1)

	// Allow the detached handler for 0 to reattach, and try to wait until it's
	// blocked on 1's completion.
	close(unblock0)
	forceRuntimeProgress()

	// Allow the handler for 1 to finish, unblocking everything else too.
	close(unblock1)
	assertIdentityResults(t, q, 0, 1, 2, 3)

	// Ensure the detached handler for 0 finished before the previously queued
	// keys.
	wantOrder := []int{-1, 1, 0, 2, 3}
	assert.Equal(t, wantOrder, handleOrder)
}

func TestQueueReattachConcurrency(t *testing.T) {
	const workerCount = 5
	const submitCount = workerCount * 10

	var (
		countAttached   atomic.Int32
		breached        atomic.Bool
		ready           = make(chan struct{})
		unblockReattach = make(chan struct{})
		unblockReturn   = make(chan struct{})
	)
	q := NewQueue(workerCount, func(qh *QueueHandle, x int) (int, error) {
		if !qh.Detach() {
			panic("did not actually detach from queue")
		}
		if qh.Detach() {
			panic("claimed to detach multiple times from queue")
		}
		ready <- struct{}{}
		<-unblockReattach
		qh.Reattach()
		if countAttached.Add(1) > workerCount {
			breached.Store(true)
		}
		defer countAttached.Add(-1)
		<-unblockReturn
		return x, nil
	})

	// Start up a bunch of handlers, and wait for all of them to detach.
	keys := makeIntKeys(submitCount)
	async(t, func() { q.GetAll(keys...) })
	assertReceiveCount(t, submitCount, ready)

	// Allow them all to start reattaching, and try to wait until all possible
	// reattachments have finished.
	close(unblockReattach)
	forceRuntimeProgress()

	// Let them all return...
	close(unblockReturn)
	assertIdentityResults(t, q, keys...)

	// ...and ensure none of the reattachers breached the limit.
	if breached.Load() {
		t.Errorf("queue breached limit of %d workers in flight during reattach", workerCount)
	}
}

func TestQueueDetachReturn(t *testing.T) {
	var (
		inflight        atomic.Int32
		breached        atomic.Bool
		ready           = make(chan struct{})
		unblockDetached = make(chan struct{})
		unblockAttached = make(chan struct{})
	)
	q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
		switch {
		case x < 0:
			qh.Detach()
			ready <- struct{}{}
			<-unblockDetached

		default:
			if inflight.Add(1) > 1 {
				breached.Store(true)
			}
			defer inflight.Add(-1)
			<-unblockAttached
		}

		return x, nil
	})

	// Start up multiple detached handlers that will never reattach.
	detachedKeys := []int{-2, -1}
	async(t, func() { q.GetAll(detachedKeys...) })
	assertReceiveCount(t, len(detachedKeys), ready)

	// Start up some normal handlers...
	attachedKeys := makeIntKeys(3 * len(detachedKeys))
	async(t, func() { q.GetAll(attachedKeys...) })

	// ...and ensure they really are blocked.
	assertKeyBlocked(t, q, attachedKeys[0])

	// Let the detached handlers finish, and push them forward if they're going to
	// incorrectly pick up keys rather than exit.
	close(unblockDetached)
	forceRuntimeProgress()

	// Unblock the rest of the handlers...
	close(unblockAttached)
	assertIdentityResults(t, q, attachedKeys...)

	// ...and ensure the limit wasn't breached.
	if breached.Load() {
		t.Error("queue breached limit of 1 worker in flight")
	}
}
