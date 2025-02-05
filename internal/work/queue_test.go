package work

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueBasic(t *testing.T) {
	q := NewQueue(1, func(_ *QueueHandle, x int) (int, error) { return x, nil })
	assertIdentityResults(t, q, 42)
	assert.Equal(t, Stats{Done: 1, Submitted: 1}, q.Stats())
}

func TestQueueGetAllError(t *testing.T) {
	const submitCount = 10
	q := NewNoValueQueue(submitCount, func(_ *QueueHandle, x int) error {
		if x >= 5 {
			return fmt.Errorf("%d", x)
		}
		return nil
	})

	keys := makeIntKeys(submitCount)
	_, err := q.GetAll(keys...)
	if err == nil || err.Error() != "5" {
		t.Errorf("GetAll() did not return expected error: got %v, want %q", err, "5")
	}

	for x := range submitCount {
		q.Get(x) // Wait for all handlers to finish.
	}
}

func TestQueuePanic(t *testing.T) {
	want := "the expected panic value"
	q := NewNoValueQueue(1, func(_ *QueueHandle, _ NoValue) error { panic(want) })
	defer func() {
		if got := recover(); got != want {
			t.Errorf("unexpected panic: got %v, want %v", got, want)
		}
	}()
	q.Get(NoValue{})
}

func TestQueueGoexitPropagation(t *testing.T) {
	q := NewNoValueQueue(1, func(_ *QueueHandle, _ NoValue) error {
		runtime.Goexit()
		return nil
	})
	// Goexit isn't allowed in tests outside of standard skip and fail functions,
	// so we need to get creative.
	done := make(chan bool)
	go func() {
		defer close(done)
		q.Get(NoValue{})
		done <- true
	}()
	if <-done {
		t.Fatalf("runtime.Goexit did not propagate")
	}
}

func TestQueueGoexitHandling(t *testing.T) {
	stepGoexit := make(chan struct{})
	q := NewQueue(1, func(_ *QueueHandle, x int) (int, error) {
		if x == 0 {
			<-stepGoexit
			<-stepGoexit
			runtime.Goexit()
		}
		return x, nil
	})

	// Start the handler that will Goexit, and ensure that it's blocked.
	async(t, func() { q.Get(0) })
	stepGoexit <- struct{}{}

	// Force some more handlers to queue up.
	async(t, func() { q.GetAll(1, 2) })
	forceRuntimeProgress()

	// Let all the handlers through, and ensure that the initial Goexit didn't
	// break the processing of other keys.
	close(stepGoexit)
	assertIdentityResults(t, q, 1, 2)
}

func TestQueueDeduplication(t *testing.T) {
	const count = 10
	const half = count / 2
	canReturn := make(chan struct{})
	q := NewQueue(0, func(_ *QueueHandle, x int) (int, error) {
		<-canReturn
		return x, nil
	})

	keys := makeIntKeys(count)

	// Handle and cache the first half of the keys.
	close(canReturn)
	assertIdentityResults(t, q, keys[:half]...)
	assert.Equal(t, Stats{Done: half, Submitted: half}, q.Stats())

	// Re-block the handler and start handling another key.
	canReturn = make(chan struct{})
	assertKeyBlocked(t, q, keys[half])
	assert.Equal(t, Stats{Done: half, Submitted: half + 1}, q.Stats())

	// Ensure that the previous results are cached.
	assertIdentityResults(t, q, keys[:half]...)

	// Finish handling the rest of the keys.
	close(canReturn)
	assertIdentityResults(t, q, keys...)
	assert.Equal(t, Stats{Done: count, Submitted: count}, q.Stats())
}

func TestQueueConcurrencyLimit(t *testing.T) {
	const (
		submitCount = 50
		workerCount = 10
	)
	var (
		inflight  atomic.Int32
		breached  atomic.Bool
		canReturn = make(chan struct{})
	)
	q := NewQueue(workerCount, func(_ *QueueHandle, x int) (int, error) {
		count := inflight.Add(1)
		defer inflight.Add(-1)
		if count > workerCount {
			breached.Store(true)
		}
		<-canReturn
		return x, nil
	})

	// Start up as many handlers as possible, let them check for breaches, then
	// block them from moving further.
	keys := makeIntKeys(submitCount)
	async(t, func() { q.GetAll(keys...) })
	forceRuntimeProgress()

	// Let them all finish, and make sure they all saw the limit respected.
	close(canReturn)
	assertIdentityResults(t, q, keys...)
	assert.Equal(t, Stats{Done: submitCount, Submitted: submitCount}, q.Stats())
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

	// Unblock all the handlers.
	close(unblock)
	assertIdentityResults(t, q, -3, -2, -1, 0, 1, 2, 3)

	// Ensure that everything was queued in the correct order.
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

	var (
		w0HasDetached = make(chan struct{})
		w0CanReattach = make(chan struct{})
	)
	workers[0] = func(qh *QueueHandle) {
		qh.Detach()
		close(w0HasDetached)
		<-w0CanReattach
		qh.Reattach()
	}

	var (
		w1HasStarted = make(chan struct{})
		w1CanReturn  = make(chan struct{})
	)
	workers[1] = func(qh *QueueHandle) {
		close(w1HasStarted)
		<-w1CanReturn
	}

	var handleOrder []int
	q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
		if x >= 0 && x < len(workers) {
			workers[x](qh)
		}
		handleOrder = append(handleOrder, x)
		return x, nil
	})

	// Create a detached handler for 0.
	async(t, func() { q.Get(0) })
	assertReceiveCount(t, 1, w0HasDetached)

	// Ensure that unrelated handlers are unblocked.
	assertIdentityResults(t, q, -1)

	// Start a non-detached handler for 1, and ensure that 2 and 3 are queued.
	async(t, func() { q.GetAll(1, 2, 3) })
	assertReceiveCount(t, 1, w1HasStarted)

	// Allow the detached handler for 0 to reattach, and try to force it to run
	// until it actually queues itself up for reattachment.
	close(w0CanReattach)
	forceRuntimeProgress()

	// Allow the handler for 1 to finish, unblocking all the rest as well.
	close(w1CanReturn)
	assertIdentityResults(t, q, 0, 1, 2, 3)

	lastHandled := handleOrder[len(handleOrder)-1]
	if lastHandled == 0 {
		t.Error("reattaching handler did not receive priority over new keys")
	}
}

func TestQueueReattachConcurrency(t *testing.T) {
	const (
		submitCount = 50
		workerCount = 10
	)
	var (
		countDetached atomic.Int32
		countAttached atomic.Int32
		breached      atomic.Bool
		hasDetached   = make(chan struct{})
		canReattach   = make(chan struct{})
		canReturn     = make(chan struct{})
	)
	q := NewQueue(workerCount, func(qh *QueueHandle, x int) (int, error) {
		if !qh.Detach() {
			panic("did not actually detach from queue")
		}
		if qh.Detach() {
			panic("claimed to detach multiple times from queue")
		}
		countDetached.Add(1)
		hasDetached <- struct{}{}

		<-canReattach
		qh.Reattach()
		count := countAttached.Add(1)
		defer countAttached.Add(-1)
		if count > workerCount {
			breached.Store(true)
		}
		<-canReturn
		return x, nil
	})

	// Start up a bunch of handlers, and wait for all of them to detach.
	keys := makeIntKeys(submitCount)
	async(t, func() { q.GetAll(keys...) })
	assertReceiveCount(t, submitCount, hasDetached)

	// Allow them to start reattaching, and force as many as possible to finish
	// reattaching and checking the reattach count.
	close(canReattach)
	forceRuntimeProgress()

	// Let them all finish and return, and make sure none saw too many handlers in
	// flight.
	close(canReturn)
	assertIdentityResults(t, q, keys...)
	assert.Equal(t, Stats{Done: submitCount, Submitted: submitCount}, q.Stats())
	if breached.Load() {
		t.Errorf("queue breached limit of %d workers in flight during reattach", workerCount)
	}
}

func TestQueueDetachReturn(t *testing.T) {
	var (
		inflight          atomic.Int32
		breached          atomic.Bool
		hasDetached       = make(chan struct{})
		detachedCanReturn = make(chan struct{})
		attachedCanReturn = make(chan struct{})
	)
	q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
		if x < 0 {
			qh.Detach()
			hasDetached <- struct{}{}
			<-detachedCanReturn
			return x, nil
		}
		count := inflight.Add(1)
		defer inflight.Add(-1)
		if count > 1 {
			breached.Store(true)
		}
		<-attachedCanReturn
		return x, nil
	})

	// Start up multiple detached handlers that will never reattach.
	detachedKeys := []int{-2, -1}
	async(t, func() { q.GetAll(detachedKeys...) })
	assertReceiveCount(t, len(detachedKeys), hasDetached)

	// Start up some normal handlers, and make sure they block.
	attachedKeys := makeIntKeys(3 * len(detachedKeys))
	async(t, func() { q.GetAll(attachedKeys...) })
	assertKeyBlocked(t, q, attachedKeys[0])

	// Let the detached handlers finish, and push them forward if they're going to
	// incorrectly pick up keys rather than exit.
	close(detachedCanReturn)
	assertIdentityResults(t, q, detachedKeys...)
	forceRuntimeProgress()

	// Unblock all handlers and make sure the limit wasn't breached.
	close(attachedCanReturn)
	assertIdentityResults(t, q, attachedKeys...)
	if breached.Load() {
		t.Error("queue breached limit of 1 worker in flight")
	}
}
