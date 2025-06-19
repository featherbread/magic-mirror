package work_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/work"
)

func TestQueueBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewQueue(1, func(_ *work.QueueHandle, x int) (int, error) {
			return x, nil
		})
		got, err := q.Get(42)
		assert.NoError(t, err)
		assert.Equal(t, 42, got)
		assert.Equal(t, work.Stats{Done: 1, Submitted: 1}, q.Stats())
	})
}

func TestQueueCollectError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		keys := makeIntKeys(10)
		q := work.NewSetQueue(0, func(_ *work.QueueHandle, x int) error {
			if x >= 5 {
				return fmt.Errorf("%d", x)
			}
			return nil
		})
		err := q.Collect(keys...)
		assert.EqualError(t, err, "5")
	})
}

func TestQueuePanicPropagation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const want = "the expected panic value"
		q := work.NewSetQueue(1, func(_ *work.QueueHandle, _ int) error {
			panic(want)
		})
		defer func() {
			got := recover()
			assert.Equal(t, want, got)
		}()
		q.Get(0)
		t.Fatal("panic did not propagate")
	})
}

func TestQueueGoexitPropagation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewSetQueue(1, func(_ *work.QueueHandle, _ int) error {
			runtime.Goexit()
			return nil
		})
		done := make(chan bool)
		go func() {
			defer close(done)
			q.Get(0)
			done <- true
		}()
		if <-done {
			t.Error("runtime.Goexit did not propagate")
		}
	})
}

func TestQueueGoexitHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stepGoexit := make(chan struct{})
		q := work.NewQueue(1, func(_ *work.QueueHandle, x int) (int, error) {
			if x == 0 {
				for range stepGoexit {
				}
				runtime.Goexit()
			}
			return x, nil
		})

		// Start the handler that will Goexit, and ensure that it's blocked.
		q.Submit(0)
		stepGoexit <- struct{}{}

		// Force some more handlers to queue up...
		keys := []int{1, 2}
		q.Submit(keys...)

		// ...then let them through.
		close(stepGoexit)

		// Ensure that the Goexit didn't break the handling of those new keys.
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)
	})
}

func TestQueueDeduplication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		q := work.NewQueue(0, func(_ *work.QueueHandle, x int) (int, error) {
			<-unblock
			return x, nil
		})

		const count = 10
		const half = count / 2
		keys := makeIntKeys(count)

		// Handle and cache the first half of the keys.
		close(unblock)
		got, _ := q.Collect(keys[:half]...)
		assert.Equal(t, keys[:half], got)
		assert.Equal(t, work.Stats{Done: half, Submitted: half}, q.Stats())

		// Re-block the handler.
		unblock = make(chan struct{})

		// Start handling a fresh key...
		done := make(chan struct{})
		go func() {
			defer close(done)
			q.Get(keys[half])
		}()

		// ...and ensure it really is blocked.
		synctest.Wait()
		select {
		case <-done:
			t.Error("computation of key was not blocked")
		default:
			assert.Equal(t, work.Stats{Done: half, Submitted: half + 1}, q.Stats())
		}

		// Ensure that the previous results are cached and available without delay.
		got, _ = q.Collect(keys[:half]...)
		assert.Equal(t, keys[:half], got)

		// Finish handling the rest of the keys.
		close(unblock)
		got, _ = q.Collect(keys...)
		assert.Equal(t, keys, got)
		assert.Equal(t, work.Stats{Done: count, Submitted: count}, q.Stats())
	})
}

func TestQueueConcurrencyLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workerCount = 5
		const submitCount = workerCount * 10

		var (
			inflight atomic.Int32
			breached atomic.Bool
			unblock  = make(chan struct{})
		)
		q := work.NewQueue(workerCount, func(_ *work.QueueHandle, x int) (int, error) {
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
		q.Submit(keys...)
		synctest.Wait()

		// Let them all finish...
		close(unblock)
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)
		assert.Equal(t, work.Stats{Done: submitCount, Submitted: submitCount}, q.Stats())

		// ...and ensure they all saw the limit respected.
		if breached.Load() {
			t.Errorf("queue breached limit of %d workers in flight", workerCount)
		}
	})
}

func TestQueueOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var handledOrder []int
		unblock := make(chan struct{})
		q := work.NewQueue(1, func(_ *work.QueueHandle, x int) (int, error) {
			<-unblock
			handledOrder = append(handledOrder, x)
			return x, nil
		})

		// Start a new blocked handler to force the queueing of subsequent keys.
		q.Submit(0)
		synctest.Wait()

		// Queue up some keys with various priorities.
		q.Submit(1, 2)
		q.SubmitUrgent(-1, -2)
		q.Submit(3)
		q.SubmitUrgent(-3)

		// Unblock all the handlers...
		close(unblock)
		keys := []int{-3, -2, -1, 0, 1, 2, 3}
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)

		// ...and ensure that everything was queued in the correct order:
		wantOrder := []int{
			// The initial blocked handler.
			0,
			// The urgent handlers, reversed from their queueing order but with keys
			// in a single SubmitUrgent call queued in the order provided.
			-3,
			-1, -2,
			// The normal handlers, in the order queued.
			1, 2,
			3,
		}
		assert.Equal(t, wantOrder, handledOrder)
	})
}

func TestQueueReattachPriority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var workers [2]func(*work.QueueHandle)

		// Create a special handler that will detach as soon as it starts...
		unblock0 := make(chan struct{})
		workers[0] = func(qh *work.QueueHandle) {
			qh.Detach()
			<-unblock0
			qh.Reattach()
		}

		// ...and a handler that will simply block.
		unblock1 := make(chan struct{})
		workers[1] = func(qh *work.QueueHandle) { <-unblock1 }

		var handleOrder []int
		q := work.NewQueue(1, func(qh *work.QueueHandle, x int) (int, error) {
			if x >= 0 && x < len(workers) {
				workers[x](qh)
			}
			handleOrder = append(handleOrder, x)
			return x, nil
		})

		// Start the handler for 0 that will detach itself from the queue...
		go func() { q.Get(0) }()
		synctest.Wait()

		// ...and ensure that unrelated handlers are, in fact, unblocked.
		q.Get(-1)

		// Start the handler for 1 that will simply block, and queue up some extra
		// keys behind it.
		q.Submit(1, 2, 3)
		synctest.Wait()

		// Allow the detached handler for 0 to reattach, and wait until it's durably
		// blocked on 1's completion.
		close(unblock0)
		synctest.Wait()

		// Allow the handler for 1 to finish, unblocking everything else too.
		close(unblock1)
		keys := []int{0, 1, 2, 3}
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)

		// Ensure the detached handler for 0 finished in the correct order relative
		// to others.
		wantOrder := []int{-1, 1, 0, 2, 3}
		assert.Equal(t, wantOrder, handleOrder)
	})
}

func TestQueueReattachConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workerCount = 5
		const submitCount = workerCount * 10

		var (
			countAttached   atomic.Int32
			breached        atomic.Bool
			unblockReattach = make(chan struct{})
			unblockReturn   = make(chan struct{})
		)
		q := work.NewQueue(workerCount, func(qh *work.QueueHandle, x int) (int, error) {
			if !qh.Detach() {
				panic("did not actually detach from queue")
			}
			if qh.Detach() {
				panic("claimed to detach multiple times from queue")
			}
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
		q.Submit(keys...)
		synctest.Wait()

		// Allow them all to start reattaching, and wait until all possible
		// reattachments have finished.
		close(unblockReattach)
		synctest.Wait()

		// Let them all return...
		close(unblockReturn)
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)

		// ...and ensure none of the reattachers breached the limit.
		if breached.Load() {
			t.Errorf("queue breached limit of %d workers in flight during reattach", workerCount)
		}
	})
}

func TestQueueDetachReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			inflight        atomic.Int32
			breached        atomic.Bool
			unblockDetached = make(chan struct{})
			unblockAttached = make(chan struct{})
		)
		q := work.NewQueue(1, func(qh *work.QueueHandle, x int) (int, error) {
			if x < 0 {
				qh.Detach()
				<-unblockDetached
			} else {
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
		q.Submit(detachedKeys...)
		synctest.Wait()

		// Start up some normal handlers...
		attachedDone := make(chan struct{})
		attachedKeys := makeIntKeys(3 * len(detachedKeys))
		go func() {
			defer close(attachedDone)
			q.Collect(attachedKeys...)
		}()

		// ...and ensure they really are blocked.
		synctest.Wait()
		select {
		case <-attachedDone:
			t.Error("computation of keys was not blocked")
		default:
		}

		// Let the detached handlers finish, and push them forward if they're going
		// to incorrectly pick up keys rather than exit.
		close(unblockDetached)
		synctest.Wait()

		// Unblock the rest of the handlers...
		close(unblockAttached)
		got, _ := q.Collect(attachedKeys...)
		assert.Equal(t, attachedKeys, got)

		// ...and ensure the limit wasn't breached.
		if breached.Load() {
			t.Error("queue breached limit of 1 worker in flight")
		}
	})
}

func makeIntKeys(n int) (keys []int) {
	keys = make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	return
}
