//go:debug panicnil=1
package work_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/work"
	"github.com/ahamlinman/magic-mirror/internal/work/catch"
)

func TestQueueBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewQueue(1, func(_ *work.QueueHandle, x int) (int, error) {
			return x, nil
		})
		got, err := q.Get(42)
		assert.NoError(t, err)
		assert.Equal(t, 42, got)
		assert.Equal(t, work.Stats{Handled: 1, Total: 1}, q.Stats())
	})
}

func TestSetQueueError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewSetQueue(0, func(_ *work.QueueHandle, x int) error {
			if x%2 == 0 {
				return fmt.Errorf("%d", x)
			}
			return nil
		})
		assert.EqualError(t, q.Collect(1, 2, 3), "2")
		assert.EqualError(t, q.Get(2), "2")
	})
}

func TestQueueUnwind(t *testing.T) {
	var someNilValue any // Never assigned; quiets lints for literal panic(nil).

	testCases := []struct {
		Description string
		Exit        func()                              // TODO: Code smell; approach with caution.
		Assert      func(*testing.T, catch.Result[int]) // TODO: EXTREME code smell; approach with caution.
	}{
		{
			Description: "panic with value",
			Exit:        func() { panic("test panic") },
			Assert: func(t *testing.T, result catch.Result[int]) {
				assert.True(t, result.Panicked())
				assert.Equal(t, "test panic", result.Recovered())
			},
		},
		{
			Description: "panic(nil)",
			Exit:        func() { panic(someNilValue) },
			Assert: func(t *testing.T, result catch.Result[int]) {
				assert.True(t, result.Panicked())
				assert.Nil(t, result.Recovered())
			},
		},
		{
			Description: "runtime.Goexit",
			Exit:        func() { runtime.Goexit(); panic("continued after Goexit") },
			Assert: func(t *testing.T, result catch.Result[int]) {
				assert.True(t, result.Goexited())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				unblock := make(chan struct{})
				q := work.NewQueue(1, func(_ *work.QueueHandle, x int) (int, error) {
					if x == 0 {
						<-unblock
						tc.Exit()
						t.Error("test case failed to unwind from handler")
					}
					return x, nil
				})

				// Start the handler that will unwind, and ensure that it's blocked.
				q.Inform(0)
				synctest.Wait()

				// Force some more handlers to queue up...
				keys := []int{1, 2}
				q.Inform(keys...)
				synctest.Wait()

				// ...then let everything through.
				close(unblock)

				// Ensure that the unwind didn't break the handling of those new keys.
				got, _ := q.Collect(keys...)
				assert.Equal(t, keys, got)

				// Ensure that we correctly pass the unwind through.
				tc.Assert(t, catch.Do(func() (int, error) {
					return q.Get(0)
				}))
				tc.Assert(t, catch.Do(func() (_ int, err error) {
					_, err = q.Collect(1, 0, 2)
					return
				}))
			})
		})
	}
}

func TestQueueCaching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			initialCachedKey = iota
			keyThatWillBlock
		)

		unblock := make(chan struct{})
		q := work.NewQueue(0, func(_ *work.QueueHandle, x int) (int, error) {
			<-unblock
			return x, nil
		})

		// Handle an initial key.
		close(unblock)
		got, _ := q.Get(initialCachedKey)
		assert.Equal(t, initialCachedKey, got)
		assert.Equal(t, work.Stats{Handled: 1, Total: 1}, q.Stats())

		// Re-block the handler.
		unblock = make(chan struct{})

		// Start handling a fresh key, and ensure it really is blocked.
		done := promise(func() { q.Get(keyThatWillBlock) })
		synctest.Wait()
		select {
		case <-done:
			t.Error("computation of key was not blocked")
		default:
			assert.Equal(t, work.Stats{Handled: 1, Total: 2}, q.Stats())
		}

		// Ensure the previous key is cached and available without delay.
		got, _ = q.Get(initialCachedKey)
		assert.Equal(t, initialCachedKey, got)

		// Finish handling the blocked key.
		close(unblock)
		keys := []int{initialCachedKey, keyThatWillBlock}
		collected, _ := q.Collect(keys...)
		assert.Equal(t, keys, collected)
		assert.Equal(t, work.Stats{Handled: 2, Total: 2}, q.Stats())
	})
}

func TestQueueConcurrencyLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			workerCount = 3
			keyCount    = workerCount + 1
		)
		var (
			inflight  atomic.Int32
			inflights = make(chan int32, keyCount)
			unblock   = make(chan struct{})
		)
		q := work.NewQueue(workerCount, func(_ *work.QueueHandle, x int) (int, error) {
			inflights <- inflight.Add(1)
			defer inflight.Add(-1)
			<-unblock
			return x, nil
		})

		// Start up as many handlers as possible, and wait for them to settle.
		keys := makeIntKeys(keyCount)
		q.Inform(keys...)
		synctest.Wait()

		// Let them all finish...
		close(unblock)
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)
		assert.Equal(t, work.Stats{Handled: keyCount, Total: keyCount}, q.Stats())

		// ...and ensure the queue respected our limit.
		close(inflights)
		var maxInFlight int
		for count := range inflights {
			maxInFlight = max(maxInFlight, int(count))
		}
		assert.LessOrEqual(t, maxInFlight, workerCount)
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
		q.Inform(0)
		synctest.Wait()

		// Add some keys at both the front and back of the queue.
		q.Inform(1, 2)
		q.InformFront(-1, -2)
		q.Inform(3)
		q.InformFront(-3)

		// Unblock all the handlers...
		close(unblock)
		keys := []int{-3, -2, -1, 0, 1, 2, 3}
		got, _ := q.Collect(keys...)
		assert.Equal(t, keys, got)

		// ...and ensure that everything was queued in the correct order:
		wantOrder := []int{
			// The initial blocked handler.
			0,
			// Front-queued keys, with the call order reversed but with keys in a
			// single call queued in the order provided.
			-3,
			-1, -2,
			// Standard keys, in the order queued.
			1, 2,
			3,
		}
		assert.Equal(t, wantOrder, handledOrder)
	})
}

func TestQueueReattachPriority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			unrelatedKeyThatGoesFirst = iota
			keyThatWillDetach
			keyThatWillBlock
			unrelatedKeyThatGoesLast
		)
		var (
			handleOrder       []int
			unblockReattacher = make(chan struct{})
			unblockBlocker    = make(chan struct{})
		)
		q := work.NewQueue(1, func(qh *work.QueueHandle, x int) (int, error) {
			switch x {
			case keyThatWillDetach:
				qh.Detach()
				<-unblockReattacher
				qh.Reattach()
			case keyThatWillBlock:
				<-unblockBlocker
			}
			handleOrder = append(handleOrder, x)
			return x, nil
		})

		// Start the handler that will detach itself from the queue, and ensure
		// unrelated handlers are unblocked.
		q.Inform(keyThatWillDetach)
		synctest.Wait()
		q.Get(unrelatedKeyThatGoesFirst)
		synctest.Wait()

		// Start the blocking handler, along with another that will queue behind it.
		q.Inform(keyThatWillBlock, unrelatedKeyThatGoesLast)
		synctest.Wait()

		// Let the detached handler reattach. The blocking handler will hold it up.
		close(unblockReattacher)
		synctest.Wait()

		// Unblock everything, and make sure the detacher gets ahead of the
		// unrelated queued key.
		close(unblockBlocker)
		wantOrder := []int{
			unrelatedKeyThatGoesFirst,
			keyThatWillBlock,
			keyThatWillDetach,
			unrelatedKeyThatGoesLast,
		}
		q.Collect(wantOrder...)
		assert.Equal(t, wantOrder, handleOrder)
	})
}

func TestQueueReattachConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workerCount = 5
		const keyCount = workerCount * 10

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
		keys := makeIntKeys(keyCount)
		q.Inform(keys...)
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
		q.Inform(detachedKeys...)
		synctest.Wait()

		// Start up some normal handlers...
		attachedKeys := makeIntKeys(3 * len(detachedKeys))
		attachedDone := promise(func() { q.Collect(attachedKeys...) })

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
