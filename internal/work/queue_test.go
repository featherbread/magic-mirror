//go:debug panicnil=1
package work_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/work"
	"github.com/ahamlinman/magic-mirror/internal/work/catch"
)

func TestQueueBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewQueue(func(_ *work.QueueHandle, x int) (int, error) {
			return x % 3, nil
		})
		q.Limit(1)

		got, err := q.Get(42)
		assert.NoError(t, err)
		assert.Equal(t, 0, got)
		assert.Equal(t, work.Stats{Handled: 1, Total: 1}, q.Stats())

		keys := makeIntKeys(6)
		collected, err := q.Collect(keys...)
		assert.NoError(t, err)
		want := []int{0, 1, 2, 0, 1, 2}
		assert.Equal(t, want, collected)
		assert.Equal(t, work.Stats{Handled: 7, Total: 7}, q.Stats())
	})
}

func TestQueueCollectOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const keyCount = 10
		q := work.NewQueue(func(_ *work.QueueHandle, x int) (int, error) {
			time.Sleep(rand.N(time.Duration(math.MaxInt64)))
			return x, nil
		})
		keys := makeIntKeys(keyCount)
		collected, err := q.Collect(keys...)
		assert.NoError(t, err)
		assert.Equal(t, keys, collected)
	})
}

func TestSetQueueError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewSetQueue(func(_ *work.QueueHandle, x int) error {
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

	// TODO: Functions in a table test are a code smell, but it's also important
	// to me that all forms of unwinding and retrieval are tested identically.
	// I have yet to find a construction I prefer.
	testCases := []struct {
		Description string
		Exit        func()
		Assert      func(*testing.T, catch.Result[struct{}])
	}{
		{
			Description: "panic with value",
			Exit:        func() { panic("test panic") },
			Assert: func(t *testing.T, result catch.Result[struct{}]) {
				assert.True(t, result.Panicked())
				assert.Equal(t, "test panic", result.Recovered())
			},
		},
		{
			Description: "panic(nil)",
			Exit:        func() { panic(someNilValue) },
			Assert: func(t *testing.T, result catch.Result[struct{}]) {
				assert.True(t, result.Panicked())
				assert.Nil(t, result.Recovered())
			},
		},
		{
			Description: "runtime.Goexit",
			Exit:        func() { runtime.Goexit(); panic("continued after Goexit") },
			Assert: func(t *testing.T, result catch.Result[struct{}]) {
				assert.True(t, result.Goexited())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				unblock := make(chan struct{})
				q := work.NewSetQueue(func(_ *work.QueueHandle, x int) error {
					if x == 0 {
						<-unblock
						tc.Exit()
						t.Error("test case failed to unwind from handler")
					}
					return nil
				})
				q.Limit(1)

				// Start the handler that will unwind, and ensure that it's blocked.
				q.Inform(0)
				synctest.Wait()

				// Force some more handlers to queue up...
				keys := []int{1, 2}
				q.Inform(keys...)
				synctest.Wait()

				// ...then let everything through.
				close(unblock)

				// Ensure the unwind didn't block the handling of those new keys.
				q.Collect(keys...)

				// Ensure we correctly pass the unwind through.
				tc.Assert(t, catch.Do(func() (_ struct{}, err error) {
					err = q.Get(0)
					return
				}))
				tc.Assert(t, catch.Do(func() (_ struct{}, err error) {
					err = q.Collect(1, 0, 2)
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
		q := work.NewSetQueue(func(_ *work.QueueHandle, x int) error {
			<-unblock
			return nil
		})

		// Handle an initial key.
		close(unblock)
		q.Get(initialCachedKey)
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

		// Ensure the previous key is cached and available without blocking.
		q.Get(initialCachedKey)

		// Finish handling the blocked key.
		close(unblock)
		q.Get(keyThatWillBlock)
		assert.Equal(t, work.Stats{Handled: 2, Total: 2}, q.Stats())
	})
}

func TestQueueConcurrencyLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			workerCount = 3
			keyCount    = workerCount * 3
		)
		var (
			inflight  atomic.Int32
			inflights = make(chan int, keyCount)
			unblock   = make(chan struct{})
		)
		q := work.NewSetQueue(func(_ *work.QueueHandle, x int) error {
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			<-unblock
			return nil
		})
		q.Limit(workerCount)

		// Start up as many handlers as possible, and wait for them to settle.
		keys := makeIntKeys(keyCount)
		q.Inform(keys...)
		synctest.Wait()

		// Let them all finish...
		close(unblock)
		q.Collect(keys...)
		assert.Equal(t, work.Stats{Handled: keyCount, Total: keyCount}, q.Stats())

		// ...and ensure the queue respected our limit.
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.LessOrEqual(t, maxInFlight, workerCount)
	})
}

func TestQueueOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var handledOrder []int
		unblock := make(chan struct{})
		q := work.NewSetQueue(func(_ *work.QueueHandle, x int) error {
			<-unblock
			handledOrder = append(handledOrder, x)
			return nil
		})
		q.Limit(1)

		// Start a new blocked handler to force the queueing of subsequent keys.
		q.Inform(0)
		synctest.Wait()

		// Add some keys at both the front and back of the queue.
		q.Inform(1, 2)
		q.InformFront(-1, -2)
		q.Inform(3)
		q.InformFront(-3)

		// Unblock all the handlers, and ensure they were queued in the right order.
		close(unblock)
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
		q.Collect(wantOrder...)
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
		q := work.NewSetQueue(func(qh *work.QueueHandle, x int) error {
			switch x {
			case keyThatWillDetach:
				qh.Detach()
				<-unblockReattacher
				qh.Reattach()
			case keyThatWillBlock:
				<-unblockBlocker
			}
			handleOrder = append(handleOrder, x)
			return nil
		})
		q.Limit(1)

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

func TestQueueMultiDetachReattach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := work.NewSetQueue(func(qh *work.QueueHandle, x int) error {
			assert.True(t, qh.Detach())  // First detach should work.
			assert.False(t, qh.Detach()) // Subsequent detaches should return false,
			assert.False(t, qh.Detach()) // and should not panic.
			qh.Reattach()                // Multiple reattaches should not panic.
			qh.Reattach()
			return nil
		})
		q.Get(0)
	})
}

func TestQueueReattachConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			workerCount = 3
			keyCount    = workerCount * 3
		)
		var (
			inflight        atomic.Int32
			inflights       = make(chan int, keyCount)
			unblockReattach = make(chan struct{})
			unblockReturn   = make(chan struct{})
		)
		q := work.NewSetQueue(func(qh *work.QueueHandle, x int) error {
			qh.Detach()
			<-unblockReattach
			qh.Reattach()
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			<-unblockReturn
			return nil
		})
		q.Limit(workerCount)

		// Start up as many handlers as possible, and wait for them to settle.
		keys := makeIntKeys(keyCount)
		q.Inform(keys...)
		synctest.Wait()

		// Let them start reattaching, and wait for things to settle.
		close(unblockReattach)
		synctest.Wait()

		// Let them all return...
		close(unblockReturn)
		q.Collect(keys...)

		// ...and ensure none of the reattachers breached the limit.
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.LessOrEqual(t, maxInFlight, workerCount)
	})
}

func TestQueueDetachReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			workerCount   = 2
			detachedCount = 5
			attachedCount = 5
		)
		var (
			inflight        atomic.Int32
			inflights       = make(chan int, detachedCount+attachedCount)
			unblockDetached = make(chan struct{})
			unblockAttached = make(chan struct{})
		)
		q := work.NewSetQueue(func(qh *work.QueueHandle, x int) error {
			if x < 0 {
				qh.Detach()
				<-unblockDetached
				return nil
			}
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			<-unblockAttached
			return nil
		})
		q.Limit(1)

		// Start up multiple detached handlers that will never reattach.
		detachedKeys := makeIntKeys(detachedCount + 1)[1:]
		for i := range detachedKeys {
			detachedKeys[i] *= -1
		}
		q.Inform(detachedKeys...)
		synctest.Wait()

		// Start up some normal handlers, and ensure they're really blocked.
		attachedKeys := makeIntKeys(attachedCount)
		attachedDone := promise(func() { q.Collect(attachedKeys...) })
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

		// Unblock the rest of the handlers, and ensure the limit wasn't breached.
		close(unblockAttached)
		<-attachedDone
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.LessOrEqual(t, maxInFlight, workerCount)
	})
}
