//go:debug panicnil=1
package parka_test

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ahamlinman/magic-mirror/internal/parka"
	"github.com/ahamlinman/magic-mirror/internal/parka/catch"
)

// someNilValue is an interface value intentionally kept unassigned, to test
// panic(nil) calls without triggering lints.
var someNilValue any

func ExampleMap() {
	m := parka.NewMap(func(_ *parka.Handle, x int) (int, error) {
		return x % 3, nil
	})
	mods, _ := m.Collect(0, 1, 2, 3, 4, 5)
	fmt.Println(mods)
	// Output: [0 1 2 0 1 2]
}

func ExampleMap_inform() {
	m := parka.NewMap(func(_ *parka.Handle, x int) (int, error) {
		if x >= 3 {
			return 0, errors.New("out of range")
		}
		return x * x, nil
	})

	// Use Inform to pre-submit keys for handling.
	m.Inform(0, 1, 2, 3, 4, 5)

	// Even though we use Get to obtain each key's error
	// (rather than coalesce to a single error with Collect),
	// all handlers are able to run concurrently.
	var errs []error
	for x := range 6 {
		if _, err := m.Get(x); err != nil {
			errs = append(errs, err)
		}
	}
	fmt.Println(len(errs))
	// Output: 3
}

func ExampleMap_limited() {
	var lsbMu parka.KeyMutex[int]
	m := parka.NewMap(func(ph *parka.Handle, x int) (int, error) {
		// Ensure one active handler per least-significant bit (LSB).
		lsb := x % 2
		lsbMu.LockDetached(ph, lsb)
		defer lsbMu.Unlock(lsb)

		n := 1
		for range x {
			n *= 2
		}
		return n, nil
	})

	// Limit to two active handlers (and any number of detached handlers).
	// Because of the KeyMutex, the two permitted handlers must be working
	// on different LSBs despite our queueing order below.
	m.Limit(2)

	exps, _ := m.Collect(0, 2, 4, 1, 3, 5)
	fmt.Println(exps)
	// Output: [1 4 16 2 8 32]
}

func TestMapBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := parka.NewMap(func(_ *parka.Handle, x int) (int, error) {
			return x % 3, nil
		})
		m.Limit(1)

		got, err := m.Get(42)
		assert.NoError(t, err)
		assert.Equal(t, 0, got)
		assert.Equal(t, parka.Stats{Handled: 1, Total: 1}, m.Stats())

		keys := makeIntKeys(6)
		collected, err := m.Collect(keys...)
		assert.NoError(t, err)
		want := []int{0, 1, 2, 0, 1, 2}
		assert.Equal(t, want, collected)
		assert.Equal(t, parka.Stats{Handled: 7, Total: 7}, m.Stats())
	})
}

func TestMapCollectOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const keyCount = 10
		m := parka.NewMap(func(_ *parka.Handle, x int) (int, error) {
			time.Sleep(rand.N(time.Duration(math.MaxInt64)))
			return x, nil
		})
		keys := makeIntKeys(keyCount)
		collected, err := m.Collect(keys...)
		assert.NoError(t, err)
		assert.Equal(t, keys, collected)
	})
}

func TestSetError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			if x%2 == 0 {
				return fmt.Errorf("%d", x)
			}
			return nil
		})
		assert.EqualError(t, s.Collect(1, 2, 3), "2")
		assert.EqualError(t, s.Get(2), "2")
	})
}

func TestMapGoexit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			if x == 0 {
				<-unblock
				exitRuntimeGoexit.Do()
				assert.Fail(t, "Test case failed to Goexit from handler")
			}
			return nil
		})
		s.Limit(1)

		// Start the handler that will unwind, and ensure that it's blocked.
		s.Inform(0)
		synctest.Wait()

		// Force some more handlers to queue up...
		keys := []int{1, 2}
		s.Inform(keys...)
		synctest.Wait()

		// ...then let everything through.
		close(unblock)

		// Ensure the unwind didn't block the handling of those new keys.
		s.Collect(keys...)

		// Ensure we correctly propagate the unwind when necessary.
		assertExitBehavior(t, exitRuntimeGoexit, func() error { return s.Get(0) })
		assertExitBehavior(t, exitRuntimeGoexit, func() error { return s.Collect(1, 0, 2) })
		assertExitBehavior(t, exitNilReturn, func() error { return s.Collect(1, 2) })
	})
}

func TestMapCaching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			initialCachedKey = iota
			keyThatWillBlock
		)

		unblock := make(chan struct{})
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			<-unblock
			return nil
		})

		// Handle an initial key.
		close(unblock)
		s.Get(initialCachedKey)
		assert.Equal(t, parka.Stats{Handled: 1, Total: 1}, s.Stats())

		// Re-block the handler.
		unblock = make(chan struct{})

		// Start handling a fresh key, and ensure it really is blocked.
		done := promise(func() { s.Get(keyThatWillBlock) })
		synctest.Wait()
		select {
		case <-done:
			assert.Fail(t, "Computation of key was not blocked")
		default:
			assert.Equal(t, parka.Stats{Handled: 1, Total: 2}, s.Stats())
		}

		// Ensure the previous key is cached and available without blocking.
		s.Get(initialCachedKey)

		// Finish handling the blocked key.
		close(unblock)
		s.Get(keyThatWillBlock)
		assert.Equal(t, parka.Stats{Handled: 2, Total: 2}, s.Stats())
	})
}

func TestMapOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var handledOrder []int
		unblock := make(chan struct{})
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			<-unblock
			handledOrder = append(handledOrder, x)
			return nil
		})
		s.Limit(1)

		// Start a new blocked handler to force the queueing of subsequent keys.
		s.Inform(0)
		synctest.Wait()

		// Add some keys at both the front and back of the queue.
		s.Inform(1, 2)
		s.InformFront(-1, -2)
		s.Inform(3)
		s.InformFront(-3)

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
		s.Collect(wantOrder...)
		assert.Equal(t, wantOrder, handledOrder)
	})
}

func TestMapReattachPriority(t *testing.T) {
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
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			switch x {
			case keyThatWillDetach:
				ph.Detach()
				<-unblockReattacher
				ph.Reattach()
			case keyThatWillBlock:
				<-unblockBlocker
			}
			handleOrder = append(handleOrder, x)
			return nil
		})
		s.Limit(1)

		// Start the handler that will detach itself, and ensure unrelated handlers
		// are unblocked.
		s.Inform(keyThatWillDetach)
		synctest.Wait()
		s.Get(unrelatedKeyThatGoesFirst)
		synctest.Wait()

		// Start the blocking handler, along with another that will queue behind it.
		s.Inform(keyThatWillBlock, unrelatedKeyThatGoesLast)
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
		s.Collect(wantOrder...)
		assert.Equal(t, wantOrder, handleOrder)
	})
}

func TestMapMultiDetachReattach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			assert.True(t, ph.Detach(), "First Detach() claimed to do nothing")
			assert.False(t, ph.Detach(), "Second Detach() claimed to detach")
			assert.False(t, ph.Detach(), "Third Detach() claimed to detach")
			ph.Reattach()
			ph.Reattach()
			return nil
		})
		s.Get(0)
	})
}

func TestMapHandleConcurrencyTorture(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const iterations = 10
		var (
			inflightFinal atomic.Int32
			unblockFinal  = make(chan struct{})
		)
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			switch {
			case x >= 0: // parka.Handle concurrency torture test.
				ch := make(chan struct{}, iterations) // TODO: sync.WaitGroup is flaky here.
				for range iterations {
					switch rand.N(2) {
					case 0:
						go func() { ph.Detach(); ch <- struct{}{} }()
					case 1:
						go func() { ph.Reattach(); ch <- struct{}{} }()
					}
				}
				for range iterations {
					<-ch
				}

			case x < 0: // Final handler concurrency invariant test.
				inflightFinal.Add(1)
				defer inflightFinal.Add(-1)
				<-unblockFinal
			}

			return nil
		})

		// Run the initial torture test, ensuring we don't deadlock or crash.
		keys := makeIntKeys(10)
		s.Limit(2)
		s.Collect(keys...)

		// Start up two handlers within the concurrency limit.
		s.Inform(-1, -2)
		synctest.Wait()
		assert.Equal(t, 2, int(inflightFinal.Load()))

		// Start up one more, and ensure it's blocked.
		done := promise(func() { s.Get(-3) })
		synctest.Wait()
		select {
		case <-done:
			assert.Fail(t, "Computation of key was not blocked")
		default:
		}

		// Unblock those final handlers.
		close(unblockFinal)
	})
}

func TestMapReattachConcurrency(t *testing.T) {
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
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			ph.Detach()
			<-unblockReattach
			ph.Reattach()
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			<-unblockReturn
			return nil
		})
		s.Limit(workerCount)

		// Start up as many handlers as possible, and wait for them to settle.
		keys := makeIntKeys(keyCount)
		s.Inform(keys...)
		synctest.Wait()

		// Let them start reattaching, and wait for things to settle.
		close(unblockReattach)
		synctest.Wait()

		// Let them all return...
		close(unblockReturn)
		s.Collect(keys...)

		// ...and ensure none of the reattachers breached the limit.
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.LessOrEqual(t, maxInFlight, workerCount,
			"Breached concurrency limit")
	})
}

func TestMapDetachAndFinish(t *testing.T) {
	exitBehaviors := []exitBehavior{
		exitNilReturn,
		exitRuntimeGoexit,
	}
	for _, exit := range exitBehaviors {
		t.Run(exit.String(), func(t *testing.T) {
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
				s := parka.NewSet(func(ph *parka.Handle, x int) error {
					if x < 0 {
						ph.Detach()
						<-unblockDetached
						return exit.Do()
					}
					inflights <- int(inflight.Add(1))
					defer inflight.Add(-1)
					<-unblockAttached
					return nil
				})
				s.Limit(1)

				// Start up multiple detached handlers that will never reattach.
				detachedKeys := makeIntKeys(detachedCount + 1)[1:]
				for i := range detachedKeys {
					detachedKeys[i] *= -1
				}
				s.Inform(detachedKeys...)
				synctest.Wait()

				// Start up some normal handlers, and ensure they're really blocked.
				attachedKeys := makeIntKeys(attachedCount)
				attachedDone := promise(func() { s.Collect(attachedKeys...) })
				synctest.Wait()
				select {
				case <-attachedDone:
					assert.Fail(t, "Computation of keys was not blocked")
				default:
				}

				// Let the detached handlers finish, and push them forward if they're
				// going to incorrectly pick up keys rather than exit.
				close(unblockDetached)
				synctest.Wait()

				// Unblock the rest of the handlers, and ensure the limit was respected.
				close(unblockAttached)
				<-attachedDone
				close(inflights)
				maxInFlight := maxOfChannel(inflights)
				assert.LessOrEqual(t, maxInFlight, workerCount,
					"Breached concurrency limit")

				// Ensure the detached keys used the correct exit behavior.
				assertExitBehavior(t, exit, func() error { return s.Get(detachedKeys[0]) })
			})
		})
	}
}

func TestMapHandlerEscape(t *testing.T) {
	testCases := []string{"Detach", "Reattach", "Detach+Detach", "Detach+Reattach"}
	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var handle *parka.Handle
				s := parka.NewSet(func(h *parka.Handle, _ struct{}) error {
					if strings.HasPrefix(tc, "Detach+") {
						h.Detach()
					}
					handle = h
					return nil
				})

				s.Get(struct{}{})
				require.NotNil(t, handle, "Failed to set handle")

				result := catch.Do(func() ([]reflect.Value, error) {
					return reflect.ValueOf(handle).
						MethodByName(strings.TrimPrefix(tc, "Detach+")).
						Call(nil), nil
				})
				assert.True(t, result.Panicked(), "Handle call did not panic")
				assert.Contains(t, result.Recovered(), "outside handler lifetime")
			})
		})
	}
}

func TestMapLimitBasic(t *testing.T) {
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
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			<-unblock
			return nil
		})
		s.Limit(workerCount)

		// Start up as many handlers as possible, and wait for them to settle.
		keys := makeIntKeys(keyCount)
		s.Inform(keys...)
		synctest.Wait()

		// Let them all finish...
		close(unblock)
		s.Collect(keys...)
		assert.Equal(t, parka.Stats{Handled: keyCount, Total: keyCount}, s.Stats())

		// ...and ensure the concurrency limit was respected.
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.LessOrEqual(t, maxInFlight, workerCount,
			"Breached concurrency limit")
	})
}

func TestMapLimitIncrease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type Key struct {
			Index  int
			Detach bool
		}
		const (
			detachedCount = 3
			blockerCount  = 1
			attachedCount = 10
		)
		var (
			inflight        atomic.Int32
			detached        atomic.Int32
			inflights       = make(chan int, detachedCount+blockerCount+attachedCount)
			unblockReattach = make(chan struct{})
			unblockReturn   = make(chan struct{})
		)
		s := parka.NewSet(func(ph *parka.Handle, k Key) error {
			if k.Detach {
				ph.Detach()
				detached.Add(1)
				<-unblockReattach
				ph.Reattach()
				detached.Add(-1)
			}
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			<-unblockReturn
			return nil
		})

		// Start up a few detached handlers.
		detachedKeys := make([]Key, detachedCount)
		for i := range detachedKeys {
			detachedKeys[i] = Key{Index: i, Detach: true}
		}
		s.Inform(detachedKeys...)
		synctest.Wait()
		assert.Equal(t, detachedCount, int(detached.Load()),
			"Missing some detached handlers")

		// Set the limit to 1, and start a blocking handler.
		s.Limit(1)
		blockingKey := Key{Index: -1, Detach: false}
		s.Inform(blockingKey)
		synctest.Wait()
		assert.Equal(t, 1, int(inflight.Load()),
			"Missing handler for blocking key")

		// Queue up a few regular handlers, and make sure they're blocked.
		attachedKeys := make([]Key, attachedCount)
		for i := range attachedKeys {
			attachedKeys[i] = Key{Index: i, Detach: false}
		}
		done := promise(func() { s.Collect(attachedKeys...) })
		synctest.Wait()
		select {
		case <-done:
			assert.Fail(t, "Computation of keys was not blocked")
		default:
		}

		// Increase the limit to 2, and make sure a reattacher takes priority.
		close(unblockReattach)
		synctest.Wait()
		s.Limit(2)
		synctest.Wait()
		assert.Equal(t, detachedCount-1, int(detached.Load()),
			"Reattacher did not have priority on limit increase")
		assert.Equal(t, 2, int(inflight.Load()),
			"Wrong number of handlers in flight")

		// Let all of the detached handlers in, along with some regular keys.
		limit := blockerCount + detachedCount + 2
		s.Limit(limit)
		synctest.Wait()
		assert.Equal(t, 0, int(detached.Load()),
			"Not all detachers reattached")
		assert.Equal(t, limit, int(inflight.Load()),
			"Wrong number of handlers in flight")

		// Let in some additional keys while we have no pending reattachers.
		limit += 2
		s.Limit(limit)
		synctest.Wait()
		assert.Equal(t, limit, int(inflight.Load()),
			"Wrong number of handlers in flight")

		// Let all handlers through, and ensure the limit was respected.
		close(unblockReturn)
		s.Get(Key{Index: -1, Detach: false})
		s.Collect(detachedKeys...)
		s.Collect(attachedKeys...)
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.LessOrEqual(t, maxInFlight, limit,
			"Breached concurrency limit")
	})
}

func TestMapLimitIncreaseMax(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const keyCount = 10
		var (
			active  atomic.Int32
			unblock = make(chan struct{})
		)
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			active.Add(1)
			defer active.Add(-1)
			if x == 0 {
				ph.Detach()
				<-unblock
				ph.Reattach()
			}
			<-unblock
			return nil
		})

		// Start up some blocked handlers (one detached + one attached),
		// and let them settle.
		keys := makeIntKeys(keyCount)
		s.Limit(1)
		s.Inform(keys...)
		synctest.Wait()
		assert.Equal(t, 2, int(active.Load()),
			"Missing some expected handlers")

		// Increase the concurrency limit well beyond the number of keys,
		// and make sure we don't panic or crash in some way.
		s.Limit(math.MaxInt)
		synctest.Wait()
		close(unblock)
		s.Collect(keys...)
	})
}

func TestMapLimitDecrease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			initialKeyCount = 5
			extraKeyCount   = 10
		)
		var (
			inflight        atomic.Int32
			detached        atomic.Int32
			extraInflights  = make(chan int, extraKeyCount)
			unblockReattach = make(chan struct{})
			unblockReturn   = make(chan struct{})
		)
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			if x < initialKeyCount && x%2 == 0 {
				ph.Detach()
				detached.Add(1)
				<-unblockReattach
				ph.Reattach()
				detached.Add(-1)
			}

			current := int(inflight.Add(1))
			defer inflight.Add(-1)
			if x >= initialKeyCount {
				extraInflights <- current
			}

			<-unblockReturn
			return nil
		})

		allKeys := makeIntKeys(initialKeyCount + extraKeyCount)
		initialKeys, extraKeys := allKeys[:initialKeyCount], allKeys[initialKeyCount:]

		// Start the handlers for our initial keys (some detached, some attached).
		s.Inform(initialKeys...)
		synctest.Wait()
		assert.Greater(t, int(detached.Load()), 0,
			"Some handlers did not detach")
		assert.Equal(t, initialKeyCount, int(detached.Load())+int(inflight.Load()),
			"Wrong number of handlers in flight")

		// Decrease the limit to 1, and ensure no existing handlers are affected.
		s.Limit(1)
		synctest.Wait()
		assert.Equal(t, initialKeyCount, int(detached.Load())+int(inflight.Load()),
			"Some handlers exited after limit decrease")

		// Force all currently detached handlers to finish before any new keys can
		// be handled.
		close(unblockReattach)
		synctest.Wait()

		// Then, add some new keys that can only be handled under the new limit.
		s.Inform(extraKeys...)
		synctest.Wait()

		// Let all of the handlers through.
		close(unblockReturn)
		s.Collect(initialKeys...)
		s.Collect(extraKeys...)

		// Ensure every handler started under the new limit saw itself as the only
		// active handler.
		close(extraInflights)
		assert.Equal(t, extraKeyCount, len(extraInflights))
		for x := range extraInflights {
			if !assert.Equal(t, 1, x,
				"Handler started under decreased limit saw another active") {
				return
			}
		}
	})
}
