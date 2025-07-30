//go:debug panicnil=1
package parka_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/featherbread/magic-mirror/internal/parka"
	"github.com/featherbread/magic-mirror/internal/parka/catch"
)

func ExampleMap() {
	m := parka.NewMap(func(_ *parka.Handle, x int) (int, error) {
		return x % 3, nil
	})
	mods, _ := m.Collect(0, 1, 2, 3, 4, 5)
	fmt.Println(mods)
	// Output: [0 1 2 0 1 2]
}

func ExampleMap_context() {
	const (
		concurrency = 3
		timeout     = 10 * time.Second
	)

	// Create a parent context to support canceling in-flight handlers.
	ctx, cancel := context.WithCancel(context.Background())

	// Use that context in a map's handler, making child contexts as needed.
	codes := parka.NewMap(func(_ *parka.Handle, url string) (int, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return headResponseCode(ctx, url)
	})

	// If Collect returns early on error, use Cleanup to dequeue pending work,
	// cancel the context, and wait for in-flight handlers.
	defer codes.Cleanup(cancel)

	codes.Limit(concurrency)
	codes.Collect(
		"https://www.example.com/",
		"https://www.example.net/",
		// ...
	)
}

func headResponseCode(ctx context.Context, url string) (code int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	resp.Body.Close()
	return resp.StatusCode, nil
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
				runtime.Goexit()
				assert.Fail(t, "Test case failed to Goexit from handler")
			}
			return nil
		})
		s.Limit(1)

		// Start the handler that will Goexit, and ensure that it's blocked.
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

		// Ensure we panic when retrieving the Goexited handler's result.
		getResult := catch.Do(func() (any, error) { return nil, s.Get(0) })
		assert.True(t, getResult.Panicked(), "Get() did not panic")
		assert.ErrorIs(t, getResult.Recovered().(error), parka.ErrHandlerGoexit)

		collectResult := catch.Do(func() (any, error) { return nil, s.Collect(1, 0, 2) })
		assert.True(t, collectResult.Panicked(), "Collect() did not panic")
		assert.ErrorIs(t, collectResult.Recovered().(error), parka.ErrHandlerGoexit)
	})
}

// someNilValue is never assigned. It quiets lints for literal panic(nil) calls.
var someNilValue any

func TestMapPanicNil(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const keyCount = 1000
		var (
			inflight  atomic.Int32
			inflights = make(chan int, keyCount)
		)
		s := parka.NewSet(func(_ *parka.Handle, _ int) error {
			inflights <- int(inflight.Add(1))
			defer inflight.Add(-1)
			panic(someNilValue)
		})
		s.Limit(1)

		keys := makeIntKeys(keyCount)
		s.Inform(keys...)
		for _, key := range keys {
			func() {
				defer func() { recover() }() // This looks like Goexit, so Get panics.
				s.Get(key)
			}()
		}

		// Make sure that confusion between panic(nil) and runtime.Goexit didn't
		// break concurrency limits.
		close(inflights)
		maxInFlight := maxOfChannel(inflights)
		assert.Equal(t, 1, maxInFlight)
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
	synctest.Test(t, func(t *testing.T) {
		const (
			workerCount   = 2
			detachedCount = 10
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
				if x%2 == 0 {
					runtime.Goexit()
				}
				return nil
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
	})
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

func TestMapDequeueAllBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			keyCount    = 10
			workerCount = 3
		)
		var (
			unblock = make(chan struct{})
			handled = make(chan int, keyCount)
		)
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			handled <- x
			<-unblock
			return nil
		})

		// Start up some blocked handlers.
		s.Limit(workerCount)
		keys := makeIntKeys(keyCount)
		errCh := make(chan error)
		go func() { errCh <- s.Collect(keys...) }()
		synctest.Wait()
		assert.Len(t, handled, workerCount)
		select {
		case <-errCh:
			assert.Fail(t, "Computation of keys was not blocked")
		default:
		}

		// Remove the unhandled keys from the queue.
		dequeued := s.DequeueAll()
		assert.Equal(t, keys[workerCount:], dequeued)

		// Unblock everything and make sure we only handled the first keys.
		close(unblock)
		assert.ErrorIs(t, <-errCh, parka.ErrTaskEjected)
		assert.Len(t, handled, workerCount)

		// Ensure we can resubmit the removed keys and have them handled.
		assert.NoError(t, s.Collect(dequeued...))
		assert.Len(t, handled, keyCount)
	})
}

func TestMapDequeueAllTorture(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			keyCount    = 100
			workerCount = 3
		)
		var (
			handled   atomic.Int32
			handleErr error
		)
		m := parka.NewMap(func(_ *parka.Handle, x int) (float64, error) {
			defer handled.Add(1)
			return math.Exp(float64(x)), handleErr
		})
		m.Limit(workerCount)

		// Dequeue and reinform in a tight loop. This isn't _guaranteed_ to overlap
		// with handler execution, but nearly always does (especially with -race).
		var cycled int
		done := make(chan struct{})
		go func() {
			defer close(done)
			for handled.Load() < keyCount {
				cycled++
				m.Inform(m.DequeueAll()...)
			}
		}()
		keys := makeIntKeys(keyCount)
		m.Inform(keys...)
		<-done

		handleErr = errors.New("handling should fail for unseen keys")
		got, err := m.Collect(keys...)
		assert.NoError(t, err)
		assert.Len(t, got, len(keys))

		t.Logf("Cycled queue %d time(s)", cycled)
	})
}

func TestMapWaitEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := parka.NewSet(func(_ *parka.Handle, _ any) error { return nil })
		s.Wait()
	})
}

func TestMapWaitNonempty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			<-unblock
			return nil
		})

		keys := makeIntKeys(10)
		s.Inform(keys...)
		synctest.Wait()

		var dones [2]<-chan struct{}
		for i := range dones {
			done := make(chan struct{})
			dones[i] = done
			go func() { s.Wait(); close(done) }()
		}
		synctest.Wait()

		for i, done := range dones {
			select {
			case <-done:
				assert.Fail(t, "Waiting for map was not blocked (i=%d)", i)
			default:
			}
		}

		close(unblock)
		for _, done := range dones {
			<-done
		}
	})
}

func TestMapCleanupWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		s := parka.NewSet(func(_ *parka.Handle, _ struct{}) error {
			<-unblock
			return nil
		})

		s.Inform(struct{}{})

		done := make(chan struct{})
		go func() { s.Cleanup(nil); close(done) }()
		synctest.Wait()
		select {
		case <-done:
			assert.Fail(t, "Cleanup was not blocked")
		default:
		}

		close(unblock)
	})
}

func TestMapCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			keyCount     = 5
			lastToHandle = 2
		)
		var (
			maxHandled  = -1
			ctx, cancel = context.WithCancel(context.Background())
		)
		s := parka.NewSet(func(_ *parka.Handle, x int) error {
			maxHandled = max(maxHandled, x)
			if x < lastToHandle {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		})
		s.Limit(1)

		keys := makeIntKeys(keyCount)
		errCh := make(chan error, 1)
		go func() { errCh <- s.Collect(keys...) }()
		synctest.Wait()

		s.Cleanup(cancel)
		assert.Equal(t, lastToHandle, maxHandled, "Started handlers that should have been dequeued")
		assert.ErrorIs(t, <-errCh, context.Canceled)
	})
}
