//go:build goexperiment.synctest

package work

import (
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
)

func TestQueueGoexitHandlingSynctest(t *testing.T) {
	synctest.Run(func() {
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
		go func() { q.Get(0) }()
		stepGoexit <- struct{}{}

		// Force some more handlers to queue up...
		go func() { q.GetAll(1, 2) }()
		synctest.Wait()

		// ...then let them through.
		close(stepGoexit)

		// Ensure that the initial Goexit didn't break the processing of other keys.
		want := []int{1, 2}
		got, err := q.GetAll(want...)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("unexpected handler results (-want +got): %s", diff)
		}
	})
}

func TestQueueDeduplicationSynctest(t *testing.T) {
	synctest.Run(func() {
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
		assertSubmittedCount(t, q, half)
		assertDoneCount(t, q, half)

		// Re-block the handler and start handling another key.
		canReturn = make(chan struct{})
		assertBlockedAfter(synctest.Wait, t, q, keys[half])
		assertSubmittedCount(t, q, half+1)
		assertDoneCount(t, q, half)

		// Ensure that the previous results are cached.
		assertIdentityResults(t, q, keys[:half]...)

		// Finish handling the rest of the keys.
		close(canReturn)
		assertIdentityResults(t, q, keys...)
		assertSubmittedCount(t, q, count)
		assertDoneCount(t, q, count)
	})
}

func TestQueueConcurrencyLimitSynctest(t *testing.T) {
	synctest.Run(func() {
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
		go func() { q.GetAll(keys...) }()
		synctest.Wait()

		// Let them all finish, and make sure they all saw the limit respected.
		close(canReturn)
		assertIdentityResults(t, q, keys...)
		assertSubmittedCount(t, q, submitCount)
		assertDoneCount(t, q, submitCount)
		if breached.Load() {
			t.Errorf("queue breached limit of %d workers in flight", workerCount)
		}
	})
}

func TestQueueOrderingSynctest(t *testing.T) {
	synctest.Run(func() {
		var handledOrder []int
		unblock := make(chan struct{})
		q := NewQueue(1, func(_ *QueueHandle, x int) (int, error) {
			<-unblock
			handledOrder = append(handledOrder, x)
			return x, nil
		})

		// Start a new blocked handler to force the queueing of subsequent keys.
		go func() { q.Get(0) }()
		synctest.Wait()

		// Queue up some keys with various priorities.
		go func() { q.GetAll(1, 2) }()
		synctest.Wait()
		go func() { q.GetAllUrgent(-1, -2) }()
		synctest.Wait()
		go func() { q.Get(3) }()
		synctest.Wait()
		go func() { q.GetUrgent(-3) }()
		synctest.Wait()

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
		if diff := cmp.Diff(wantOrder, handledOrder); diff != "" {
			t.Errorf("incorrect handling order (-want +got):\n%s", diff)
		}
	})
}

func TestQueueReattachPrioritySynctest(t *testing.T) {
	synctest.Run(func() {
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
		go func() { q.Get(0) }()
		assertReceiveCount(t, 1, w0HasDetached)

		// Ensure that unrelated handlers are unblocked.
		assertIdentityResults(t, q, -1)

		// Start a non-detached handler for 1, and ensure that 2 and 3 are queued.
		go func() { q.GetAll(1, 2, 3) }()
		assertReceiveCount(t, 1, w1HasStarted)

		// Allow the detached handler for 0 to reattach, and try to force it to run
		// until it actually queues itself up for reattachment.
		close(w0CanReattach)
		synctest.Wait()

		// Allow the handler for 1 to finish, unblocking all the rest as well.
		close(w1CanReturn)
		assertIdentityResults(t, q, 0, 1, 2, 3)

		lastHandled := handleOrder[len(handleOrder)-1]
		if lastHandled == 0 {
			t.Error("reattaching handler did not receive priority over new keys")
		}
	})
}

func TestQueueReattachConcurrencySynctest(t *testing.T) {
	synctest.Run(func() {
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
		go func() { q.GetAll(keys...) }()
		assertReceiveCount(t, submitCount, hasDetached)

		// Allow them to start reattaching, and force as many as possible to finish
		// reattaching and checking the reattach count.
		close(canReattach)
		synctest.Wait()

		// Let them all finish and return, and make sure none saw too many handlers in
		// flight.
		close(canReturn)
		assertIdentityResults(t, q, keys...)
		assertSubmittedCount(t, q, submitCount)
		assertDoneCount(t, q, submitCount)
		if breached.Load() {
			t.Errorf("queue breached limit of %d workers in flight during reattach", workerCount)
		}
	})
}

func TestQueueDetachReturnSynctest(t *testing.T) {
	synctest.Run(func() {
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
		go func() { q.GetAll(detachedKeys...) }()
		assertReceiveCount(t, len(detachedKeys), hasDetached)

		// Start up some normal handlers, and make sure they block.
		attachedKeys := makeIntKeys(3 * len(detachedKeys))
		go func() { q.GetAll(attachedKeys...) }()
		assertBlockedAfter(synctest.Wait, t, q, attachedKeys[0])

		// Let the detached handlers finish, and push them forward if they're going to
		// incorrectly pick up keys rather than exit.
		close(detachedCanReturn)
		assertIdentityResults(t, q, detachedKeys...)
		synctest.Wait()

		// Unblock all handlers and make sure the limit wasn't breached.
		close(attachedCanReturn)
		assertIdentityResults(t, q, attachedKeys...)
		if breached.Load() {
			t.Error("queue breached limit of 1 worker in flight")
		}
	})
}

func TestKeyMutexBasicSynctest(t *testing.T) {
	synctest.Run(func() {
		const (
			nKeys    = 3
			nWorkers = nKeys * 2
		)
		var (
			km          KeyMutex[int]
			locked      [nKeys]atomic.Int32
			hasStarted  = make(chan struct{})
			canReturn   = make(chan struct{})
			hasFinished = make(chan struct{}, nWorkers)
		)
		for i := 0; i < nWorkers; i++ {
			key := i / 2
			go func() {
				defer func() { hasFinished <- struct{}{} }()
				hasStarted <- struct{}{}

				km.Lock(key)
				defer km.Unlock(key)

				locked[key].Add(1)
				defer locked[key].Add(-1)
				<-canReturn
			}()
		}

		// Wait for every goroutine to be running, then force them all forward and
		// check for limit breaches.
		assertReceiveCount(t, nWorkers, hasStarted)
		synctest.Wait()
		for i := range locked {
			if count := locked[i].Load(); count > 1 {
				t.Errorf("mutex for %d held %d times", i, count)
			}
		}

		// Wait for the workers to finish.
		close(canReturn)
		assertReceiveCount(t, nWorkers, hasFinished)
	})
}

func TestKeyMutexDetachReattachSynctest(t *testing.T) {
	synctest.Run(func() {
		var (
			km      KeyMutex[NoValue]
			workers [1]func(*QueueHandle)
		)

		var (
			w0HasStarted = make(chan struct{})
			w0HasLocked  = make(chan struct{})
			w0CanUnlock  = make(chan struct{})
		)
		workers[0] = func(qh *QueueHandle) {
			close(w0HasStarted)
			km.LockDetached(qh, NoValue{})
			close(w0HasLocked)
			<-w0CanUnlock
			km.Unlock(NoValue{})
		}

		q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
			if x >= 0 && x < len(workers) {
				workers[x](qh)
			}
			return x, nil
		})

		// Start the handler for 0, but force it to detach by holding the lock first.
		km.Lock(NoValue{})
		go func() { q.Get(0) }()
		<-w0HasStarted

		// Ensure that unrelated handlers can proceed while handler 0 awaits the lock.
		assertIdentityResults(t, q, 1)

		// Allow handler 0 to obtain the lock.
		km.Unlock(NoValue{})
		<-w0HasLocked

		// Ensure that unrelated handlers are blocked.
		assertBlockedAfter(synctest.Wait, t, q, 2)

		// Allow both handlers to finish.
		close(w0CanUnlock)
		assertIdentityResults(t, q, 0, 2)
	})
}
