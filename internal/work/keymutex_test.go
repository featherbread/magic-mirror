package work

import (
	"sync/atomic"
	"testing"
)

func TestKeyMutexBasic(t *testing.T) {
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
	forceRuntimeProgress()
	for i := range locked {
		if count := locked[i].Load(); count > 1 {
			t.Errorf("mutex for %d held %d times", i, count)
		}
	}

	// Wait for the workers to finish.
	close(canReturn)
	assertReceiveCount(t, nWorkers, hasFinished)
}

func TestKeyMutexDetachReattach(t *testing.T) {
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
	async(t, func() { q.Get(0) })
	<-w0HasStarted

	// Ensure that unrelated handlers can proceed while handler 0 awaits the lock.
	assertIdentityResults(t, q, 1)

	// Allow handler 0 to obtain the lock.
	km.Unlock(NoValue{})
	<-w0HasLocked

	// Ensure that unrelated handlers are blocked.
	assertKeyBlocked(t, q, 2)

	// Allow both handlers to finish.
	close(w0CanUnlock)
	assertIdentityResults(t, q, 0, 2)
}

func TestKeyMutexDoubleUnlock(t *testing.T) {
	defer func() {
		const want = "key is already unlocked"
		if r := recover(); r != want {
			t.Errorf("unexpected panic: got %v, want %v", r, want)
		}
	}()
	var km KeyMutex[NoValue]
	km.Unlock(NoValue{})
}
