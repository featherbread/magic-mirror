package work

import (
	"sync/atomic"
	"testing"
)

func TestKeyMutexBasic(t *testing.T) {
	const keyCount = 3
	const workerCount = 2 * keyCount

	var (
		km      KeyMutex[int]
		locked  [keyCount]atomic.Int32
		unblock = make(chan struct{})
	)
	for i := range workerCount {
		key := i / 2
		async(t, func() {
			km.Lock(key)
			defer km.Unlock(key)

			locked[key].Add(1)
			defer locked[key].Add(-1)

			<-unblock
		})
	}

	// Try to wait for every goroutine to be blocked, then check for limit
	// breaches.
	forceRuntimeProgress()
	for i := range locked {
		if count := locked[i].Load(); count > 1 {
			t.Errorf("mutex for %d held %d times", i, count)
		}
	}

	// Let all of the workers finish.
	close(unblock)
}

func TestKeyMutexDetachReattach(t *testing.T) {
	var (
		km       KeyMutex[Empty]
		started0 = make(chan struct{})
		locked0  = make(chan struct{})
		unblock0 = make(chan struct{})
	)
	q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
		if x == 0 {
			close(started0)
			km.LockDetached(qh, Empty{})
			close(locked0)
			<-unblock0
			km.Unlock(Empty{})
		}
		return x, nil
	})

	// Take the lock.
	km.Lock(Empty{})

	// Start the handler for 0, which will have to detach since we're holding
	// the lock.
	async(t, func() { q.Get(0) })
	assertReceiveCount(t, 1, started0)

	// Ensure that unrelated handlers can, in fact, proceed.
	assertIdentityResults(t, q, 1)

	// Release the lock so handler 0 can obtain it.
	km.Unlock(Empty{})
	assertReceiveCount(t, 1, locked0)

	// Start another handler, and ensure it really is blocked behind handler 0.
	assertKeyBlocked(t, q, 2)

	// Allow all of the handlers to finish.
	close(unblock0)
	keys := []int{0, 1, 2}
	assertIdentityResults(t, q, keys...)
}

func TestKeyMutexDoubleUnlock(t *testing.T) {
	defer func() {
		const want = "key is already unlocked"
		if r := recover(); r != want {
			t.Errorf("unexpected panic: got %v, want %v", r, want)
		}
	}()
	var km KeyMutex[Empty]
	km.Unlock(Empty{})
}
