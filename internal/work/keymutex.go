package work

import (
	"sync"
)

// KeyMutex provides mutual exclusion among operations targeting a given
// comparable key. The zero value is a valid KeyMutex with no locked keys.
//
// A KeyMutex must not be copied after first use.
type KeyMutex[K comparable] struct {
	chans   map[K]chan struct{}
	chansMu sync.Mutex
}

// LockDetached blocks the calling goroutine until any other lock on the
// provided key is released. If the lock is not immediately available,
// LockDetached will detach the caller from the [Queue] associated with q while
// it waits for the lock, and will reattach before returning.
func (km *KeyMutex[K]) LockDetached(q *QueueHandle, key K) {
	var (
		detached    bool
		triedDetach bool
	)

	tryDetach := func() {
		if triedDetach {
			return
		}
		triedDetach = true
		if err := q.Detach(); err == nil {
			detached = true
		}
	}

	defer func() {
		if detached {
			// This won't return an error since we know we detached from the queue.
			q.Reattach()
		}
	}()

	for {
		km.chansMu.Lock()
		ch, ok := km.chans[key]
		if !ok {
			// We are the only ones currently trying to lock this key, and will
			// initialize a channel for future waiters to block on.
			if km.chans == nil {
				km.chans = make(map[K]chan struct{})
			}
			km.chans[key] = make(chan struct{})
			km.chansMu.Unlock()
			return
		}

		// Someone else currently holds the lock on this key. We'll detach from the
		// parent queue and start waiting on their channel. If possible, we'll try
		// to receive a single token from another unlocker, to limit channel
		// allocations and take advantage of whatever fairness mechanisms the Go
		// runtime provides. Otherwise, we'll loop and try to get or create a fresh
		// channel again.
		km.chansMu.Unlock()
		tryDetach()
		_, passed := <-ch
		if passed {
			return
		}
	}
}

// Unlock releases the lock on the provided key. It panics if the key is not
// currently locked.
func (km *KeyMutex[K]) Unlock(key K) {
	km.chansMu.Lock()
	defer km.chansMu.Unlock()

	ch, ok := km.chans[key]
	if !ok {
		panic("key is already unlocked")
	}
	select {
	case ch <- struct{}{}:
		// We successfully passed the lock to another waiter.
	default:
		// There are no other waiters at this time. If a waiter got this channel and
		// hasn't yet blocked on it, they'll need to detect that it's closed and get
		// a new one.
		close(ch)
		delete(km.chans, key)
	}
}
