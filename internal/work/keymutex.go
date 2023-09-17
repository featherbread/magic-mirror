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
// provided key is released, or until ctx is canceled. It returns nil if the
// lock was successfully acquired, or ctx.Err() if ctx was canceled before
// locking could finish. A non-nil error indicates that the lock is not held,
// and that the caller must not unlock the key or violate any invariant that the
// lock protects.
//
// If ctx is associated with a [Queue], LockDetached will [Detach] the caller
// from the queue until it acquires the lock, and will attempt to [Reattach]
// before returning. It will hold the lock while waiting to reattach, and will
// release the lock if ctx is canceled before Reattach completes.
func (km *KeyMutex[K]) LockDetached(wctx Context, key K) (err error) {
	var (
		locked      bool
		detached    bool
		triedDetach bool
	)

	tryDetach := func() {
		if triedDetach {
			return
		}
		triedDetach = true
		if err := wctx.Detach(); err == nil {
			detached = true
		}
	}

	defer func() {
		if detached {
			// This won't lose any information. The only error we can return is
			// ctx.Err(), and since we know we successfully detached from a queue
			// that's also the only error Reattach can return.
			err = wctx.Reattach(wctx)
			if err != nil && locked {
				km.Unlock(key)
			}
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
			locked = true
			return nil
		}

		// Someone else currently holds the lock on this key. We'll detach from the
		// parent queue and start waiting on their channel. At this point, one of
		// three things can happen:
		//
		// 1. Our context expires before the key is unlocked. We break out without
		//    ever obtaining the lock.
		//
		// 2. We enter the `select` before the key is unlocked. The unlocker will
		//    see that we're waiting and directly pass a single token that grants
		//    the lock. This limits new channel allocations, and takes advantage of
		//    whatever mechanisms the Go runtime provides to ensure fairness and
		//    limit starvation among waiters.
		//
		// 3. The key is unlocked before we can enter the `select`. If no other
		//    waiters are ready to receive a token, the unlocker will close the
		//    channel and remove it from the map. When this happens, we need to loop
		//    back and try to create or obtain a fresh channel.
		km.chansMu.Unlock()
		tryDetach()
		select {
		case <-wctx.Done():
			return wctx.Err()
		case _, passed := <-ch:
			if passed {
				locked = true
				return nil
			}
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
		// This is case 2 of 3 in LockDetached: we successfully passed the lock to
		// another waiter.
	default:
		// This is case 3 of 3 in LockDetached: there are no other waiters at this
		// time. If a waiter got this channel and hasn't yet blocked on it, they'll
		// need to detect that it's closed and get a new one.
		close(ch)
		delete(km.chans, key)
	}
}
