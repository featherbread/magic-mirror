package work

import (
	"context"
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
func (km *KeyMutex[K]) LockDetached(ctx context.Context, key K) (err error) {
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
		if err := Detach(ctx); err == nil {
			detached = true
		}
	}

	defer func() {
		if detached {
			// This won't lose any information. The only error we can return is
			// ctx.Err(), and since we know we successfully detached from a queue
			// that's also the only error Reattach can return.
			err = Reattach(ctx)
			if err != nil && locked {
				km.Unlock(key)
			}
		}
	}()

	for {
		km.chansMu.Lock()
		ch, ok := km.chans[key]
		if ok {
			km.chansMu.Unlock()
			tryDetach()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if km.chans == nil {
			km.chans = make(map[K]chan struct{})
		}
		km.chans[key] = make(chan struct{})
		km.chansMu.Unlock()
		locked = true
		return nil
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
	close(ch)
	delete(km.chans, key)
}
