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
// provided key has been released, or until ctx is canceled.
//
// If ctx is associated with a [Queue], LockDetached will [Detach] the caller
// from the queue until it is able to lock the key, and will attempt to
// [Reattach] before returning.
//
// LockDetached returns ctx.Err() if ctx is canceled before the key is locked.
func (km *KeyMutex[K]) LockDetached(ctx context.Context, key K) (err error) {
	var detached, triedDetach bool
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
			// ctx.Err(), and since we know we detached from a queue that's also the
			// only error Reattach can return. (TODO: Only true if multiple Detach
			// calls result in an error, which is not yet the case.)
			err = Reattach(ctx)
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
		ch = make(chan struct{})
		km.chans[key] = ch
		km.chansMu.Unlock()
		return nil
	}
}

func (km *KeyMutex[K]) Unlock(key K) {
	km.chansMu.Lock()
	defer km.chansMu.Unlock()

	ch, ok := km.chans[key]
	if !ok {
		panic("nothing in flight for key")
	}
	close(ch)
	delete(km.chans, key)
}
