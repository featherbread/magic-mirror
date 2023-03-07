package work

import (
	"context"
	"sync"
)

type SingleFlight[K comparable] struct {
	inflight map[K]chan struct{}
	mu       sync.Mutex
}

func NewSingleFlight[K comparable]() SingleFlight[K] {
	return SingleFlight[K]{inflight: make(map[K]chan struct{})}
}

func (sf *SingleFlight[K]) Start(ctx context.Context, key K) error {
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

	for {
		sf.mu.Lock()
		ch, ok := sf.inflight[key]
		if ok {
			sf.mu.Unlock()
			tryDetach()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
				continue
			}
		}

		ch = make(chan struct{})
		sf.inflight[key] = ch
		sf.mu.Unlock()

		if detached {
			return Reattach(ctx)
		}
		return nil
	}
}

func (sf *SingleFlight[K]) Finish(key K) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	ch, ok := sf.inflight[key]
	if !ok {
		panic("nothing in flight for key")
	}
	close(ch)
	delete(sf.inflight, key)
}
