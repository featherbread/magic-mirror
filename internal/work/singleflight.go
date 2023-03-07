package work

import "sync"

type SingleFlight[K comparable] struct {
	inflight map[K]chan struct{}
	mu       sync.Mutex
}

func NewSingleFlight[K comparable]() SingleFlight[K] {
	return SingleFlight[K]{inflight: make(map[K]chan struct{})}
}

func (sf *SingleFlight[K]) Running(key K) (ok bool) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	_, ok = sf.inflight[key]
	return
}

func (sf *SingleFlight[K]) Start(key K) {
	for {
		sf.mu.Lock()
		ch, ok := sf.inflight[key]
		if ok {
			sf.mu.Unlock()
			<-ch
			continue
		}

		ch = make(chan struct{})
		sf.inflight[key] = ch
		sf.mu.Unlock()
		return
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
