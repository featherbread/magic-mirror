package parka

// Set wraps a [Queue] whose handlers return no meaningful value with simplified
// error-only result APIs.
type Set[K comparable] struct {
	*Queue[K, struct{}]
}

// NewSet is analogous to [NewQueue], but accepts a simplified handler returning
// only an error.
func NewSet[K comparable](handle func(*Handle, K) error) Set[K] {
	return Set[K]{
		Queue: NewQueue(func(qh *Handle, key K) (_ struct{}, err error) {
			err = handle(qh, key)
			return
		}),
	}
}

// Get is analogous to [Queue.Get].
func (s Set[K]) Get(key K) error {
	_, err := s.Queue.Get(key)
	return err
}

// Collect is analogous to [Queue.Collect].
func (s Set[K]) Collect(keys ...K) error {
	_, err := s.Queue.Collect(keys...)
	return err
}
