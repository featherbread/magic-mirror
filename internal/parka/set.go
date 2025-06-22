package parka

// Set wraps a [Map] whose handlers return no meaningful value with simplified
// error-only result APIs.
type Set[K comparable] struct {
	*Map[K, struct{}]
}

// NewSet is analogous to [NewMap], but accepts a simplified handler returning
// only an error.
func NewSet[K comparable](handle func(*Handle, K) error) Set[K] {
	return Set[K]{
		Map: NewMap(func(qh *Handle, key K) (_ struct{}, err error) {
			err = handle(qh, key)
			return
		}),
	}
}

// Get is analogous to [Map.Get].
func (s Set[K]) Get(key K) error {
	_, err := s.Map.Get(key)
	return err
}

// Collect is analogous to [Map.Collect].
func (s Set[K]) Collect(keys ...K) error {
	_, err := s.Map.Collect(keys...)
	return err
}
