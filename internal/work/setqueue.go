package work

// Empty is the canonical empty value type for a [Queue].
type Empty = struct{}

// SetHandler is a type for a [SetQueue]'s handler function.
type SetHandler[K comparable] = func(*QueueHandle, K) error

// SetQueue represents a [Queue] whose handlers return no meaningful value.
type SetQueue[K comparable] struct {
	*Queue[K, Empty]
}

// NewSetQueue creates a [SetQueue] in a manner equivalent to how [NewQueue]
// creates a [Queue].
func NewSetQueue[K comparable](concurrency int, handle SetHandler[K]) SetQueue[K] {
	return SetQueue[K]{
		Queue: NewQueue(concurrency, func(qh *QueueHandle, key K) (_ Empty, err error) {
			err = handle(qh, key)
			return
		}),
	}
}

// Get is analogous to [Queue.Get], but does not return a meaningful value.
func (q SetQueue[K]) Get(key K) error {
	_, err := q.Queue.Get(key)
	return err
}

// Collect is analogous to [Queue.Collect], but does not return meaningful
// values.
func (q SetQueue[K]) Collect(keys ...K) error {
	_, err := q.Queue.Collect(keys...)
	return err
}
