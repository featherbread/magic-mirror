package work

// SetHandler is a type for a [SetQueue]'s handler function.
type SetHandler[K comparable] = func(*QueueHandle, K) error

// SetQueue wraps a [Queue] whose handlers return no meaningful value with a
// simplified error-only API.
type SetQueue[K comparable] struct {
	*Queue[K, struct{}]
}

// NewSetQueue is analogous to [NewQueue].
func NewSetQueue[K comparable](concurrency int, handle SetHandler[K]) SetQueue[K] {
	return SetQueue[K]{
		Queue: NewQueue(concurrency, func(qh *QueueHandle, key K) (_ struct{}, err error) {
			err = handle(qh, key)
			return
		}),
	}
}

// Get is analogous to [Queue.Get].
func (q SetQueue[K]) Get(key K) error {
	_, err := q.Queue.Get(key)
	return err
}

// Collect is analogous to [Queue.Collect].
func (q SetQueue[K]) Collect(keys ...K) error {
	_, err := q.Queue.Collect(keys...)
	return err
}
