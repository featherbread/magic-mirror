package parka

// SetQueue wraps a [Queue] whose handlers return no meaningful value with
// simplified error-only result APIs.
type SetQueue[K comparable] struct {
	*Queue[K, struct{}]
}

// NewSetQueue is analogous to [NewQueue], but accepts a simplified handler
// returning only an error.
func NewSetQueue[K comparable](handle func(*QueueHandle, K) error) SetQueue[K] {
	return SetQueue[K]{
		Queue: NewQueue(func(qh *QueueHandle, key K) (_ struct{}, err error) {
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
