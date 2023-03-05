package engine

import "sync"

// NoValue is the standard value type for queues whose tasks do not produce
// values.
type NoValue = struct{}

// Handler is a type for an queue's handler function.
type Handler[K comparable, T any] func(K) (T, error)

// Queue is a parallel and deduplicating task runner.
//
// Every unique value provided to GetOrSubmit is mapped to a single task, which
// will eventually produce a value or an error. The Queue limits the number of
// tasks that may be in progress at any one time, and does not retry failed
// tasks.
type Queue[K comparable, T any] struct {
	handle Handler[K, T]

	tasks   map[K]*Task[T]
	tasksMu sync.Mutex

	queue   []K
	queueMu sync.Mutex
	// For queues with a worker count, queueReady is buffered to the number of
	// workers, and provides "readiness tokens" that can activate a worker and
	// allow it to pull from the queue. Every push to the queue should attempt to
	// send one token without blocking (since if the channel's buffer is full, we
	// know that's enough to eventually activate all workers).
	queueReady chan struct{}
}

// NewQueue creates a queue that uses handle to fulfill submitted requests.
//
// If workers <= 0, every submitted request will immediately spawn a new
// goroutine to complete the requested task.
//
// If workers > 0, the concurrency of submitted requests will instead be limited
// to the provided value.
func NewQueue[K comparable, T any](workers int, handle Handler[K, T]) *Queue[K, T] {
	q := &Queue[K, T]{
		handle: handle,
		tasks:  make(map[K]*Task[T]),
	}
	if workers > 0 {
		q.queueReady = make(chan struct{}, workers)
		for i := 0; i < workers; i++ {
			go q.worker()
		}
	}
	return q
}

// NoValueHandler wraps handlers for queues that produce NoValue, so that the
// handler can be written without a return value type.
func NoValueHandler[K comparable](handle func(K) error) Handler[K, NoValue] {
	return func(key K) (_ NoValue, err error) {
		err = handle(key)
		return
	}
}

// GetOrSubmit returns the unique task associated with the provided key, either
// by returning an existing task or scheduling a new one.
func (q *Queue[K, T]) GetOrSubmit(key K) *Task[T] {
	return q.GetOrSubmitAll(key)[0]
}

func (q *Queue[K, T]) GetOrSubmitAll(keys ...K) []*Task[T] {
	tasks, newKeys := q.getOrCreateTasks(keys...)
	if len(newKeys) > 0 {
		q.scheduleNewKeys(newKeys)
	}
	return tasks
}

// Close indicates that no more requests will be submitted to the Engine,
// allowing it to eventually shut down.
//
// The behavior of all Engine methods (including Close) after a call to Close is
// undefined.
func (q *Queue[K, T]) Close() {
	if q.queueReady != nil {
		close(q.queueReady)
	}
}

func (q *Queue[K, T]) getOrCreateTasks(keys ...K) (tasks []*Task[T], newKeys []K) {
	tasks = make([]*Task[T], len(keys))
	newKeys = make([]K, 0, len(keys))

	q.tasksMu.Lock()
	defer q.tasksMu.Unlock()

	for i, key := range keys {
		key := key

		if task, ok := q.tasks[key]; ok {
			tasks[i] = task
			continue
		}

		task := &Task[T]{done: make(chan struct{})}
		q.tasks[key] = task
		tasks[i] = task
		newKeys = append(newKeys, key)
	}
	return
}

func (q *Queue[K, T]) scheduleNewKeys(keys []K) {
	if q.queueReady != nil {
		q.scheduleQueued(keys)
	} else {
		q.scheduleUnqueued(keys)
	}
}

func (q *Queue[K, T]) scheduleQueued(keys []K) {
	q.queueMu.Lock()
	q.queue = append(q.queue, keys...)
	q.queueMu.Unlock()

	for range keys {
		select {
		case q.queueReady <- struct{}{}:
		default:
			return
		}
	}
}

func (q *Queue[K, T]) scheduleUnqueued(keys []K) {
	for _, key := range keys {
		key := key
		go q.completeTask(key)
	}
}

func (q *Queue[K, T]) completeTask(key K) {
	q.tasksMu.Lock()
	task := q.tasks[key]
	q.tasksMu.Unlock()

	task.value, task.err = q.handle(key)
	close(task.done)
}

func (q *Queue[K, V]) worker() {
	if _, ok := <-q.queueReady; !ok {
		return
	}

	for {
		key, ok := q.tryPop()
		if !ok {
			if _, ready := <-q.queueReady; ready {
				continue
			} else {
				return
			}
		}

		q.completeTask(key)

		select {
		case <-q.queueReady:
		default:
		}
	}
}

func (q *Queue[K, T]) tryPop() (key K, ok bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if len(q.queue) > 0 {
		key = q.queue[0]
		ok = true
		q.queue = q.queue[1:]
	}
	return
}

type Task[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func (t *Task[T]) Wait() (T, error) {
	<-t.done
	return t.value, t.err
}
