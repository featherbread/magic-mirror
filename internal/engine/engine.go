package engine

import "sync"

// NoValue is the standard value type for Engines whose tasks do not produce
// values.
type NoValue = struct{}

// Handler is a type for an Engine's handler function.
type Handler[K comparable, T any] func(K) (T, error)

// Engine is a parallel and deduplicating task runner.
//
// Every unique value provided to GetOrSubmit is mapped to a single Task, which
// will eventually produce a value or an error. The Engine limits the number of
// Tasks that may be in progress at any one time, and does not retry failed
// Tasks.
type Engine[K comparable, T any] struct {
	handle Handler[K, T]

	tasks   map[K]*Task[T]
	tasksMu sync.Mutex

	queue   []K
	queueMu sync.Mutex
	// For Engines with a worker count, queueReady is buffered to the number of
	// workers, and provides "readiness tokens" that can activate a worker and
	// allow it to pull from the queue. Every push to the queue should attempt to
	// send one token without blocking (since if the channel's buffer is full, we
	// know that's enough to eventually activate all workers).
	queueReady chan struct{}
}

// NewEngine creates an Engine that uses handle to fulfill submitted requests.
//
// If workers <= 0, every submitted request will immediately spawn a new
// goroutine to complete the requested task.
//
// If workers > 0, the concurrency of submitted requests will instead be limited
// to the provided value.
func NewEngine[K comparable, T any](workers int, handle Handler[K, T]) *Engine[K, T] {
	e := &Engine[K, T]{
		handle: handle,
		tasks:  make(map[K]*Task[T]),
	}
	if workers > 0 {
		e.queueReady = make(chan struct{}, workers)
		for i := 0; i < workers; i++ {
			go e.run()
		}
	}
	return e
}

// NoValueHandler wraps handlers for Engines that produce NoValue, so that the
// handler can be written without a return value type.
func NoValueHandler[K comparable](handle func(K) error) Handler[K, NoValue] {
	return func(key K) (_ NoValue, err error) {
		err = handle(key)
		return
	}
}

// GetOrSubmit returns the unique Task associated with the provided key, either
// by returning an existing Task or scheduling a new one.
func (e *Engine[K, T]) GetOrSubmit(key K) *Task[T] {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()

	if task, ok := e.tasks[key]; ok {
		return task
	}

	task := &Task[T]{done: make(chan struct{})}
	e.tasks[key] = task

	if e.queueReady == nil {
		go func() {
			task.value, task.err = e.handle(key)
			close(task.done)
		}()
		return task
	}

	e.push(key)
	return task
}

// Close indicates that no more requests will be submitted to the Engine,
// allowing it to eventually shut down.
//
// The behavior of all Engine methods (including Close) after a call to Close is
// undefined.
func (e *Engine[K, T]) Close() {
	if e.queueReady != nil {
		close(e.queueReady)
	}
}

func (e *Engine[K, V]) run() {
	if _, ok := <-e.queueReady; !ok {
		return
	}

	for {
		key, ok := e.tryPop()
		if !ok {
			if _, ready := <-e.queueReady; ready {
				continue
			} else {
				return
			}
		}

		e.tasksMu.Lock()
		task := e.tasks[key]
		e.tasksMu.Unlock()

		task.value, task.err = e.handle(key)
		close(task.done)

		select {
		case <-e.queueReady:
		default:
		}
	}
}

func (e *Engine[K, T]) push(key K) {
	e.queueMu.Lock()
	e.queue = append(e.queue, key)
	e.queueMu.Unlock()

	select {
	case e.queueReady <- struct{}{}:
	default:
	}
}

func (e *Engine[K, T]) tryPop() (key K, ok bool) {
	e.queueMu.Lock()
	defer e.queueMu.Unlock()

	if len(e.queue) > 0 {
		key = e.queue[0]
		ok = true
		e.queue = e.queue[1:]
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
