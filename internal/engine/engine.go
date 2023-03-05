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

	pending chan K
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
		e.pending = make(chan K, workers)
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
// by returning an existing Task or scheduling a new one. It never blocks,
// regardless of the Engine's worker count.
func (e *Engine[K, T]) GetOrSubmit(key K) *Task[T] {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()

	if task, ok := e.tasks[key]; ok {
		return task
	}

	task := &Task[T]{done: make(chan struct{})}
	e.tasks[key] = task

	if e.pending == nil {
		go func() {
			task.value, task.err = e.handle(key)
			close(task.done)
		}()
		return task
	}

	select {
	case e.pending <- key:
		return task
	default:
		go func() { e.pending <- key }()
		return task
	}
}

// Close indicates that no more requests will be submitted to the Engine,
// allowing it to eventually shut down.
//
// The behavior of all Engine methods (including Close) after a call to Close is
// undefined.
func (e *Engine[K, T]) Close() {
	if e.pending != nil {
		close(e.pending)
	}
}

func (e *Engine[K, V]) run() {
	for key := range e.pending {
		e.tasksMu.Lock()
		task := e.tasks[key]
		e.tasksMu.Unlock()

		task.value, task.err = e.handle(key)
		close(task.done)
	}
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
