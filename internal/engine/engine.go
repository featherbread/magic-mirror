package engine

import "sync"

type NoValue = struct{}

type Handler[K comparable, T any] func(K) (T, error)

type Engine[K comparable, T any] struct {
	handle Handler[K, T]

	tasks   map[K]*Task[T]
	tasksMu sync.Mutex

	pending chan K
}

func NewEngine[K comparable, T any](workers int, handle Handler[K, T]) *Engine[K, T] {
	e := &Engine[K, T]{
		handle:  handle,
		tasks:   make(map[K]*Task[T]),
		pending: make(chan K),
	}
	for i := 0; i < workers; i++ {
		go e.run()
	}
	return e
}

func NoValueHandler[K comparable](handle func(K) error) Handler[K, NoValue] {
	return func(key K) (_ NoValue, err error) {
		err = handle(key)
		return
	}
}

func (e *Engine[K, T]) GetOrSubmit(key K) *Task[T] {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()

	if task, ok := e.tasks[key]; ok {
		return task
	}

	task := &Task[T]{done: make(chan struct{})}
	e.tasks[key] = task
	go func() { e.pending <- key }()
	return task
}

func (e *Engine[K, T]) Close() {
	close(e.pending)
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
