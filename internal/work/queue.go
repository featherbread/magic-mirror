package work

import (
	"context"
	"errors"
	"sync"
)

// NoValue is the canonical empty value type for a queue.
type NoValue = struct{}

// Handler is the type for a queue's handler function.
type Handler[K comparable, T any] func(K) (T, error)

// Queue is a deduplicating work queue. It maps each unique key provided to
// GetOrSubmit[All] to a single [Task], which acts as a promise for the result
// of running a handler function with that key. Tasks may return a value and an
// error; when a task produces an error, the queue does not retry it. After all
// keys have been submitted to a queue, CloseSubmit should be called to permit
// the release of resources associated with it.
type Queue[K comparable, T any] struct {
	handle Handler[K, T]

	tasks   map[K]*Task[T]
	tasksMu sync.Mutex

	queue   []K
	queueMu sync.Mutex
	// For queues with a worker count, queueReady is buffered to the number of
	// workers, and provides "readiness tokens" that activate workers who are
	// waiting on an empty queue to fill up. Every push to the queue should
	// attempt to send one token without blocking; if the channel's buffer is
	// full, that's enough to eventually activate all workers. Correspondingly,
	// workers ready to pop from the queue should try to steal an available token
	// without blocking, and maybe keep an idle worker from having to awaken.
	queueReady chan struct{}
}

// NewQueue creates a queue that uses the provided handler function to complete
// tasks.
//
// If workers > 0, the queue will run up to that number of tasks concurrently.
// If workers <= 0, the queue's concurrency is unbounded.
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

// NoValueHandler wraps handlers for queues that produce [NoValue], so the
// handler function can be written to only return an error.
func NoValueHandler[K comparable](handle func(K) error) Handler[K, NoValue] {
	return func(key K) (_ NoValue, err error) {
		err = handle(key)
		return
	}
}

// GetOrSubmit returns the unique task for the provided key. If the key has not
// previously been submitted, the new task will be scheduled for execution after
// all existing tasks in the queue.
func (q *Queue[K, T]) GetOrSubmit(key K) *Task[T] {
	return q.GetOrSubmitAll(key)[0]
}

// GetOrSubmitAll returns the set of unique tasks for the provided keys. For all
// keys not previously submitted, the new tasks will be scheduled for execution
// after all existing tasks in the queue, in the order of their corresponding
// keys, without interleaving tasks from any other call to GetOrSubmit[All].
func (q *Queue[K, T]) GetOrSubmitAll(keys ...K) TaskList[T] {
	tasks, newKeys := q.getOrCreateTasks(keys...)
	if len(newKeys) > 0 {
		q.scheduleNewKeys(newKeys)
	}
	return tasks
}

// CloseSubmit indicates that no more requests will be submitted to the queue,
// permitting the eventual cleanup of the queue's resources after all
// outstanding tasks have completed.
//
// The behavior of any method of the queue (including CloseSubmit) after a call
// to CloseSubmit is undefined.
func (q *Queue[K, T]) CloseSubmit() {
	if q.queueReady != nil {
		close(q.queueReady)
	}
}

func (q *Queue[K, T]) getOrCreateTasks(keys ...K) (tasks TaskList[T], newKeys []K) {
	tasks = make(TaskList[T], len(keys))

	q.tasksMu.Lock()
	defer q.tasksMu.Unlock()

	for i, key := range keys {
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
	for {
		if key, ok := q.tryPop(); ok {
			q.completeTask(key)
			select {
			case <-q.queueReady:
			default:
			}
			continue
		}
		if _, ready := <-q.queueReady; !ready {
			return
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

// Task represents the eventual result of a queued work item, which may produce
// both a value and an error.
type Task[T any] struct {
	done  chan struct{}
	value T
	err   error
}

// Wait blocks until the task has completed, then returns its value and error.
func (t *Task[T]) Wait() (T, error) {
	<-t.done
	return t.value, t.err
}

// TaskList represents a list of tasks.
type TaskList[T any] []*Task[T]

// Wait blocks until all of the tasks in the list have completed, then returns
// their associated values. The returned error is the concatenation of the
// errors from all tasks, following the semantics of [errors.Join]. To associate
// errors with specific tasks, call Wait directly on each task in the list.
func (ts TaskList[T]) Wait() ([]T, error) {
	values := make([]T, len(ts))
	errs := make([]error, len(ts))
	for i, task := range ts {
		values[i], errs[i] = task.Wait()
	}
	return values, errors.Join(errs...)
}

// Detach unbounds the calling [Handler] from the concurrency limit of the
// [Queue] that invoked it, allowing the queue to immediately start work on
// another task. It returns an error if ctx is not associated with a [Queue].
//
// The corresponding [Reattach] function permits a detached handler to
// reestablish itself within the queue's concurrency limit ahead of any pending
// submissions. A typical use for detaching is to temporarily block on the
// completion of another task in the same queue, so that the current task may
// take advantage of caching or other side effects associated with that task to
// improve its performance.
func Detach(ctx context.Context) error {
	return errors.New("not implemented")
}

// Reattach blocks the calling [Handler] until it can continue executing within
// the concurrency limit of the [Queue] that invoked it, or until ctx is
// canceled. It returns an error if ctx is not associated with a [Queue], or if
// the handler did not previously [Detach] from the queue.
//
// See [Detach] for additional information.
func Reattach(ctx context.Context) error {
	return errors.New("not implemented")
}
