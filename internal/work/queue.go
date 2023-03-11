package work

import (
	"context"
	"errors"
	"sync"
)

// NoValue is the canonical empty value type for a queue.
type NoValue = struct{}

// Handler is the type for a queue's handler function.
type Handler[K comparable, T any] func(context.Context, K) (T, error)

// Queue is a deduplicating work queue. It maps each unique key provided to
// GetOrSubmit[All] to a single [Task], which acts as a promise for the result
// of running a handler function with that key. Tasks may return a value and an
// error; when a task produces an error, the queue does not retry it. After all
// keys have been submitted to a queue, CloseSubmit should be called to permit
// the release of resources associated with it.
type Queue[K comparable, T any] struct {
	handle Handler[K, T]

	ctx    context.Context
	cancel context.CancelFunc

	tasks   map[K]*Task[T]
	tasksMu sync.Mutex

	queue   []K
	queueMu sync.Mutex

	// For queues with a worker count, ready is buffered to the number of workers,
	// and provides "readiness tokens" that activate workers who are waiting on an
	// empty queue to fill up. Every push to the queue should attempt to send one
	// token without blocking; if the channel's buffer is full, that's enough to
	// eventually activate all workers. Correspondingly, workers ready to pop from
	// the queue should try to steal an available token without blocking, and
	// maybe keep an idle worker from having to awaken.
	ready chan struct{}

	// For queues with a worker count, reattach is an unbuffered channel that
	// allows a detached worker to reestablish itself within the worker limit by
	// forcing another worker to exit. Workers ready to pop from the queue should
	// first try to handle a reattach request without blocking, and return if they
	// receive a token.
	reattach chan struct{}
}

// NewQueue creates a queue that uses the provided handler function to complete
// tasks.
//
// If workers > 0, the queue will run up to that number of tasks concurrently.
// If workers <= 0, the queue's concurrency is unbounded.
func NewQueue[K comparable, T any](workers int, handle Handler[K, T]) *Queue[K, T] {
	ctx, cancel := context.WithCancel(context.TODO())
	q := &Queue[K, T]{
		handle: handle,
		ctx:    ctx,
		cancel: cancel, // TODO: Call this somewhere.
		tasks:  make(map[K]*Task[T]),
	}
	if workers > 0 {
		q.ready = make(chan struct{}, workers)
		q.reattach = make(chan struct{})
		for i := 0; i < workers; i++ {
			go q.workOnQueue()
		}
	}
	return q
}

// NoValueHandler wraps handlers for queues that produce [NoValue], so the
// handler function can be written to only return an error.
func NoValueHandler[K comparable](handle func(context.Context, K) error) Handler[K, NoValue] {
	return func(ctx context.Context, key K) (_ NoValue, err error) {
		err = handle(ctx, key)
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
	tasks, newKeys := q.getOrCreateTasks(keys)
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
	if q.ready != nil {
		close(q.ready)
	}
}

func (q *Queue[K, T]) getOrCreateTasks(keys []K) (tasks TaskList[T], newKeys []K) {
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
	if q.ready == nil {
		q.scheduleUnqueued(keys)
	} else {
		q.scheduleQueued(keys)
	}
}

func (q *Queue[K, T]) scheduleUnqueued(keys []K) {
	for _, key := range keys {
		go q.completeTask(key)
	}
}

func (q *Queue[K, T]) scheduleQueued(keys []K) {
	q.queueMu.Lock()
	q.queue = append(q.queue, keys...)
	q.queueMu.Unlock()

	for range keys {
		select {
		case q.ready <- struct{}{}:
		default:
			return
		}
	}
}

func (q *Queue[K, T]) workOnQueue() {
	for {
		key, ok := q.tryPop()
		if !ok {
			select {
			case _, ok := <-q.ready:
				if ok {
					continue
				} else {
					return
				}
			case <-q.reattach:
				return
			}
		}

		taskCtx := q.completeTask(key)

		if taskCtx.detached {
			return
		}

		select {
		case <-q.reattach:
			return
		default:
		}

		select {
		case <-q.ready:
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

func (q *Queue[K, T]) completeTask(key K) *taskContext {
	q.tasksMu.Lock()
	task := q.tasks[key]
	q.tasksMu.Unlock()

	taskCtx := &taskContext{
		detach:   q.handleDetach,
		reattach: q.handleReattach,
	}
	ctx := context.WithValue(q.ctx, taskContextKey{}, taskCtx)

	task.value, task.err = q.handle(ctx, key)
	close(task.done)

	return taskCtx
}

func (q *Queue[K, T]) handleDetach() {
	if q.ready != nil {
		go q.workOnQueue()
	}
}

func (q *Queue[K, T]) handleReattach(ctx context.Context) error {
	if q.ready == nil {
		return nil
	}
	select {
	case q.reattach <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

type taskContextKey struct{}

type taskContext struct {
	detached bool

	detach   func()
	reattach func(context.Context) error
}

func getTaskContext(ctx context.Context) (taskCtx *taskContext, err error) {
	if v := ctx.Value(taskContextKey{}); v != nil {
		taskCtx = v.(*taskContext)
	} else {
		err = errors.New("context not associated with any queue")
	}
	return
}

// Detach unbounds the calling [Handler] from the concurrency limit of the
// [Queue] that invoked it, allowing the queue to immediately start work on
// another task. It returns an error if ctx is not associated with a [Queue], or
// if the handler has already detached.
//
// The corresponding [Reattach] function permits a detached handler to
// reestablish itself within the queue's concurrency limit ahead of submitted
// tasks whose handlers have not yet started.
//
// A typical use for detaching is to temporarily block on the completion of
// another task in the same queue, so that the current task may take advantage
// of caching or other side effects of the sibling task to improve performance.
// [KeyMutex] facilitates this pattern by automatically detaching from a queue
// while it waits for the lock on a key.
func Detach(ctx context.Context) error {
	taskCtx, err := getTaskContext(ctx)
	if err != nil {
		return err
	}

	if taskCtx.detached {
		return errors.New("task already detached from queue")
	}

	taskCtx.detach()
	taskCtx.detached = true
	return nil
}

// Reattach blocks the calling [Handler] until it can continue executing within
// the concurrency limit of the [Queue] that invoked it, or until ctx is
// canceled. It returns an error if ctx is not associated with a [Queue], or if
// the handler did not previously [Detach] from the queue. When ctx is canceled
// before the handler reattaches, the returned error is ctx.Err().
//
// See [Detach] for more information.
func Reattach(ctx context.Context) error {
	taskCtx, err := getTaskContext(ctx)
	if err != nil {
		return err
	}

	if !taskCtx.detached {
		return errors.New("task not detached from queue")
	}

	if err := taskCtx.reattach(ctx); err != nil {
		return err
	}
	taskCtx.detached = false
	return nil
}
