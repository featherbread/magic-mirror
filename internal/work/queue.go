package work

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// NoValue is the canonical empty value type for a queue.
type NoValue = struct{}

// Handler is the type for a queue's handler function.
type Handler[K comparable, V any] func(context.Context, K) (V, error)

// Queue is a deduplicating work queue. It maps each unique key provided to
// GetOrSubmit[All] to a single [Task], which acts as a promise for the result
// of running a handler function with that key. Tasks may return a value and an
// error; when a task produces an error, the queue does not retry it. After all
// keys have been submitted to a queue, CloseSubmit should be called to permit
// the release of resources associated with it.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	ctx    context.Context
	cancel context.CancelFunc

	tasks     map[K]*task[V]
	tasksMu   sync.Mutex
	tasksDone atomic.Uint64

	maxWorkers int
	state      chan workState[K]
}

type workState[K comparable] struct {
	keys        []K
	workers     int
	reattachers []chan bool
}

// NewQueue creates a queue that uses the provided handler function to complete
// tasks.
//
// If concurrency > 0, the queue will run up to that number of tasks concurrently.
// If concurrency <= 0, the queue's concurrency is unbounded.
func NewQueue[K comparable, V any](concurrency int, handle Handler[K, V]) *Queue[K, V] {
	ctx, cancel := context.WithCancel(context.TODO())
	q := &Queue[K, V]{
		handle: handle,
		ctx:    ctx,
		cancel: cancel, // TODO: Call this somewhere.
		tasks:  make(map[K]*task[V]),
	}
	if concurrency > 0 {
		q.maxWorkers = concurrency
		q.state = make(chan workState[K], 1)
		q.state <- workState[K]{}
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

// Get returns the results for the provided key, either by reading the results
// already computed by the queue's handler, or by scheduling a new call to the
// handler and blocking until it is complete.
func (q *Queue[K, V]) Get(key K) (V, error) {
	return q.getTask(key).Wait()
}

func (q *Queue[K, V]) getTask(key K) *task[V] {
	return q.getAllTasks(key)[0]
}

// GetAll returns the corresponding values for all of the provided keys, along
// with the concatenation of their corresponding errors following the semantics
// of [errors.Join], either by reading results already computed or by scheduling
// new handler calls and blocking until they are complete.
func (q *Queue[K, V]) GetAll(keys ...K) ([]V, error) {
	return q.getAllTasks(keys...).Wait()
}

func (q *Queue[K, V]) getAllTasks(keys ...K) taskList[V] {
	tasks, newKeys := q.getOrCreateTasks(keys)
	if len(newKeys) > 0 {
		q.scheduleNewKeys(newKeys)
	}
	return tasks
}

// Stats returns information about the number of tasks in the queue:
//
//   - done represents the number of tasks whose handlers have finished executing.
//   - submitted represents the total number of unique tasks in the queue.
func (q *Queue[K, V]) Stats() (done, submitted uint64) {
	q.tasksMu.Lock()
	defer q.tasksMu.Unlock()
	done = q.tasksDone.Load()
	submitted = uint64(len(q.tasks))
	return
}

func (q *Queue[K, V]) getOrCreateTasks(keys []K) (tasks taskList[V], newKeys []K) {
	tasks = make(taskList[V], len(keys))

	q.tasksMu.Lock()
	defer q.tasksMu.Unlock()

	for i, key := range keys {
		if task, ok := q.tasks[key]; ok {
			tasks[i] = task
			continue
		}
		task := &task[V]{done: make(chan struct{})}
		q.tasks[key] = task
		tasks[i] = task
		newKeys = append(newKeys, key)
	}
	return
}

func (q *Queue[K, V]) scheduleNewKeys(keys []K) {
	if q.state == nil {
		q.scheduleUnlimited(keys)
	} else {
		q.scheduleLimited(keys)
	}
}

func (q *Queue[K, V]) scheduleUnlimited(keys []K) {
	for _, key := range keys {
		go q.completeTask(key)
	}
}

func (q *Queue[K, V]) scheduleLimited(keys []K) {
	state := <-q.state
	state.keys = append(state.keys, keys...)
	newWorkers := min(q.maxWorkers-state.workers, len(keys))
	state.workers += newWorkers
	q.state <- state
	for i := 0; i < newWorkers; i++ {
		go q.work()
	}
}

func (q *Queue[K, V]) work() {
	for {
		state := <-q.state

		if len(state.reattachers) > 0 {
			reattach := state.reattachers[0]
			state.reattachers = state.reattachers[1:]
			q.state <- state
			if <-reattach {
				return
			}
			continue
		}

		if len(state.keys) == 0 {
			state.workers -= 1
			q.state <- state
			return
		}

		key := state.keys[0]
		state.keys = state.keys[1:]
		q.state <- state

		taskCtx := q.completeTask(key)
		if taskCtx.detached {
			return
		}
	}
}

func (q *Queue[K, V]) completeTask(key K) *taskContext {
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
	q.tasksDone.Add(1)

	return taskCtx
}

func (q *Queue[K, V]) handleDetach() {
	if q.state == nil {
		return
	}
	state := <-q.state
	if len(state.keys) > 0 || len(state.reattachers) > 0 {
		go q.work()
	} else {
		state.workers -= 1
	}
	q.state <- state
}

func (q *Queue[K, V]) handleReattach(ctx context.Context) error {
	if q.state == nil {
		return nil
	}

	var state workState[K]
	select {
	case state = <-q.state:
	case <-ctx.Done():
		return ctx.Err()
	}

	if state.workers < q.maxWorkers {
		state.workers += 1
		q.state <- state
		return nil
	}

	reattach := make(chan bool)
	state.reattachers = append(state.reattachers, reattach)
	q.state <- state

	select {
	case reattach <- true:
		return nil
	case <-ctx.Done():
		close(reattach)
		return ctx.Err()
	}
}

type task[V any] struct {
	done  chan struct{}
	value V
	err   error
}

func (t *task[V]) Wait() (V, error) {
	<-t.done
	return t.value, t.err
}

type taskList[V any] []*task[V]

func (ts taskList[V]) Wait() ([]V, error) {
	values := make([]V, len(ts))
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
