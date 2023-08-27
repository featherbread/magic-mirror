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

// Queue is a deduplicating work queue. It acts as a map that lazily computes
// and caches results corresponding to unique keys by calling a "handler"
// function in a new goroutine. It optionally limits the number of concurrent
// handler calls in flight, queueing keys for handling in the order that they
// are requested.
//
// The cached result for each key consists of a value and an error. Results that
// include a non-nil error receive no special treatment from the queue; they are
// cached as usual and their handlers are never retried.
//
// Handlers receive a context that allows them to [Detach] from a queue,
// temporarily increasing its concurrency limit. The context is never canceled.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	ctx    context.Context
	cancel context.CancelFunc

	tasks     map[K]*task[V]
	tasksMu   sync.Mutex
	tasksDone atomic.Uint64

	// Queues with limited concurrency share a single workState[K] through a
	// 1-buffered channel, which enables correct tracking and management of the
	// number of active goroutines calling handlers. This management is based on
	// the concept of implicit "work grants," which represent the right to perform
	// work within the queue's concurrency limit. As work grants are not directly
	// tracked within this state, we will comment on their handling throughout the
	// implementation.
	//
	// Queues with unlimited concurrency are represented by the absence of a state
	// channel.
	state     chan workState[K]
	maxGrants int
}

type workState[K comparable] struct {
	grants      int
	keys        []K
	reattachers []chan bool
}

// NewQueue creates a queue that uses the provided handler to compute the result
// for each key.
//
// If concurrency > 0, the queue will run up to that many concurrent handler
// calls in new goroutines (and potentially more if a handler calls [Detach]).
// If concurrency <= 0, the queue may run an unlimited number of concurrent
// handler calls.
func NewQueue[K comparable, V any](concurrency int, handle Handler[K, V]) *Queue[K, V] {
	ctx, cancel := context.WithCancel(context.TODO())
	q := &Queue[K, V]{
		handle: handle,
		ctx:    ctx,
		cancel: cancel, // TODO: Call this somewhere.
		tasks:  make(map[K]*task[V]),
	}
	if concurrency > 0 {
		q.maxGrants = concurrency
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

// Get returns the results for the provided key, either by immediately returning
// a computed result or by blocking until a corresponding call to the queue's
// handler is complete.
func (q *Queue[K, V]) Get(key K) (V, error) {
	return q.getTasks(key)[0].Wait()
}

// GetAll returns the corresponding values for the provided keys, or the first
// error among the results of the provided keys with respect to their ordering.
//
// When GetAll returns an error corresponding to one of the keys, it does not
// wait for handler calls corresponding to subsequent keys to complete. If it is
// necessary to associate errors with specific keys, or to wait for all handlers
// to complete even in the presence of errors, call [Queue.Get] for each key
// instead.
//
// For all keys whose results are not yet computed in a queue with limited
// concurrency, GetAll queues those keys for handling in the order in which they
// are presented, without interleaving keys from any other call to Get[All].
func (q *Queue[K, V]) GetAll(keys ...K) ([]V, error) {
	return q.getTasks(keys...).Wait()
}

// Stats returns information about the keys and results in the queue:
//
//   - done represents the number of keys whose results have been computed and
//     cached.
//   - submitted represents the total number of keys whose results have been
//     requested from the queue, including keys whose results are not yet
//     computed.
func (q *Queue[K, V]) Stats() (done, submitted uint64) {
	q.tasksMu.Lock()
	defer q.tasksMu.Unlock()
	done = q.tasksDone.Load()
	submitted = uint64(len(q.tasks))
	return
}

func (q *Queue[K, V]) getTasks(keys ...K) taskList[V] {
	tasks, newKeys := q.getOrCreateTasks(keys)
	if len(newKeys) > 0 {
		q.scheduleNewKeys(newKeys)
	}
	return tasks
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
	// Work grants first come into existence here, and are issued to the handlers
	// that we spawn to process new keys.
	state := <-q.state
	state.keys = append(state.keys, keys...)
	newGrants := min(q.maxGrants-state.grants, len(keys))
	state.grants += newGrants
	q.state <- state
	for i := 0; i < newGrants; i++ {
		go q.work()
	}
}

func (q *Queue[K, V]) work() {
	for {
		state := <-q.state

		if len(state.reattachers) > 0 {
			// We must attempt to transfer our work grant to a waiting reattacher
			// before processing any new keys.
			reattach := state.reattachers[0]
			state.reattachers = state.reattachers[1:]
			q.state <- state
			if <-reattach {
				// We have successfully transferred our work grant to the reattaching
				// handler, and as such must not perform any further work.
				return
			}
			// The reattaching handler bailed out before we could synchronize with
			// them. We retain the work grant and must continue to use it.
			continue
		}

		if len(state.keys) == 0 {
			// With no reattachers and no keys, we may safely retire the work grant.
			// The party that next queues a key or attempts to reattach must issue a
			// new work grant to consume any available capacity.
			state.grants -= 1
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

// handleDetach transfers or retires the work grant associated with the handler
// that calls it. Its behavior is undefined if the handler that calls it does
// not hold an active work grant.
func (q *Queue[K, V]) handleDetach() {
	if q.state == nil {
		return
	}
	state := <-q.state
	if len(state.keys) > 0 || len(state.reattachers) > 0 {
		// If our work grant is useful for any queued work, we must transfer it to a
		// new worker, or else the queue may deadlock.
		go q.work()
	} else {
		// Otherwise, we may safely retire the work grant.
		state.grants -= 1
	}
	q.state <- state
}

// handleReattach acquires or self-issues a work grant for the handler that
// calls it. Its behavior is undefined if the handler that calls it already
// holds an active work grant.
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

	if state.grants < q.maxGrants {
		// We have the capacity to self-issue a work grant.
		state.grants += 1
		q.state <- state
		return nil
	}

	// We do not have the capacity to issue a new work grant, so we must wait for
	// another worker to transfer theirs to us. As this is an unbuffered channel,
	// a successful send will synchronize precisely with a specific worker.
	reattach := make(chan bool)
	state.reattachers = append(state.reattachers, reattach)
	q.state <- state

	select {
	case reattach <- true:
		// We have successfully received a work grant.
		return nil
	case <-ctx.Done():
		// We can no longer wait to receive a work grant, and instead signal to the
		// worker that picks up our channel that they must retain their work grant.
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

func (ts taskList[V]) Wait() (values []V, err error) {
	values = make([]V, len(ts))
	for i, task := range ts {
		values[i], err = task.Wait()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

type taskContextKey struct{}

type taskContext struct {
	// detached indicates that the handler that owns this context does not have a
	// work grant for its associated queue.
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
// [Queue] that invoked it, allowing the queue to immediately start handling new
// keys. It returns an error if ctx is not associated with a [Queue], or if the
// handler has already detached.
//
// The corresponding [Reattach] function permits a detached handler to
// reestablish itself within the queue's concurrency limit ahead of the handling
// of new keys.
//
// A typical use for detaching is to temporarily block on the completion of
// another handler call for the same queue, where caching or other side effects
// performed by that handler may improve the performance of this handler.
// [KeyMutex] facilitates this pattern by automatically detaching from a queue
// while it waits for the lock on a key.
func Detach(ctx context.Context) error {
	taskCtx, err := getTaskContext(ctx)
	if err != nil {
		return err
	}

	if taskCtx.detached {
		return errors.New("already detached from queue")
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
		return errors.New("not detached from queue")
	}

	if err := taskCtx.reattach(ctx); err != nil {
		return err
	}
	taskCtx.detached = false
	return nil
}
