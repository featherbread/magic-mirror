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
// and caches results corresponding to unique keys by calling a [Handler] in a
// new goroutine. It optionally limits the number of concurrent handler calls in
// flight, queueing keys for handling in the order that they are requested.
//
// The cached result for each key consists of a value and an error. Results that
// include a non-nil error receive no special treatment from the queue; they are
// cached as usual and their handlers are never retried.
//
// Handlers receive a context that allows them to [Detach] from a queue,
// temporarily increasing its concurrency limit. The context is never canceled.
// This is clearly a bad design, and at least one of these two things will
// change in the future.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	ctx    context.Context
	cancel context.CancelFunc

	tasks     map[K]*task[V]
	tasksMu   sync.Mutex
	tasksDone atomic.Uint64

	// Queues with limited concurrency share a workState through a 1-buffered
	// channel. Queues with unlimited concurrency are represented by a nil state
	// channel.
	state     chan workState[K]
	maxGrants int
}

// workState tracks pending work in a limited concurrency queue, along with the
// number of outstanding "work grants" that have been issued to execute that
// work.
//
// Work grants are an abstract concept not directly represented by any type or
// value. However, the issuance, transfer, and retiring of work grants is
// critical to the correct operation of a limited concurrency queue. In short,
// work grants represent both the right and the obligation to execute work on
// behalf of a queue. They operate as follows:
//
//   - In order to execute any work on behalf of a limited concurrency queue, an
//     outstanding work grant must be held.
//
//   - When a new work unit is dispatched to the queue while the number of
//     outstanding work grants is lower than the concurrency limit, the
//     dispatcher must immediately issue itself a new work grant and assume
//     responsibility for all duties associated with it. When the number of
//     outstanding work grants is not lower than the concurrency limit, the
//     dispatcher must queue the work unit for later execution by an existing
//     work grant holder.
//
//   - If the holder of a work grant does not intend to continue executing work
//     on behalf of the queue, and cannot retire the work grant as described
//     below, it must transfer the work grant to a holder who can continue to
//     discharge the duties associated with it, and cease to discharge any of
//     those duties itself.
//
//   - If the holder of a work grant cannot find any additional work units to
//     execute on behalf of the queue, it must retire the work grant and cease
//     to discharge any duties associated with it.
type workState[K comparable] struct {
	grants      int
	keys        []K
	reattachers []chan bool
}

// NewQueue creates a queue that uses the provided handler to compute the result
// for each key.
//
// If concurrency > 0, the queue will run up to that many handler calls
// concurrently in new goroutines (potentially more if a handler calls
// [Detach]). If concurrency <= 0, the queue may run an unlimited number of
// concurrent handler calls.
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
//
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
	// Because we are dispatching new work units, we must issue as many new work
	// grants as the concurrency limit allows. We transfer each work grant to a
	// worker goroutine that can discharge all duties associated with it.
	state := <-q.state
	state.keys = append(state.keys, keys...)
	newGrants := min(q.maxGrants-state.grants, len(keys))
	state.grants += newGrants
	q.state <- state
	for i := 0; i < newGrants; i++ {
		go q.work()
	}
}

// work, when invoked in a new goroutine, accepts ownership of a work grant and
// discharges all duties associated with it.
func (q *Queue[K, V]) work() {
	for {
		state := <-q.state

		if len(state.reattachers) > 0 {
			// See handleReattach for details.
			reattach := state.reattachers[0]
			state.reattachers = state.reattachers[1:]
			q.state <- state
			if <-reattach {
				return // We have successfully transferred our work grant.
			}
			continue // The reattacher bailed, so we retain the work grant.
		}

		if len(state.keys) == 0 {
			// With no reattachers and no keys, we have no pending work and must
			// retire the work grant.
			state.grants -= 1
			q.state <- state
			return
		}

		// We have pending work and must use our work grant to execute it.
		key := state.keys[0]
		state.keys = state.keys[1:]
		q.state <- state

		taskCtx := q.completeTask(key)
		if taskCtx.detached {
			return // We no longer have a work grant.
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

// handleDetach relinquishes the work grant held by the handler that calls it.
// Its behavior is undefined if its caller does not hold an outstanding work
// grant.
func (q *Queue[K, V]) handleDetach() {
	if q.state == nil {
		return
	}

	// It is always correct to simply transfer our work grant to a new worker.
	// However, if we can get access to the state without blocking, we'll see if
	// we can retire the work grant without having to spawn a new goroutine.
	select {
	case state := <-q.state:
		if len(state.keys) == 0 && len(state.reattachers) == 0 {
			state.grants -= 1 // There is no pending work, so we can retire the work grant.
		} else {
			go q.work() // There is pending work, so we must transfer the work grant.
		}
		q.state <- state

	default:
		go q.work() // Transfer the work grant immediately to avoid blocking the caller.
	}
}

// handleReattach obtains a work grant for the handler that calls it, or returns
// ctx.Err() if ctx is canceled before the work grant is obtained. Its behavior
// is undefined if its caller already holds an outstanding work grant, or if its
// caller is not prepared to discharge all duties associated with a work grant.
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
		// There is capacity for a new work grant, so we must issue one.
		state.grants += 1
		q.state <- state
		return nil
	}

	// There is no capacity for a new work grant, so we must obtain one from an
	// existing holder. Because this is an unbuffered channel, a successful send
	// will synchronize perfectly with the receive from inside of work(),
	// representing a mutual agreement to the transfer.
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
	// detached indicates that the handler that owns this context has relinquished
	// its work grant.
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
// [Queue] that invoked it, allowing the queue to immediately start handling
// other work. It returns an error if ctx is not associated with a [Queue], or
// if the handler has already detached.
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
