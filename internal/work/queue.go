package work

import (
	"sync"
	"sync/atomic"
)

// NoValue is the canonical empty value type for a queue.
type NoValue = struct{}

// Handler is the type for a queue's handler function.
type Handler[K comparable, V any] func(*QueueHandle, K) (V, error)

// Queue is a deduplicating work queue. It acts like a map that lazily computes
// and caches results corresponding to unique keys by calling a [Handler] in a
// new goroutine. It optionally limits the number of concurrent handler calls in
// flight, queueing keys for handling in the order that they are requested.
//
// The cached result for each key consists of a value and an error. Results that
// include a non-nil error receive no special treatment from the queue; they are
// cached as usual and their handlers are never retried.
//
// Handlers receive a [QueueHandle] that allows them to "detach" from a queue,
// temporarily increasing its concurrency limit. See [QueueHandle.Detach] for
// details.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

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
	reattachers []chan<- struct{}
}

// NewQueue creates a queue that uses the provided handler to compute the result
// for each key.
//
// If concurrency > 0, the queue will run up to that many handler calls
// concurrently in new goroutines (potentially more if a handler calls
// [QueueHandle.Detach]). If concurrency <= 0, the queue may run an unlimited
// number of concurrent handler calls.
func NewQueue[K comparable, V any](concurrency int, handle Handler[K, V]) *Queue[K, V] {
	q := &Queue[K, V]{
		handle: handle,
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
func NoValueHandler[K comparable](handle func(*QueueHandle, K) error) Handler[K, NoValue] {
	return func(qh *QueueHandle, key K) (_ NoValue, err error) {
		err = handle(qh, key)
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
			close(reattach)
			return // We have successfully transferred our work grant.
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

func (q *Queue[K, V]) completeTask(key K) *QueueHandle {
	q.tasksMu.Lock()
	task := q.tasks[key]
	q.tasksMu.Unlock()

	taskCtx := &QueueHandle{
		detach:   q.handleDetach,
		reattach: q.handleReattach,
	}

	task.value, task.err = q.handle(taskCtx, key)
	close(task.done)
	q.tasksDone.Add(1)

	return taskCtx
}

// handleDetach relinquishes the work grant held by the handler that calls it.
// Its behavior is undefined if its caller does not hold an outstanding work
// grant.
func (q *Queue[K, V]) handleDetach() {
	if q.state != nil {
		go q.work() // Transfer our work grant to a new worker.
	}
}

// handleReattach obtains a work grant for the handler that calls it. Its
// behavior is undefined if its caller already holds an outstanding work grant,
// or if its caller is not prepared to discharge all duties associated with a
// work grant.
func (q *Queue[K, V]) handleReattach() {
	if q.state == nil {
		return
	}

	state := <-q.state

	if state.grants < q.maxGrants {
		// There is capacity for a new work grant, so we must issue one.
		state.grants += 1
		q.state <- state
		return
	}

	// There is no capacity for a new work grant, so we must wait for one from an
	// existing holder, as indicated by the holder closing our channel.
	reattach := make(chan struct{})
	state.reattachers = append(state.reattachers, reattach)
	q.state <- state
	<-reattach
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

// QueueHandle allows a [Handler] to interact with its parent queue.
type QueueHandle struct {
	// detached indicates that the handler that owns this context has relinquished
	// its work grant.
	detached bool

	detach   func()
	reattach func()
}

// Detach unbounds the calling [Handler] from the concurrency limit of the
// [Queue] that invoked it, allowing the queue to immediately start handling
// other work. It returns true if the call detached the handler from the queue,
// or false if the handler was already detached.
//
// The corresponding Reattach function permits a detached handler to reestablish
// itself within the queue's concurrency limit ahead of the handling of new
// keys.
//
// A typical use for detaching is to temporarily block on the completion of
// another handler call for the same queue, where caching or other side effects
// performed by that handler may improve the performance of this handler.
// [KeyMutex] facilitates this pattern by automatically detaching from a queue
// while it waits for the lock on a key.
func (qh *QueueHandle) Detach() bool {
	if qh.detached {
		return false
	}
	qh.detach()
	qh.detached = true
	return true
}

// Reattach blocks the calling [Handler] until it can continue executing within
// the concurrency limit of the [Queue] that invoked it. It returns true if the
// call attached the handler to the queue, or false if the handler was already
// attached.
func (qh *QueueHandle) Reattach() bool {
	if !qh.detached {
		return false
	}
	qh.reattach()
	qh.detached = false
	return true
}
