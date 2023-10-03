package work

import (
	"sync"
	"sync/atomic"
)

// NoValue is the canonical empty value type for a [Queue].
type NoValue = struct{}

// Handler is the type for a [Queue]'s handler function.
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
// Handlers receive a [QueueHandle] that allows them to detach from the queue,
// temporarily increasing its concurrency limit. See [QueueHandle.Detach] for
// details.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	// Queues with unlimited concurrency have maxGrants == 0. Otherwise, maxGrants
	// is the maximum allowed number of outstanding work grants; see workState for
	// details.
	maxGrants int

	state   workState[K]
	stateMu sync.Mutex

	tasks     map[K]*task[V]
	tasksMu   sync.Mutex
	tasksDone atomic.Uint64
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
	reattachers []chan<- struct{} // TODO: CHANNELS ARE ALREADY FIFO QUEUES!
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
	done = q.tasksDone.Load()
	q.tasksMu.Lock()
	submitted = uint64(len(q.tasks))
	q.tasksMu.Unlock()
	return
}

func (q *Queue[K, V]) getTasks(keys ...K) taskList[V] {
	tasks, newKeys := q.getOrCreateTasks(keys)
	q.scheduleNewKeys(newKeys)
	return tasks
}

func (q *Queue[K, V]) getOrCreateTasks(keys []K) (tasks taskList[V], newKeys []K) {
	tasks = make(taskList[V], len(keys))
	newKeys = make([]K, 0, len(keys))

	q.tasksMu.Lock()
	defer q.tasksMu.Unlock()

	for i, key := range keys {
		if task, ok := q.tasks[key]; ok {
			tasks[i] = task
			continue
		}
		task := &task[V]{}
		task.wg.Add(1)
		q.tasks[key] = task
		tasks[i] = task
		newKeys = append(newKeys, key)
	}
	return
}

func (q *Queue[K, V]) scheduleNewKeys(keys []K) {
	if q.maxGrants == 0 {
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
	if len(keys) == 0 {
		return // No need to lock up the state.
	}

	// Because we are dispatching new work units, we must issue as many new work
	// grants as the concurrency limit allows. We transfer each work grant to a
	// worker goroutine that can discharge all duties associated with it.
	q.stateMu.Lock()
	newGrants := min(q.maxGrants-q.state.grants, len(keys))
	initialKeys, queuedKeys := keys[:newGrants], keys[newGrants:]
	q.state.grants += newGrants
	q.state.keys = append(q.state.keys, queuedKeys...)
	q.stateMu.Unlock()

	for _, key := range initialKeys {
		go q.work(key)
	}
}

// work, when invoked in a new goroutine, accepts ownership of a work grant and
// discharges all duties associated with it. If provided with a single initial
// key, it will execute the task associated with that key before looking for any
// queued work.
func (q *Queue[K, V]) work(initialKey ...K) {
	for {
		var key K
		switch {
		case len(initialKey) > 1:
			panic("worker received more than one initial key")

		case len(initialKey) == 1:
			key, initialKey = initialKey[0], nil

		default:
			var ok bool
			key, ok = q.tryGetQueuedKey()
			if !ok {
				return // We no longer have a work grant; see tryGetQueuedKey.
			}
		}

		detached := q.completeTask(key)
		if detached {
			return // We no longer have a work grant.
		}
	}
}

// tryGetQueuedKey, when called with a work grant held, either relinquishes the
// work grant (returning ok == false) or returns a key (with ok == true) whose
// associated work the caller must execute.
func (q *Queue[K, V]) tryGetQueuedKey() (key K, ok bool) {
	q.stateMu.Lock()
	return q.tryGetQueuedKeyLocked()
}

// tryGetQueuedKeyLocked behaves like [Queue.tryGetQueuedKey], but assumes that
// q.stateMu is locked on entry and unlocks q.stateMu before it returns.
func (q *Queue[K, V]) tryGetQueuedKeyLocked() (key K, ok bool) {
	if len(q.state.reattachers) > 0 {
		// We can transfer our work grant to a reattacher; see handleReattach for
		// details.
		reattach := q.state.reattachers[0]
		q.state.reattachers = q.state.reattachers[1:]
		q.stateMu.Unlock()
		close(reattach)
		return
	}

	if len(q.state.keys) == 0 {
		// With no reattachers and no keys, we have no pending work and must
		// retire the work grant.
		q.state.grants -= 1
		q.stateMu.Unlock()
		return
	}

	// We have pending work and must use the work grant to execute it.
	key = q.state.keys[0]
	q.state.keys = q.state.keys[1:]
	q.stateMu.Unlock()
	ok = true
	return
}

func (q *Queue[K, V]) completeTask(key K) (detached bool) {
	q.tasksMu.Lock()
	task := q.tasks[key]
	q.tasksMu.Unlock()

	qh := &QueueHandle{
		detach:   q.handleDetach,
		reattach: q.handleReattach,
	}
	task.value, task.err = q.handle(qh, key)
	q.tasksDone.Add(1)
	task.wg.Done()
	return qh.detached
}

// handleDetach relinquishes the work grant held by the handler that calls it.
// Its behavior is undefined if its caller does not hold an outstanding work
// grant.
func (q *Queue[K, V]) handleDetach() bool {
	if q.maxGrants == 0 {
		return false
	}

	// If we can quickly get a lock on the state, we'll see if we can relinquish
	// the work grant directly instead of starting a new worker.
	if q.stateMu.TryLock() {
		key, ok := q.tryGetQueuedKeyLocked()
		if ok {
			go q.work(key)
		}
		return true
	}

	// Otherwise, we'll simply transfer our work grant to a new worker and let it
	// figure out what to do, so we don't block the detach.
	go q.work()
	return true
}

// handleReattach obtains a work grant for the handler that calls it. Its
// behavior is undefined if its caller already holds an outstanding work grant,
// or if its caller is not prepared to discharge all duties associated with a
// work grant.
func (q *Queue[K, V]) handleReattach() {
	if q.maxGrants == 0 {
		return
	}

	q.stateMu.Lock()

	if q.state.grants < q.maxGrants {
		// There is capacity for a new work grant, so we must issue one.
		q.state.grants += 1
		q.stateMu.Unlock()
		return
	}

	// There is no capacity for a new work grant, so we must wait for one from an
	// existing holder, as indicated by the holder closing our channel.
	reattach := make(chan struct{})
	q.state.reattachers = append(q.state.reattachers, reattach)
	q.stateMu.Unlock()
	<-reattach
}

// QueueHandle allows a [Handler] to interact with its parent queue.
type QueueHandle struct {
	// detached indicates that the handler is detached from its queue. In the case
	// of a limited concurrency queue, this means that the goroutine running the
	// handler has relinquished its work grant.
	detached bool
	detach   func() bool
	reattach func()
}

// Detach unbounds the calling [Handler] from any concurrency limit on the
// [Queue] that invoked it, allowing the queue to immediately start handling
// other work. It returns true if the call actually unbound the handler from a
// limit it was previously subject to, or false if the handler was already
// executing outside of a concurrency limit, either because the handler
// previously detached or because the queue's concurrency is unlimited.
//
// [QueueHandle.Reattach] permits a detached handler to reestablish itself
// within the queue's concurrency limit ahead of the handling of new keys.
//
// A typical use for detaching is to block on the completion of another handler
// call for the same queue, where caching or other side effects performed by
// that handler may improve the performance of this handler. [KeyMutex]
// facilitates this pattern by automatically detaching from a queue while it
// waits for the lock on a key.
func (qh *QueueHandle) Detach() bool {
	if qh.detached {
		return false
	}
	qh.detached = qh.detach()
	return qh.detached
}

// Reattach blocks the calling [Handler] until it can continue executing within
// the concurrency limit of the [Queue] that invoked it. It has no effect if the
// handler is already attached to the queue, or if the queue's concurrency is
// unlimited.
func (qh *QueueHandle) Reattach() {
	if qh.detached {
		qh.reattach()
		qh.detached = false
	}
}

type task[V any] struct {
	wg    sync.WaitGroup
	value V
	err   error
}

func (t *task[V]) Wait() (V, error) {
	t.wg.Wait()
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
