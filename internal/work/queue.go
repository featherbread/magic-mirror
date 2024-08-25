package work

import (
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"
)

// NoValue is the canonical empty value type for a [Queue].
type NoValue = struct{}

// Handler is the type for a [Queue]'s handler function.
type Handler[K comparable, V any] func(*QueueHandle, K) (V, error)

// Queue is a deduplicating work queue. It acts like a map that computes and
// caches the result for each unique key by calling a [Handler] in a new
// goroutine. It optionally limits the concurrency of handlers in flight,
// computing results in the order that keys are requested.
//
// The cached result for each key consists of a value and an error. Results with
// non-nil errors receive no special treatment; a Queue caches them as usual and
// never retries their handlers.
//
// Handlers receive a [QueueHandle] that allows them to detach from the queue,
// temporarily increasing its concurrency limit. See [QueueHandle.Detach] for
// details.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	state    workState[K]
	stateMu  sync.Mutex
	reattach chan struct{}

	tasks     map[K]*task[V]
	tasksMu   sync.Mutex
	tasksDone atomic.Uint64
}

// workState tracks the pending work in a queue, along with the outstanding
// "work grants" issued to handle that work.
//
// Work grants are an abstract concept not directly represented by any type or
// value. Their correct issuance, transfer, and retirement is critical to the
// maintenance of the queue's concurrency limit. They represent both the right
// and the obligation to execute work on behalf of a queue, and operate as
// follows:
//
//   - To execute work on behalf of a queue, a work grant must be held.
//
//   - To initiate new work when the number of outstanding work grants is lower
//     than the maximum, a work grant must be issued (by incrementing grants),
//     and its recipient must assume responsibility for all duties associated
//     with it.
//
//   - To initiate new work when the number of outstanding work grants is not
//     lower than the maximum, it must be queued for later handling by an
//     existing work grant holder.
//
//   - The holder of a work grant must handle queued work after finishing its
//     current work. Should it find no queued work, it must retire the work
//     grant (by decrementing grants) and cease to fulfill the duties associated
//     with it.
//
//   - To stop handling queued work, the holder of a work grant must retire it
//     if able, or transfer it to a worker who can continue to fulfill the
//     duties associated with it.
type workState[K comparable] struct {
	grants      int
	maxGrants   int
	reattachers int
	keys        *deque.Deque[K]
}

// NewQueue creates a queue that uses the provided handler to compute the result
// for each key.
//
// If concurrency > 0, the queue will run up to that many handlers concurrently
// in new goroutines, unless a handler calls [QueueHandle.Detach] to unbound
// itself from the concurrency limit. If concurrency <= 0, the queue will permit
// up to [math.MaxInt] concurrent handlers; that is, effectively unlimited
// concurrency.
//
// If the handler panics or calls [runtime.Goexit], every Get[All][Urgent] call
// with that key will panic with the same value or invoke Goexit, respectively.
func NewQueue[K comparable, V any](concurrency int, handle Handler[K, V]) *Queue[K, V] {
	state := workState[K]{
		keys:      deque.New[K](),
		maxGrants: concurrency,
	}
	if state.maxGrants <= 0 {
		state.maxGrants = math.MaxInt
	}
	return &Queue[K, V]{
		handle:   handle,
		state:    state,
		tasks:    make(map[K]*task[V]),
		reattach: make(chan struct{}),
	}
}

// NoValueHandler wraps handlers for queues that produce [NoValue], so the
// handler function can be written to only return an error.
func NoValueHandler[K comparable](handle func(*QueueHandle, K) error) Handler[K, NoValue] {
	return func(qh *QueueHandle, key K) (_ NoValue, err error) {
		err = handle(qh, key)
		return
	}
}

// Get returns the result for the provided key, blocking if necessary until a
// corresponding call to the queue's handler finishes.
func (q *Queue[K, V]) Get(key K) (V, error) {
	return q.getTasks(pushAllBack, key)[0].Wait()
}

// GetUrgent behaves like [Queue.Get], but pushes the key to the front of the
// queue (rather than the back) if it is not yet handled.
func (q *Queue[K, V]) GetUrgent(key K) (V, error) {
	return q.getTasks(pushAllFront, key)[0].Wait()
}

// GetAll returns the corresponding values for the provided keys, or the first
// error among the results of the provided keys with respect to their ordering.
//
// When GetAll returns an error, it does not wait for handlers corresponding to
// subsequent keys to finish. To associate errors with specific keys, or to
// wait for all handlers even in the presence of errors, call [Queue.Get] for
// each key instead.
//
// When a handler for one of the provided keys panics or calls [runtime.Goexit],
// GetAll propagates the first panic or Goexit among the provided keys with
// respect to their ordering.
//
// When concurrency limits require handlers for some keys to be queued, GetAll
// queues the unhandled keys in the order provided, without interleaving keys
// from any other call to Get[All]. Keys subsequently queued by Get[All]Urgent
// may, however, be interleaved between these keys.
func (q *Queue[K, V]) GetAll(keys ...K) ([]V, error) {
	return q.getTasks(pushAllBack, keys...).Wait()
}

// GetAllUrgent behaves like [Queue.GetAll], but pushes unhandled keys to the
// front of the queue (rather than the back), in the order provided and without
// interleaving keys from any other call to Get[All].
func (q *Queue[K, V]) GetAllUrgent(keys ...K) ([]V, error) {
	return q.getTasks(pushAllFront, keys...).Wait()
}

// Stats returns information about the keys and results in the queue:
//
//   - done is the number of keys whose results are computed and cached.
//
//   - submitted is the number of keys whose results have been requested from
//     the queue, including keys whose results are not yet computed.
func (q *Queue[K, V]) Stats() (done, submitted uint64) {
	done = q.tasksDone.Load()
	q.tasksMu.Lock()
	submitted = uint64(len(q.tasks))
	q.tasksMu.Unlock()
	return
}

func (q *Queue[K, V]) getTasks(enqueue enqueueFunc[K], keys ...K) taskList[V] {
	tasks, newKeys := q.getOrCreateTasks(keys)
	q.scheduleNewKeys(enqueue, newKeys)
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

func (q *Queue[K, V]) scheduleNewKeys(enqueue enqueueFunc[K], keys []K) {
	if len(keys) == 0 {
		return // No need to lock up the state.
	}

	// To enqueue new keys, we must issue as many new work grants as the
	// concurrency limit allows, and transfer them to workers who can fulfill all
	// duties associated with them.
	q.stateMu.Lock()
	newGrants := min(q.state.maxGrants-q.state.grants, len(keys))
	initialKeys, queuedKeys := keys[:newGrants], keys[newGrants:]
	q.state.grants += newGrants
	enqueue(q.state.keys, queuedKeys)
	q.stateMu.Unlock()

	for _, key := range initialKeys {
		go q.work(&key)
	}
}

// enqueueFunc adds new keys to an internal queue for later handling.
type enqueueFunc[T any] func(*deque.Deque[T], []T)

func pushAllBack[T any](d *deque.Deque[T], all []T) {
	for _, v := range all {
		d.PushBack(v)
	}
}

func pushAllFront[T any](d *deque.Deque[T], all []T) {
	for _, v := range slices.Backward(all) {
		d.PushFront(v)
	}
}

// work, when invoked in a new goroutine, accepts ownership of a work grant and
// fulfills all duties associated with it. If provided with an initial key, it
// executes the task for that key before handling queued work.
func (q *Queue[K, V]) work(initialKey *K) {
	for {
		var key K
		if initialKey != nil {
			key, initialKey = *initialKey, nil
		} else {
			var ok bool
			key, ok = q.tryGetQueuedKey()
			if !ok {
				return // We no longer have a work grant; see tryGetQueuedKey.
			}
		}

		q.tasksMu.Lock()
		task := q.tasks[key]
		q.tasksMu.Unlock()

		qh := &QueueHandle{
			detach:   q.handleDetach,
			reattach: q.handleReattach,
		}
		func() {
			defer func() {
				q.tasksDone.Add(1)
				if task.goexit {
					go q.work(nil) // We can't stop Goexit, so we must transfer our work grant.
				}
			}()
			task.Do(func() (V, error) { return q.handle(qh, key) })
		}()
		if qh.detached {
			return // We no longer have a work grant.
		}
	}
}

// tryGetQueuedKey, when called with a work grant held, either relinquishes the
// work grant (returning ok == false) or returns a key (ok == true) whose work
// the caller must execute.
func (q *Queue[K, V]) tryGetQueuedKey() (key K, ok bool) {
	q.stateMu.Lock()
	return q.tryGetQueuedKeyLocked()
}

func (q *Queue[K, V]) tryGetQueuedKeyLocked() (key K, ok bool) {
	if q.state.reattachers > 0 {
		// We can transfer our work grant to a reattacher; see handleReattach for
		// details.
		q.state.reattachers -= 1
		q.stateMu.Unlock()
		<-q.reattach
		return
	}

	if q.state.keys.Len() == 0 {
		// With no reattachers and no keys, we have no pending work and must
		// retire the work grant.
		q.state.grants -= 1
		q.stateMu.Unlock()
		return
	}

	// We have pending work and must use the work grant to execute it.
	key = q.state.keys.PopFront()
	q.stateMu.Unlock()
	ok = true
	return
}

// handleDetach relinquishes the work grant held by the handler that calls it.
// Its behavior is undefined if its caller does not hold an outstanding work
// grant.
func (q *Queue[K, V]) handleDetach() {
	if q.stateMu.TryLock() {
		// If we can quickly get a lock on the state, try to relinquish the work
		// grant directly instead of starting a new worker.
		key, ok := q.tryGetQueuedKeyLocked()
		if ok {
			go q.work(&key)
		}
	} else {
		// Otherwise, transfer the work grant so we don't block the detach.
		go q.work(nil)
	}
}

// handleReattach obtains a work grant for the handler that calls it. Its
// behavior is undefined if its caller already holds an outstanding work grant,
// or if its caller is not prepared to fulfill all duties associated with a work
// grant.
func (q *Queue[K, V]) handleReattach() {
	q.stateMu.Lock()

	if q.state.grants < q.state.maxGrants {
		// There is capacity for a new work grant, so we must issue one.
		q.state.grants += 1
		q.stateMu.Unlock()
		return
	}

	// There is no capacity for a new work grant, so we must inform an existing
	// worker that a reattacher is ready to take theirs.
	q.state.reattachers += 1
	q.stateMu.Unlock()
	q.reattach <- struct{}{}
}

// QueueHandle allows a [Handler] to interact with its parent queue.
type QueueHandle struct {
	// detached indicates that the goroutine running the handler has relinquished
	// its work grant.
	detached bool
	detach   func()
	reattach func()
}

// Detach unbounds the calling [Handler] from the concurrency limit of the
// [Queue] that invoked it, allowing the queue to immediately handle other work.
// It returns true if this call detached the handler, or false if the handler
// had already detached.
//
// [QueueHandle.Reattach] permits a detached handler to reestablish itself
// within the queue's concurrency limit ahead of the handling of new keys.
//
// A typical use for detaching is to block on the completion of another handler
// for the same queue, where caching or other side effects from that handler
// may be useful. [KeyMutex] facilitates this by detaching from a queue while
// awaiting a lock on a key.
func (qh *QueueHandle) Detach() bool {
	if qh.detached {
		return false
	}
	qh.detach()
	qh.detached = true
	return true
}

// Reattach blocks the calling [Handler] until it can execute within the
// concurrency limit of the [Queue] that invoked it. It has no effect if the
// handler is already attached.
func (qh *QueueHandle) Reattach() {
	if qh.detached {
		qh.reattach()
		qh.detached = false
	}
}

type task[V any] struct {
	wg       sync.WaitGroup
	complete bool
	goexit   bool
	value    V
	err      error
	panicval any
}

func (t *task[V]) Do(fn func() (V, error)) {
	defer t.wg.Done()
	t.goexit = true
	func() {
		defer func() { t.panicval = recover() }()
		t.value, t.err = fn()
		t.complete = true
	}()
	t.goexit = false
}

func (t *task[V]) Wait() (V, error) {
	t.wg.Wait()
	switch {
	case t.complete:
		return t.value, t.err
	case t.goexit:
		runtime.Goexit()
		panic("continued after runtime.Goexit")
	default:
		panic(t.panicval)
	}
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
