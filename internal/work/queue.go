package work

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"

	"github.com/ahamlinman/magic-mirror/internal/work/catch"
)

// Handler is a type for a [Queue]'s handler function.
type Handler[K comparable, V any] = func(*QueueHandle, K) (V, error)

// Queue runs a [Handler] once per key in a distinct goroutine and caches the
// result, while imposing dynamic concurrency limits on handler executions.
//
// The result for each key nominally consists of a value and error, but may
// instead capture a panic or a call to [runtime.Goexit], which the queue
// propagates to any caller retrieving that key's result.
//
// Each queue is initialized with a limit on the number of goroutines that may
// concurrently handle new keys. However, [QueueHandle.Detach] permits a handler
// to exclude itself from the concurrency limit for the remainder of its own
// lifetime, or until it calls [QueueHandle.Reattach]. [KeyMutex] in particular
// helps handlers temporarily detach from their queue while awaiting exclusive
// use of a shared resource, typically one identified by a subset of the
// handler's current key.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	state    workState[K]
	stateMu  sync.Mutex
	reattach reattachQueue

	tasks        map[K]*task[V]
	tasksMu      sync.Mutex
	tasksHandled atomic.Uint64
}

// workState tracks the pending work in a queue, along with the "work grants"
// that control the concurrency of goroutines handling that work.
//
// A work grant is an abstract resource that both permits and obligates the
// goroutine holding it to execute the queue's handler for any pending keys.
// Work grants are issued (by incrementing grants), retired (by decrementing
// grants), and transferred between goroutines to maintain the following
// invariants:
//
//   - Exactly one work grant is outstanding for every unhandled key known to
//     the queue, up to a limit of maxGrants.
//   - No goroutine holds more than one work grant.
//   - Any goroutine holding a work grant is either executing the handler for an
//     unhandled key or maintaining these invariants.
//
// Handlers that detach from the queue relinquish their goroutine's work grant
// to continue execution outside of the concurrency limit. Reattaching handlers
// that wish to re-obtain their work grant should receive priority over the
// handling of new keys.
type workState[K comparable] struct {
	grants      int
	maxGrants   int
	reattachers int
	keys        deque.Deque[K]
}

// reattachQueue defines the protocol by which a goroutine transfers its work
// grant to a reattaching handler. Counterintuitively, the grantee _sends_ into
// the unbuffered channel to obtain the grant, while the grantor _receives_ from
// the channel to unblock them. The thinking is that Go's spec defines channels
// as FIFO queues, so if we want reattachers to resume in FIFO order we should
// queue them up on the sending side. Though in practice, for a variety of
// reasons, the order probably doesn't matter much.
type reattachQueue chan struct{}

func (rq reattachQueue) BequeathGrant() { <-rq }
func (rq reattachQueue) ObtainGrant()   { rq <- struct{}{} }

// task represents a unit of pending or handled work for a single key.
type task[V any] struct {
	wg     sync.WaitGroup
	result catch.Result[V]
}

func (t *task[V]) Handle(fn func() (V, error)) {
	defer t.wg.Done()
	t.result = catch.Goexit[V]()
	t.result = catch.DoOrExit(fn)
}

func (t *task[V]) Wait() (V, error) {
	t.wg.Wait()
	return t.result.Unwrap()
}

// NewQueue creates a [Queue] with the provided concurrency limit and handler.
//
// If concurrency <= 0, the queue is created with an effectively unlimited
// concurrency of [math.MaxInt].
func NewQueue[K comparable, V any](concurrency int, handle Handler[K, V]) *Queue[K, V] {
	state := workState[K]{maxGrants: concurrency}
	if state.maxGrants <= 0 {
		state.maxGrants = math.MaxInt
	}
	return &Queue[K, V]{
		handle:   handle,
		state:    state,
		tasks:    make(map[K]*task[V]),
		reattach: make(reattachQueue),
	}
}

// Inform advises the queue of keys that it should ensure are handled and cached
// as soon as possible.
//
// Inform has no effect on keys already handled or pending in the queue. The new
// keys among those provided are enqueued at the back of the queue, in the order
// given, without interleaving the keys of any other enqueue operation. A future
// [Queue.InformFront] call may interpose new keys between those enqueued in a
// single Inform call.
func (q *Queue[K, V]) Inform(keys ...K) {
	q.getTasks(pushAllBack, keys...)
}

// InformFront behaves like [Queue.Inform], but enqueues new keys at the front
// of the queue rather than the back. Like Inform, it does not affect the order
// of keys already pending; it is not possible to move pending keys to the front
// of the queue.
func (q *Queue[K, V]) InformFront(keys ...K) {
	q.getTasks(pushAllFront, keys...)
}

// Get informs the queue of the key as if by [Queue.Inform], blocks until the
// queue has handled the key, then propagates its result: returning its value
// and error, or forwarding a panic or [runtime.Goexit].
func (q *Queue[K, V]) Get(key K) (V, error) {
	return q.getTasks(pushAllBack, key)[0].Wait()
}

// Collect informs the queue of the keys as if by [Queue.Inform], then coalesces
// their results. If any key's handler returns an error, panics, or calls
// [runtime.Goexit], Collect propagates the first of those results with respect
// to the order of the keys, without waiting for the queue to handle the
// remaining keys. Otherwise, it returns the values corresponding to the keys.
func (q *Queue[K, V]) Collect(keys ...K) ([]V, error) {
	var err error
	tasks := q.getTasks(pushAllBack, keys...)
	values := make([]V, len(tasks))
	for i, task := range tasks {
		values[i], err = task.Wait()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// Stats conveys information about the keys and results in a [Queue].
type Stats struct {
	// Handled is the count of keys whose results are computed and cached.
	Handled uint64
	// Total is the count of all pending and handled keys known to the queue.
	Total uint64
}

// Stats returns the [Stats] for a [Queue] as of the time of the call.
func (q *Queue[K, V]) Stats() Stats {
	var stats Stats
	stats.Handled = q.tasksHandled.Load()
	q.tasksMu.Lock()
	stats.Total = uint64(len(q.tasks))
	q.tasksMu.Unlock()
	return stats
}

func (q *Queue[K, V]) getTasks(enqueue enqueueFunc[K], keys ...K) []*task[V] {
	tasks, newKeys := q.getOrCreateTasks(keys)
	q.scheduleNewKeys(enqueue, newKeys)
	return tasks
}

func (q *Queue[K, V]) getOrCreateTasks(keys []K) (tasks []*task[V], newKeys []K) {
	tasks = make([]*task[V], len(keys))

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

	immediateKeys := func() []K {
		q.stateMu.Lock()
		defer q.stateMu.Unlock()

		// Issue as many new work grants as we can.
		newGrants := min(q.state.maxGrants-q.state.grants, len(keys))
		q.state.grants += newGrants

		immediateKeys, queuedKeys := keys[:newGrants], keys[newGrants:]
		enqueue(&q.state.keys, queuedKeys)
		return immediateKeys
	}()

	// Transfer our issued work grants, to discharge our own responsibility and
	// restore the invariant that no goroutine holds more than one.
	for _, key := range immediateKeys {
		go q.work(&key)
	}
}

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
				if task.result.Goexited() {
					go q.work(nil) // We can't stop Goexit, so we must transfer our work grant.
				}
			}()
			task.Handle(func() (V, error) {
				defer q.tasksHandled.Add(1)
				return q.handle(qh, key)
			})
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
	var mustBequeath bool

	func() {
		q.stateMu.Lock()
		defer q.stateMu.Unlock()

		switch {
		case q.state.grants > q.state.maxGrants:
			// We are in violation of a decreased concurrency limit, and must retire
			// the work grant even if work is pending.
			q.state.grants -= 1

		case q.state.reattachers > 0:
			// We can transfer our work grant to a reattacher; see handleReattach for
			// details.
			q.state.reattachers -= 1
			mustBequeath = true

		case q.state.keys.Len() == 0:
			// With no reattachers and no keys, we have no pending work and must
			// retire the work grant.
			q.state.grants -= 1

		default:
			// We have pending work and must use the work grant to execute it.
			key = q.state.keys.PopFront()
			ok = true
		}
	}()

	if mustBequeath {
		q.reattach.BequeathGrant()
	}

	return
}

// handleDetach relinquishes the current goroutine's work grant. Its behavior is
// undefined if its caller does not hold a work grant.
func (q *Queue[K, V]) handleDetach() {
	// `go q.work(nil)` would be a correct implementation of this function that
	// biases toward unblocking the detacher as quickly as possible. But since the
	// typical use for detaching is to block on another resource, we can afford to
	// spend some quality time with the state lock, and perhaps relinquish our
	// work grant directly instead of spawning a new goroutine to transfer it.
	if key, ok := q.tryGetQueuedKey(); ok {
		go q.work(&key)
	}
}

// handleReattach obtains a work grant for the current goroutine, which must be
// prepared to fulfill the work grant invariants.
func (q *Queue[K, V]) handleReattach() {
	mustObtain := func() bool {
		q.stateMu.Lock()
		defer q.stateMu.Unlock()

		if q.state.grants < q.state.maxGrants {
			// There is capacity for a new work grant, so we must issue one.
			q.state.grants += 1
			return false
		}

		// There is no capacity for a new work grant, so we must inform an existing
		// worker that a reattacher is ready to take theirs.
		q.state.reattachers += 1
		return true
	}()

	if mustObtain {
		q.reattach.ObtainGrant()
	}
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
// [Queue] that invoked it, allowing the queue to immediately handle other keys.
// It returns true if this call detached the handler, or false if the handler
// already detached.
func (qh *QueueHandle) Detach() bool {
	if qh.detached {
		return false
	}
	qh.detach()
	qh.detached = true
	return true
}

// Reattach blocks the calling [Handler] until it can execute within the
// concurrency limit of the [Queue] that invoked it, taking priority over
// unhandled queued keys. It has no effect if the handler is already attached.
func (qh *QueueHandle) Reattach() {
	if qh.detached {
		qh.reattach()
		qh.detached = false
	}
}
