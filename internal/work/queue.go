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

// Queue is a concurrency-limited deduplicating work queue. It acts like a map
// that computes and caches the value for each requested key while limiting the
// number of computations in flight.
//
// Each queue makes one or more concurrent calls to its [Handler] in new
// goroutines to compute a result for each requested key. It handles each key
// once regardless of the number of concurrent requests for that key, and caches
// and returns a single result for all requests.
//
// The result for each key nominally consists of a value and an error, but may
// instead capture a panic or a call to [runtime.Goexit]. In the latter cases,
// the queue's Get* methods propagate the panic or Goexit to their caller.
//
// # Urgent Variants
//
// The Urgent variants of the queue's Get* methods push unhandled keys to the
// front of the work queue rather than the back.
//
// The behavior of the Urgent variants is equivalent to that of the standard
// variants in any of the following cases:
//
//   - The results for all requested keys are already cached.
//   - Ample concurrency is available to handle the requested keys immediately.
//   - The requested keys were previously queued by a non-Urgent call. Urgent
//     calls cannot "promote" these earlier non-Urgent calls.
//
// # All Variants
//
// The All variants of the queue's Get* methods coalesce the results for
// multiple keys into a single call. If every result consists of a value and
// non-nil error, the method returns a slice of values corresponding to the
// requested keys. Otherwise, it returns the first error or propagates the first
// panic or Goexit with respect to the order of the keys, without waiting for
// subsequent handlers to finish. To associate a result with a specific key or
// wait for all handlers, use the non-All variants instead.
//
// When the queue's concurrency limit requires some keys to be queued for later
// handling, the All variants enqueue the unhandled keys in the order given,
// without interleaving keys from any other Get* call. However, a future Urgent
// call may interpose its unhandled key(s) between those enqueued by an earlier
// All call.
//
// # Concurrency Limits and Detaching
//
// Each queue is initialized with a limit on the number of goroutines that will
// concurrently handle new keys. However, [QueueHandle.Detach] permits a handler
// to increase the queue's effective concurrency limit for as long as it runs,
// or until it calls [QueueHandle.Reattach]. See [QueueHandle] for details.
type Queue[K comparable, V any] struct {
	handle Handler[K, V]

	state    workState[K]
	stateMu  sync.Mutex
	reattach chan struct{}

	tasks     map[K]*task[V]
	tasksMu   sync.Mutex
	tasksDone atomic.Uint64
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
	keys        *deque.Deque[K]
}

// NewQueue creates a [Queue] with the provided concurrency limit and handler.
//
// If concurrency <= 0, the queue is created with an effectively unlimited
// concurrency of [math.MaxInt].
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

// NewNoValueQueue wraps a handler that only returns an error to create a
// [Queue] that produces [NoValue] for each key.
func NewNoValueQueue[K comparable](concurrency int, handle func(*QueueHandle, K) error) *Queue[K, NoValue] {
	return NewQueue(concurrency, func(qh *QueueHandle, key K) (_ NoValue, err error) {
		err = handle(qh, key)
		return
	})
}

// Get returns the result or propagates the panic or Goexit for the provided key
// as described in the [Queue] documentation, blocking if necessary until the
// corresponding call to the queue's handler finishes.
func (q *Queue[K, V]) Get(key K) (V, error) {
	return q.getTasks(pushAllBack, key)[0].Wait()
}

// GetUrgent behaves like [Queue.Get], but pushes new keys to the front of the
// queue as described in the "Urgent Variants" section of the [Queue]
// documentation.
func (q *Queue[K, V]) GetUrgent(key K) (V, error) {
	return q.getTasks(pushAllFront, key)[0].Wait()
}

// GetAll coalesces the results for multiple keys as described in the "All
// Variants" section of the [Queue] documentation.
func (q *Queue[K, V]) GetAll(keys ...K) ([]V, error) {
	return q.getTasks(pushAllBack, keys...).Wait()
}

// GetAllUrgent behaves like [Queue.GetAll], but pushes new keys to the front of
// the queue as described in the "Urgent Variants" section of the [Queue]
// documentation.
func (q *Queue[K, V]) GetAllUrgent(keys ...K) ([]V, error) {
	return q.getTasks(pushAllFront, keys...).Wait()
}

// Stats conveys information about the keys and results in a [Queue].
type Stats struct {
	// Done is the number of handled keys, whose results are computed and cached.
	Done uint64
	// Submitted is the number of keys whose results have been requested,
	// including unhandled keys.
	Submitted uint64
}

// Stats returns the [Stats] for a [Queue] as of the time of the call.
func (q *Queue[K, V]) Stats() Stats {
	var stats Stats
	stats.Done = q.tasksDone.Load()
	q.tasksMu.Lock()
	stats.Submitted = uint64(len(q.tasks))
	q.tasksMu.Unlock()
	return stats
}

func (q *Queue[K, V]) getTasks(enqueue enqueueFunc[K], keys ...K) taskList[V] {
	tasks, newKeys := q.getOrCreateTasks(keys)
	q.scheduleNewKeys(enqueue, newKeys)
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

	// Issue work grants up to the concurrency limit, then transfer them to new
	// goroutines to discharge our own responsibility for them and restore the
	// invariant that no goroutine holds more than one.
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
				if task.goexit {
					go q.work(nil) // We can't stop Goexit, so we must transfer our work grant.
				}
			}()
			task.Do(func() (V, error) {
				defer q.tasksDone.Add(1)
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

// handleDetach relinquishes the current goroutine's work grant. Its behavior is
// undefined if its caller does not hold a work grant.
func (q *Queue[K, V]) handleDetach() {
	if q.stateMu.TryLock() {
		// If we can quickly lock the state, try to relinquish the work grant
		// without starting a new worker.
		key, ok := q.tryGetQueuedKeyLocked()
		if ok {
			go q.work(&key)
		}
	} else {
		// Otherwise, transfer the work grant so we don't block the detach.
		go q.work(nil)
	}
}

// handleReattach obtains a work grant for the current goroutine, which must be
// prepared to fulfill the work grant invariants.
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
// [Queue] that invoked it, allowing the queue to immediately handle other keys.
// It returns true if this call detached the handler, or false if the handler
// already detached.
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
