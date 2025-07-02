package parka

import (
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"

	"github.com/ahamlinman/magic-mirror/internal/parka/catch"
)

// ErrHandlerGoexit is panicked when retrieving a [Map] result for which the
// corresponding handler called [runtime.Goexit].
var ErrHandlerGoexit = errors.New("parka: handler executed runtime.Goexit")

// ErrTaskEjected is the error returned when retrieving a [Map] result for which
// the corresponding handler run was canceled.
var ErrTaskEjected = errors.New("task ejected from queue")

// Map runs a handler function once per key in a distinct goroutine and caches
// the result, while supporting dynamic concurrency limits on handlers.
//
// The result from the handler consists of a value and error. Map does not
// recover panics; they crash the program if not recovered within the handler.
// If a handler calls [runtime.Goexit], retrieving the key's result panics with
// [ErrHandlerGoexit].
//
// New maps permit an effectively unlimited number of goroutines ([math.MaxInt])
// to concurrently handle new keys. [Map.Limit] can change this limit at any
// time. [Handle.Detach] permits an individual handler to exclude itself from
// the concurrency limit for the remainder of its own lifetime, or until it
// calls [Handle.Reattach]. In particular, [KeyMutex] helps handlers detach from
// the limit while awaiting exclusive use of a shared resource, typically one
// identified by a subset of the handler's current key.
type Map[K comparable, V any] struct {
	handle func(*Handle, K) (V, error)

	// tasks tracks all pending and completed work known to the map.
	// Modifications to the task set must maintain the invariant that every
	// incomplete key can become known to a worker that can run its handler.
	tasks        map[K]*task[V]
	tasksMu      sync.Mutex // 1st in locking order.
	tasksHandled atomic.Uint64

	// See [workState].
	state    workState[K]
	stateMu  sync.Mutex // 2nd in locking order.
	reattach reattachQueue
}

// workState tracks a map's pending work, along with the "work grants" that
// control the concurrency of handler execution.
//
// A work grant is an abstract resource that both permits and obligates its
// holder to execute a map's pending work. Work grants are issued (incrementing
// grants), retired (decrementing grants), and transferred between holders to
// maintain the following invariants:
//
//   - Exactly one work grant is outstanding for every unhandled key known to
//     the map, up to grantLimit.
//   - No goroutine holds more than one work grant.
//   - Any work grant holder is either executing the handler for an unhandled
//     key or maintaining these invariants.
//
// Detaching handlers relinquish their work grant to continue execution outside
// of the concurrency limit. Reattaching handlers that wish to re-obtain their
// work grant should receive priority over the handling of new keys.
type workState[K comparable] struct {
	grants      int
	grantLimit  int
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

func (rq reattachQueue) SendGrant()    { <-rq }
func (rq reattachQueue) ReceiveGrant() { rq <- struct{}{} }

// task represents a unit of pending or handled work for a single key.
type task[V any] struct {
	wg     sync.WaitGroup
	result catch.Result[V]
}

func (t *task[V]) Wait() (V, error) {
	t.wg.Wait()
	if !t.result.Returned() {
		panic(ErrHandlerGoexit) // Must be Goexit, since wg isn't done when the handler panics.
	}
	return t.result.Unwrap()
}

// NewMap creates a [Map] with the provided handler.
func NewMap[K comparable, V any](handle func(*Handle, K) (V, error)) *Map[K, V] {
	return &Map[K, V]{
		handle:   handle,
		state:    workState[K]{grantLimit: math.MaxInt},
		tasks:    make(map[K]*task[V]),
		reattach: make(reattachQueue),
	}
}

// Inform advises the map of keys that it should ensure are handled and cached
// as soon as possible.
//
// Inform has no effect on keys already handled or pending. When concurrency
// limits prohibit immediate handling of all keys, the new keys among those
// provided are enqueued at the back of a work queue, in the order given,
// without interleaving the keys of any other enqueue operation. A future
// [Map.InformFront] call may interpose new keys between those enqueued in a
// single Inform call.
func (m *Map[K, V]) Inform(keys ...K) {
	m.getTasks(pushAllBack, keys...)
}

// InformFront behaves like [Map.Inform], but enqueues new keys at the front of
// the map's work queue rather than the back. Like Inform, it does not affect
// the order of keys already pending.
func (m *Map[K, V]) InformFront(keys ...K) {
	m.getTasks(pushAllFront, keys...)
}

// Get informs the map of the key as if by [Map.Inform], blocks until it has
// handled the key, then returns the key's result.
//
// If the key's handler called [runtime.Goexit], Get panics with [ErrHandlerGoexit].
func (m *Map[K, V]) Get(key K) (V, error) {
	return m.getTasks(pushAllBack, key)[0].Wait()
}

// Collect informs the map of the keys as if by [Map.Inform], then coalesces
// their results. If any key's handler returns an error or calls [runtime.Goexit],
// Collect returns that error or panics with [ErrHandlerGoexit] without waiting
// for the map to handle subsequent keys. Otherwise, Collect returns a slice of
// values corresponding to the keys.
func (m *Map[K, V]) Collect(keys ...K) ([]V, error) {
	var err error
	tasks := m.getTasks(pushAllBack, keys...)
	values := make([]V, len(tasks))
	for i, task := range tasks {
		values[i], err = task.Wait()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// Limit updates the map's concurrency limit for handling pending keys,
// guaranteeing a limit of at least 1 regardless of the limit provided.
//
// Limit may be called at any time, even while handlers are running.
// The map immediately spawns handlers for as many pending keys as an
// increased limit allows, or permits in-flight handlers in violation
// of a decreased limit to finish in the background.
func (m *Map[K, V]) Limit(limit int) {
	var (
		keys      []K
		transfers int
	)
	func() {
		m.stateMu.Lock()
		defer m.stateMu.Unlock()

		// Update the limit, and determine how many new work grants we can issue.
		m.state.grantLimit = max(1, limit)
		issuable := max(0, m.state.grantLimit-m.state.grants)

		// Issue as many work grants as possible to reattachers.
		transfers = min(issuable, m.state.reattachers)
		m.state.grants += transfers
		m.state.reattachers -= transfers
		issuable -= transfers

		// Issue as many work grants as possible for handling keys.
		workable := min(issuable, m.state.keys.Len())
		m.state.grants += workable
		keys = make([]K, workable)
		for i := range keys {
			keys[i] = m.state.keys.PopFront()
		}
	}()

	// Start by transferring the work grants issued for new keys, since this won't
	// require any blocking.
	for _, key := range keys {
		go m.work(&key)
	}

	// Transfer the work grants issued for reattachers, which may block but only
	// for a short time.
	for range transfers {
		m.reattach.SendGrant()
	}
}

// Stats conveys information about the keys and results in a [Map].
type Stats struct {
	// Handled is the count of keys whose results are computed and cached.
	Handled uint64
	// Total is the count of all pending and handled keys in the map.
	Total uint64
}

// Stats returns the [Stats] for a [Map] as of the time of the call.
func (m *Map[K, V]) Stats() Stats {
	var stats Stats
	stats.Handled = m.tasksHandled.Load()
	m.tasksMu.Lock()
	stats.Total = uint64(len(m.tasks))
	m.tasksMu.Unlock()
	return stats
}

// DequeueAll removes and returns any queued keys that have not been picked up
// for handling. Any retrieval of the keys' corresponding results started before
// DequeueAll returns the zero value of V and [ErrTaskEjected]. Any retrieval
// started after DequeueAll re-informs the map of the key. DequeueAll has no
// effect on cached results or handlers in flight.
func (m *Map[K, V]) DequeueAll() []K {
	var (
		keys  []K
		tasks []*task[V]
	)

	func() {
		// The tasks invariant precludes emptying the queue and removing the tasks
		// in separate critical sections, as the incomplete keys in the map
		// can no longer "become known to a worker" once we empty the queue.
		m.tasksMu.Lock()
		defer m.tasksMu.Unlock()
		m.stateMu.Lock()
		defer m.stateMu.Unlock()

		var swapped deque.Deque[K]
		swapped, m.state.keys = m.state.keys, swapped

		keys = make([]K, swapped.Len())
		tasks = make([]*task[V], swapped.Len())
		for i := range swapped.Len() {
			keys[i] = swapped.At(i)
			tasks[i] = m.tasks[keys[i]]
			delete(m.tasks, keys[i])
		}
	}()

	for _, task := range tasks {
		task.result = catch.Return(*new(V), ErrTaskEjected)
		task.wg.Done()
	}
	return keys
}

func (m *Map[K, V]) getTasks(enqueue enqueueFunc[K], keys ...K) []*task[V] {
	tasks, newKeys := m.getOrCreateTasks(keys)
	m.scheduleNewKeys(enqueue, newKeys)
	return tasks
}

func (m *Map[K, V]) getOrCreateTasks(keys []K) (tasks []*task[V], newKeys []K) {
	tasks = make([]*task[V], len(keys))

	m.tasksMu.Lock()
	defer m.tasksMu.Unlock()

	for i, key := range keys {
		if task, ok := m.tasks[key]; ok {
			tasks[i] = task
			continue
		}
		task := &task[V]{}
		task.wg.Add(1)
		m.tasks[key] = task
		tasks[i] = task
		newKeys = append(newKeys, key)
	}
	return
}

func (m *Map[K, V]) scheduleNewKeys(enqueue enqueueFunc[K], keys []K) {
	if len(keys) == 0 {
		return // No need to lock up the state.
	}

	var immediateKeys []K

	func() {
		m.stateMu.Lock()
		defer m.stateMu.Unlock()

		// Issue as many new work grants as we can.
		newGrants := max(0, min(m.state.grantLimit-m.state.grants, len(keys)))
		m.state.grants += newGrants

		var queuedKeys []K
		immediateKeys, queuedKeys = keys[:newGrants], keys[newGrants:]
		enqueue(&m.state.keys, queuedKeys)
	}()

	// Transfer our issued work grants, to discharge our own responsibility and
	// restore the invariant that no goroutine holds more than one.
	for _, key := range immediateKeys {
		go m.work(&key)
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
func (m *Map[K, V]) work(initialKey *K) {
	// Precondition: we hold a single work grant.
	for {
		var (
			key K
			ok  bool
		)
		if initialKey != nil {
			key, initialKey = *initialKey, nil
		} else if key, ok = m.tryGetQueuedKey(); !ok {
			return // We no longer have a work grant; see tryGetQueuedKey.
		}

		m.tasksMu.Lock()
		task := m.tasks[key]
		m.tasksMu.Unlock()

		detached := m.completeTask(key, task)
		if detached {
			return // We no longer have a work grant.
		}
	}
}

// workPanic supports mocking panic() in unit tests.
var workPanic = func(v any) { panic(v) }

// completeTask, when called with a work grant held, executes the handler for
// key, which may relinquish the work grant if it detaches.
func (m *Map[K, V]) completeTask(key K, task *task[V]) (detached bool) {
	h := &Handle{ // Loan our work grant to the handler.
		detach:   m.handleDetach,
		reattach: m.handleReattach,
	}

	defer func() {
		detached = h.terminate() // Try to take our work grant back.
		if rv := recover(); rv != nil {
			// If we're panicking, the program will soon crash.
			// There's no point in letting a waiter see our worthless non-result,
			// or in transferring any work grant we have.
			workPanic(rv)
		}
		if !detached && !task.result.Returned() {
			// We have a work grant and are (likely) Goexiting, so must transfer it.
			// This could also be a panic(nil) if GODEBUG=panicnil=1, but the only
			// harm is one extra goroutine stack in the forthcoming crash dump.
			go m.work(nil)
		}
		m.tasksHandled.Add(1)
		task.wg.Done()
	}()

	task.result = catch.Goexit[V]()
	task.result = catch.Return(m.handle(h, key)) // May unwind before assignment.
	return
}

// tryGetQueuedKey, when called with a work grant held, either relinquishes the
// work grant (returning ok == false) or returns a key (ok == true) whose work
// the caller must execute.
func (m *Map[K, V]) tryGetQueuedKey() (key K, ok bool) {
	var mustSendGrant bool

	func() {
		m.stateMu.Lock()
		defer m.stateMu.Unlock()

		switch {
		case m.state.grants > m.state.grantLimit:
			// We are in violation of a decreased concurrency limit, and must retire
			// the work grant even if work is pending.
			m.state.grants -= 1

		case m.state.reattachers > 0:
			// We can transfer our work grant to a reattacher; see handleReattach for
			// details.
			m.state.reattachers -= 1
			mustSendGrant = true

		case m.state.keys.Len() == 0:
			// With no reattachers and no keys, we have no pending work and must
			// retire the work grant.
			m.state.grants -= 1

		default:
			// We have pending work and must use the work grant to execute it.
			key = m.state.keys.PopFront()
			ok = true
		}
	}()

	if mustSendGrant {
		m.reattach.SendGrant()
	}
	return
}

// handleDetach relinquishes a work grant held by its caller.
func (m *Map[K, V]) handleDetach() {
	// `go q.work(nil)` would be a correct implementation of this function that
	// biases toward unblocking the detacher as quickly as possible. But since the
	// typical use for detaching is to block on another resource, we can afford to
	// spend some quality time with the state lock, and perhaps relinquish our
	// work grant directly instead of spawning a new goroutine to transfer it.
	if key, ok := m.tryGetQueuedKey(); ok {
		go m.work(&key)
	}
}

// handleReattach obtains a work grant to replace one previously relinquished by
// [Map.handleDetach].
func (m *Map[K, V]) handleReattach() {
	var mustReceiveGrant bool

	func() {
		m.stateMu.Lock()
		defer m.stateMu.Unlock()

		if m.state.grants < m.state.grantLimit {
			// There is capacity for a new work grant, so we must issue one.
			m.state.grants += 1
			return
		}

		// There is no capacity for a new work grant, so we must inform an existing
		// worker that a reattacher is ready to take theirs.
		m.state.reattachers += 1
		mustReceiveGrant = true
	}()

	if mustReceiveGrant {
		m.reattach.ReceiveGrant()
	}
}

// Handle allows a handler to interact with its parent [Map].
//
// It is permitted to call Handle methods in goroutines separate from the
// handler's own. In the terminology of the Go memory model, the return of every
// Handle call must be synchronized before the handler's termination.
type Handle struct {
	detach   func()
	reattach func()

	// [Map.work] loans its goroutine's work grant to the map's handler.
	// Then, the handler may relinquish or reobtain it in a controlled manner.
	// Finally, the worker takes its work grant back if the Handle still holds it.
	//
	// mu protects the invariant that this Handle contains a work grant iff
	// !detached && !terminated. detached marks the temporary loss of a work grant
	// through the handler's actions, while terminated marks its permanent loss.
	mu         sync.Mutex
	detached   bool
	terminated bool
}

// Detach unbounds the calling handler from the concurrency limit of the [Map]
// that invoked it, allowing the map to immediately handle other keys.
// It returns true if this call detached the handler, or false if the handler
// already detached.
func (h *Handle) Detach() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.terminated {
		panic("parka: attempted Detach outside handler lifetime")
	}
	if !h.detached {
		h.detach()
		h.detached = true
		return true
	}
	return false
}

// Reattach blocks the calling handler until it can execute within the
// concurrency limit of the [Map] that invoked it, taking priority over
// unhandled queued keys. It has no effect if the handler is already attached.
func (h *Handle) Reattach() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.terminated {
		panic("parka: attempted Reattach outside handler lifetime")
	}
	if h.detached {
		h.reattach()
		h.detached = false
	}
}

// terminate extracts the work grant, if any, currently held by h. The goroutine
// that loaned h its work grant is trusted to call terminate exactly once and
// rely on that single return value.
func (h *Handle) terminate() (wasDetached bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.terminated = true
	return h.detached
}
