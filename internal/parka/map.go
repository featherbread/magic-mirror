package parka

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"

	"github.com/ahamlinman/magic-mirror/internal/parka/catch"
)

// Map runs a handler function once per key in a distinct goroutine and caches
// the result, while supporting dynamic concurrency limits on handlers.
//
// The result for each key nominally consists of a value and error, but may
// instead capture a panic or a call to [runtime.Goexit], which propagates to
// any caller retrieving that key's result.
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

	state    workState[K]
	stateMu  sync.Mutex
	reattach reattachQueue

	tasks        map[K]*task[V]
	tasksMu      sync.Mutex
	tasksHandled atomic.Uint64
}

// workState tracks a map's pending work, along with the "work grants" that
// control the concurrency of goroutines handling it.
//
// A work grant is an abstract resource that both permits and obligates the
// goroutine holding it to execute the map's handler for any pending keys.
// Work grants are issued (by incrementing grants), retired (by decrementing
// grants), and transferred between goroutines to maintain the following
// invariants:
//
//   - Exactly one work grant is outstanding for every unhandled key known to
//     the map, up to grantLimit.
//   - No goroutine holds more than one work grant.
//   - Any goroutine holding a work grant is either executing the handler for an
//     unhandled key or maintaining these invariants.
//
// Detaching handlers relinquish their goroutine's work grant to continue
// execution outside of the concurrency limit. Reattaching handlers that wish to
// re-obtain their work grant should receive priority over the handling of new
// keys.
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

func (t *task[V]) Handle(fn func() (V, error)) {
	defer t.wg.Done()
	t.result = catch.Goexit[V]()
	t.result = catch.DoOrExit(fn)
}

func (t *task[V]) Wait() (V, error) {
	t.wg.Wait()
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
// handled the key, then propagates the key's result: returning its value and
// error, or forwarding a panic or [runtime.Goexit].
func (m *Map[K, V]) Get(key K) (V, error) {
	return m.getTasks(pushAllBack, key)[0].Wait()
}

// Collect informs the map of the keys as if by [Map.Inform], then coalesces
// their results. If any key's handler returns an error, panics, or calls
// [runtime.Goexit], Collect propagates the first of those results with respect
// to the order of the keys, without waiting for the map to handle the remaining
// keys. Otherwise, it returns the values corresponding to the keys.
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

		var (
			h = &Handle{
				detach:   m.handleDetach,
				reattach: m.handleReattach,
			}
			detached bool
		)
		func() {
			defer func() {
				if task.result.Goexited() && !detached {
					go m.work(nil) // We have a work grant and can't stop Goexit, so must transfer.
				}
			}()
			task.Handle(func() (V, error) {
				defer func() {
					detached = h.terminate()
					m.tasksHandled.Add(1)
				}()
				return m.handle(h, key)
			})
		}()
		if detached {
			return // We no longer have a work grant.
		}
	}
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

// handleDetach relinquishes the current goroutine's work grant. Its behavior is
// undefined if its caller does not hold a work grant.
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

// handleReattach obtains a work grant for the current goroutine, which must be
// prepared to fulfill the work grant invariants.
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
type Handle struct {
	detach   func()
	reattach func()

	// mu protects the invariant that this Handle contains a work grant iff
	// !detached && !terminated.
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
