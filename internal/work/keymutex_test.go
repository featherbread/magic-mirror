package work

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyMutexBasic(t *testing.T) {
	const (
		nKeys    = 3
		nWorkers = nKeys * 2
	)

	// TODO: This is weird.
	taskCtx := &taskContext{
		detach:   func() {},
		reattach: func() {},
	}

	var (
		km      KeyMutex[int]
		ready   = make(chan struct{})
		locked  [nKeys]atomic.Int32
		unblock = make(chan struct{})
		done    = make(chan struct{}, nWorkers)
	)
	for i := 0; i < nWorkers; i++ {
		key := i / 2
		go func() {
			defer func() { done <- struct{}{} }()
			<-ready

			km.LockDetached(taskCtx, key)
			defer km.Unlock(key)

			locked[key].Add(1)
			defer locked[key].Add(-1)
			<-unblock
		}()
	}

	for i := 0; i < nWorkers; i++ {
		ready <- struct{}{}
	}
	forceRuntimeProgress(nWorkers)
	for i := range locked {
		if count := locked[i].Load(); count > 1 {
			t.Errorf("mutex for %d held %d times", i, count)
		}
	}

	close(unblock)
	timeout := time.After(2 * time.Second)
	for i := 0; i < nWorkers; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatalf("timed out waiting for test goroutines to finish")
		}
	}
}

func TestKeyMutexDetach(t *testing.T) {
	const submitCount = 5

	var (
		km      KeyMutex[NoValue]
		started = make(chan struct{}, submitCount)
		locked  atomic.Bool
	)
	q := NewQueue(1, func(q QueueHandle, x int) (int, error) {
		started <- struct{}{}

		km.LockDetached(q, NoValue{})
		defer km.Unlock(NoValue{})

		if !locked.CompareAndSwap(false, true) {
			t.Errorf("more than one queue handler holding the lock")
		}
		defer locked.Store(false)
		return x, nil
	})

	// TODO: This is weird.
	taskCtx := &taskContext{
		detach:   func() {},
		reattach: func() {},
	}
	km.LockDetached(taskCtx, NoValue{})

	keys := makeIntKeys(submitCount)
	go func() { q.GetAll(keys...) }()
	timeout := time.After(2 * time.Second)
	for i := range keys {
		select {
		case <-started:
		case <-timeout:
			t.Fatalf("timed out waiting for tasks to detach: %d of %d running", i, len(keys))
		}
	}

	km.Unlock(NoValue{})
	assertSucceedsWithin(t, 2*time.Second, q, keys, keys)
}
