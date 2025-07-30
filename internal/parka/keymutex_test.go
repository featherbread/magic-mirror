package parka_test

import (
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/featherbread/magic-mirror/internal/parka"
	"github.com/featherbread/magic-mirror/internal/parka/catch"
)

func TestKeyMutexZeroUnlock(t *testing.T) {
	var km parka.KeyMutex[int]
	result := catch.Do(func() (int, error) { km.Unlock(0); return 0, nil })
	assert.True(t, result.Panicked())
	assert.Contains(t, result.Recovered(), "key is already unlocked")
}

func TestKeyMutextDoubleUnlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var km parka.KeyMutex[int]
		km.Lock(0)
		km.Lock(1)
		km.Unlock(1)
		result := catch.Do(func() (int, error) { km.Unlock(1); return 0, nil })
		assert.True(t, result.Panicked())
		assert.Contains(t, result.Recovered(), "key is already unlocked")
	})
}

func TestKeyMutexBasic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			keyCount    = 3
			workerCount = 2 * keyCount
		)
		var (
			km      parka.KeyMutex[int]
			locked  [keyCount]atomic.Int32
			unblock = make(chan struct{})
		)
		for i := range workerCount {
			key := i / 2
			go func() {
				km.Lock(key)
				defer km.Unlock(key)

				locked[key].Add(1)
				defer locked[key].Add(-1)

				<-unblock
			}()
		}

		// Wait for every goroutine to be durably blocked, then check for limit
		// breaches.
		synctest.Wait()
		for i := range locked {
			count := int(locked[i].Load())
			assert.Equal(t, 1, count, "At index %d", i)
		}

		// Let all of the workers finish.
		close(unblock)
	})
}

func TestKeyMutexDetachReattach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			km       parka.KeyMutex[struct{}]
			unblock0 = make(chan struct{})
		)
		s := parka.NewSet(func(ph *parka.Handle, x int) error {
			if x == 0 {
				km.LockDetached(ph, struct{}{})
				<-unblock0
				km.Unlock(struct{}{})
			}
			return nil
		})
		s.Limit(1)

		// Take the lock.
		km.Lock(struct{}{})

		// Start the handler for 0, which must detach since we're holding the lock.
		s.Inform(0)
		synctest.Wait()

		// Ensure that unrelated handlers can, in fact, proceed.
		s.Get(1)

		// Release the lock so handler 0 can obtain it.
		km.Unlock(struct{}{})
		synctest.Wait()

		// Start another handler, and ensure it really is blocked.
		done := promise(func() { s.Get(2) })
		synctest.Wait()
		select {
		case <-done:
			assert.Fail(t, "Computation of key was not blocked")
		default:
		}

		// Allow all of the handlers to finish.
		close(unblock0)
		s.Collect(0, 1, 2)
	})
}
