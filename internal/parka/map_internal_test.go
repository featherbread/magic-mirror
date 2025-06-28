package parka

import (
	"runtime"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/parka/catch"
)

// TestWorkPanic asserts that our mockable panic() wrapper really does panic.
func TestWorkPanic(t *testing.T) {
	result := catch.Do(func() (any, error) {
		workPanic("TestWorkPanic")
		return nil, nil
	})
	assert.True(t, result.Panicked(), "Result is not Panicked()")
	assert.Equal(t, result.Recovered(), "TestWorkPanic")
}

// TestHandlerPanic asserts that [Map] does not permit any result to be returned
// when a handler panics, and that the panic is permitted to travel up the stack
// and terminate the program. This requires special access to Map internals:
// (1) to mock panic, and (2) to complete the intentionally stuck task.
func TestHandlerPanic(t *testing.T) {
	var panicval any
	oldWorkPanic := workPanic
	workPanic = func(v any) { panicval = v; runtime.Goexit() }
	defer func() { workPanic = oldWorkPanic }()

	synctest.Test(t, func(t *testing.T) {
		s := NewSet(func(_ *Handle, _ struct{}) error {
			panic("TestHandlerPanic")
		})

		done := make(chan struct{})
		go func() {
			defer func() {
				recover() // Our panic() mock Goexits, which makes Get panic.
				close(done)
			}()
			s.Get(struct{}{})
		}()
		synctest.Wait()

		select {
		case <-done:
			assert.Fail(t, "Computation from panicked handler was not blocked")
		default:
			assert.Equal(t, "TestHandlerPanic", panicval, "Panic not forced to propagate")
			assert.Equal(t, Stats{Handled: 0, Total: 1}, s.Stats())
		}

		for _, task := range s.tasks {
			task.wg.Done()
		}
	})
}
