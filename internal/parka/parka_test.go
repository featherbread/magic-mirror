package parka_test

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/parka/catch"
)

func makeIntKeys(n int) []int {
	keys := make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	return keys
}

func maxOfChannel(ch <-chan int) int {
	var maxI int
	for i := range ch {
		maxI = max(i, maxI)
	}
	return maxI
}

func promise(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	return done
}

// exitBehavior represents a way for a function to terminate.
type exitBehavior int

const (
	exitNilReturn exitBehavior = iota
	exitValuePanic
	exitNilPanic
	exitRuntimeGoexit
	_exitBehaviorCount
)

func (eb exitBehavior) Do() error {
	switch eb {
	case exitValuePanic:
		panic("exitBehavior")
	case exitNilPanic:
		panic(someNilValue)
	case exitRuntimeGoexit:
		runtime.Goexit()
	}
	return nil // Any other fallback would be too confusing.
}

func (eb exitBehavior) String() string {
	switch eb {
	case exitNilReturn:
		return "return nil"
	case exitValuePanic:
		return `panic("exitBehavior")`
	case exitNilPanic:
		return `panic(nil)`
	case exitRuntimeGoexit:
		return `runtime.Goexit()`
	}
	panic("unknown exitBehavior")
}

func assertExitBehavior(t assert.TestingT, eb exitBehavior, fn func() error) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}

	result := catch.Do(func() (any, error) { return nil, fn() })

	switch eb {
	case exitNilReturn:
		if assert.True(t, result.Returned(), "Result is not Returned()") {
			_, err := result.Unwrap()
			assert.NoError(t, err, "Result contained a non-nil error")
		}

	case exitValuePanic:
		assert.True(t, result.Panicked(), "Result is not Panicked()")
		assert.Equal(t, "exitBehavior", result.Recovered())

	case exitNilPanic:
		assert.True(t, result.Panicked(), "Result is not Panicked()")
		assert.Nil(t, result.Recovered())

	case exitRuntimeGoexit:
		assert.True(t, result.Goexited(), "Result is not Goexited()")

	default:
		assert.FailNow(t, "Unrecognized exit behavior: %d", eb)
	}
}

func TestExitBehaviorConsistency(t *testing.T) {
	for exit := range _exitBehaviorCount {
		t.Run(exit.String(), func(t *testing.T) {
			// Ensure the assertions match up with the real behavior of this exit.
			assertExitBehavior(t, exit, exit.Do)

			// Ensure the _other_ assertions do _not_ match up.
			for wrongExit := range _exitBehaviorCount {
				if wrongExit == exit {
					continue
				}
				var fakeT testFailed
				assertExitBehavior(&fakeT, wrongExit, exit.Do)
				assert.True(t, fakeT.Load(), "Asserts for %s also allow %s", exit, wrongExit)
			}
		})
	}
}

// testFailed implements [assert.TestingT], but merely indicates whether Errorf
// was called at any point.
type testFailed struct{ atomic.Bool }

func (t *testFailed) Errorf(_ string, _ ...any) { t.Store(true) }
