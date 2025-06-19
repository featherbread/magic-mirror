//go:debug panicnil=1
package pen_test

import (
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/work/pen"
)

// someNilValue is intentionally never set to anything other than nil.
// Using it to test nil panics silences analyzers like gopls.
var someNilValue any

func TestZero(t *testing.T) {
	var r pen.Result[int]

	assert.True(t, r.Goexited())
	assert.False(t, r.Panicked())
	// Get has more specific tests below.
}

func TestNormalReturn(t *testing.T) {
	r := pen.Do(func() (int, error) { return 42, errors.New("silly goose") })

	assert.False(t, r.Goexited())
	assert.False(t, r.Panicked())

	v, err := r.Unwrap()
	assert.Equal(t, 42, v)
	assert.ErrorContains(t, err, "silly goose")
}

func TestPanic(t *testing.T) {
	r := pen.Do(func() (int, error) { panic("silly panda") })

	assert.False(t, r.Goexited())
	assert.True(t, r.Panicked())
	assert.Equal(t, "silly panda", r.Recovered())

	defer func() {
		rv := recover()
		assert.Equal(t, "silly panda", rv)
	}()
	r.Unwrap()
	t.Error("continued after Result.Get should have panicked")
}

func TestPanicNil(t *testing.T) {
	r := pen.Do(func() (int, error) { panic(someNilValue) })

	assert.False(t, r.Goexited())
	assert.True(t, r.Panicked())
	assert.Nil(t, r.Recovered())

	defer func() {
		rv := recover()
		assert.Nil(t, rv)
	}()
	r.Unwrap()
	t.Error("continued after Result.Get should have panicked")
}

func TestGoexit(t *testing.T) {
	r := pen.Do(func() (int, error) { runtime.Goexit(); return 0, nil })

	assert.True(t, r.Goexited())
	assert.False(t, r.Panicked())

	// Go's test framework doesn't allow tests to Goexit without failing.
	var wg sync.WaitGroup
	wg.Go(func() {
		r.Unwrap()
		t.Error("continued after Result.Get should have Goexited")
	})
	wg.Wait()
}
