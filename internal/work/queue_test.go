package work

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueuePanicPropagation(t *testing.T) {
	const want = "the expected panic value"
	q := NewNoValueQueue(1, func(_ *QueueHandle, _ NoValue) error { panic(want) })
	defer func() {
		got := recover()
		assert.Equal(t, want, got)
	}()
	q.Get(NoValue{})
}

func TestQueueGoexitPropagation(t *testing.T) {
	q := NewNoValueQueue(1, func(_ *QueueHandle, _ NoValue) error {
		runtime.Goexit()
		return nil
	})
	// Goexit isn't allowed in tests outside of standard skip and fail functions,
	// so we need to get creative.
	done := make(chan bool)
	go func() {
		defer close(done)
		q.Get(NoValue{})
		done <- true
	}()
	if <-done {
		t.Fatalf("runtime.Goexit did not propagate")
	}
}
