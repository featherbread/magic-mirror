package work

import (
	"fmt"
	"runtime"
	"testing"
)

func TestQueueGetAllError(t *testing.T) {
	const submitCount = 10
	q := NewNoValueQueue(submitCount, func(_ *QueueHandle, x int) error {
		if x >= 5 {
			return fmt.Errorf("%d", x)
		}
		return nil
	})

	keys := makeIntKeys(submitCount)
	_, err := q.GetAll(keys...)
	if err == nil || err.Error() != "5" {
		t.Errorf("GetAll() did not return expected error: got %v, want %q", err, "5")
	}

	for x := range submitCount {
		q.Get(x) // Wait for all handlers to finish.
	}
}

func TestQueuePanic(t *testing.T) {
	want := "the expected panic value"
	q := NewNoValueQueue(1, func(_ *QueueHandle, _ NoValue) error { panic(want) })
	defer func() {
		if got := recover(); got != want {
			t.Errorf("unexpected panic: got %v, want %v", got, want)
		}
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
