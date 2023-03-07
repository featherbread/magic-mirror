package work

import (
	"testing"
	"time"
)

func TestQueueBasic(t *testing.T) {
	q := NewQueue(0, func(x int) (int, error) {
		return x + 2, nil
	})
	task := q.GetOrSubmit(2)

	done := make(chan *Task[int], 1)
	go func() {
		task.Wait()
		done <- task
	}()

	select {
	case task := <-done:
		y, err := task.Wait()
		if err != nil {
			t.Errorf("unexpected error from task: %v", err)
		}
		if y != 4 {
			t.Errorf("unexpected value from task: %v", err)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task")
	}
}
