//go:build !goexperiment.synctest

package work

import (
	"testing"
	"time"
)

func assertEventuallyUnblocks(t *testing.T, done <-chan struct{}) {
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(timeout):
			panic("leaked a blocked goroutine from this test")
		}
	})
}
