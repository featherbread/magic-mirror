//go:build goexperiment.synctest

package work

import "testing"

func assertEventuallyUnblocks(t *testing.T, done <-chan struct{}) {
	// synctest.Run only returns after all bubbled goroutines exit. If the test
	// finishes, we know that it could not have leaked bubbled goroutines.
	//
	// Of course, if we _do_ leak a goroutine that never durably blocks, the test
	// will get stuck for 10 minutes unless a shorter -timeout is set. That could
	// be annoying.
	//
	// It may also be annoying that this applies to the non-synctest cases
	// whenever the synctest experiment is enabled, and we lose the shorter
	// timeouts where they're actually useful. But the tests will usually run
	// without the experiment enabled, and this hack should be temporary either
	// way.
}
