//go:build !goexperiment.synctest

package work

import "testing"

func cleanup(t *testing.T, fn func()) {
	t.Cleanup(fn)
}
