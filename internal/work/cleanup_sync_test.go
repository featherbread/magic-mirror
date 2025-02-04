//go:build goexperiment.synctest

package work

import "testing"

func cleanup(t *testing.T, fn func()) {
	// TODO: Reimplement this? We only use it to check for leaked goroutines,
	// which isn't an issue under synctest since Run won't exit until all
	// goroutines are done.
}
