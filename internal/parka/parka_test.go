package parka_test

import "runtime"

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
	exitRuntimeGoexit
	_exitBehaviorCount
)

func (eb exitBehavior) Do() error {
	if eb == exitRuntimeGoexit {
		runtime.Goexit()
	}
	return nil
}

func (eb exitBehavior) String() string {
	switch eb {
	case exitNilReturn:
		return "return nil"
	case exitRuntimeGoexit:
		return `runtime.Goexit()`
	}
	panic("unknown exitBehavior")
}
