// Package pen confines the effects of panics and [runtime.Goexit] calls.
package pen

import (
	"runtime"
	"sync"
)

// Do runs fn in an independent goroutine and captures its exit behavior,
// isolating the caller from any panic or [runtime.Goexit].
func Do[T any](fn func() (T, error)) (r Result[T]) {
	r = Goexit[T]()
	var wg sync.WaitGroup
	wg.Go(func() { r = DoOrExit(fn) })
	wg.Wait()
	return
}

// DoOrExit runs fn in the current goroutine and captures a return or panic.
// Unlike [Do], it propagates [runtime.Goexit] from fn rather than returning.
func DoOrExit[T any](fn func() (T, error)) (r Result[T]) {
	r.started = true
	func() {
		defer func() { r.panicval = recover() }()
		r.value, r.err = fn()
		r.returned = true
	}()
	r.recovered = true
	return
}

// Goexit constructs a synthetic result that captures [runtime.Goexit].
func Goexit[T any]() Result[T] {
	return Result[T]{started: true}
}

// Result captures the exit behavior of an isolated function. The zero value
// behaves as if capturing the return of a zero value and nil error.
type Result[T any] struct {
	started   bool
	returned  bool
	recovered bool
	value     T
	err       error
	panicval  any
}

// Unwrap propagates the result of an isolated function to the current goroutine:
// returning its values, panicking, or calling [runtime.Goexit].
func (r Result[T]) Unwrap() (T, error) {
	switch {
	case !r.started || r.returned:
		return r.value, r.err
	case r.recovered:
		panic(r.panicval)
	default:
		runtime.Goexit()
		panic("continued after runtime.Goexit")
	}
}

// Goexited returns true if this result captures a [runtime.Goexit] call.
func (r Result[T]) Goexited() bool {
	return r.started && !r.returned && !r.recovered
}

// Panicked returns true if this result captures a panic.
func (r Result[T]) Panicked() bool {
	return r.started && !r.returned && r.recovered
}

// Recovered returns any panic value captured by this result. This value may be
// nil if the result does not capture a panic, or if the panicnil=1 GODEBUG is
// enabled. [Panicked] can distinguish whether a nil panic actually occurred.
func (r Result[T]) Recovered() any {
	return r.panicval
}
