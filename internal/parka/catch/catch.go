// Package catch confines the effects of panics and [runtime.Goexit] calls.
package catch

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
// Unlike [Do], it propagates [runtime.Goexit] without returning.
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
	return Result[T]{
		started:   true,
		returned:  false,
		recovered: false,
	}
}

// Panic constructs a synthetic result that captures "panic(panicval)".
func Panic[T any](panicval any) Result[T] {
	return Result[T]{
		started:   true,
		returned:  false,
		recovered: true,
		panicval:  panicval,
	}
}

// Return constructs a synthetic result that captures "return value, err".
func Return[T any](value T, err error) Result[T] {
	return Result[T]{
		started:   true,
		returned:  true,
		recovered: true,
		value:     value,
		err:       err,
	}
}

// Result captures the exit behavior of an isolated function. The zero Result
// behaves as if capturing the return of a zero T and nil error.
type Result[T any] struct {
	started   bool
	returned  bool
	recovered bool
	value     T
	err       error
	panicval  any
}

// Unwrap propagates the captured result to the current goroutine:
// returning its values, panicking, or calling [runtime.Goexit].
// It is guaranteed to return, rather than panic or Goexit,
// if and only if [Result.Returned] is true.
//
// Unwrap is the only way to obtain the value and error of a captured return;
// there is no "safe" accessor guaranteed to return in all cases.
// This intentional design choice discourages the use of results to ignore
// abnormal exit behaviors rather than inspect and handle them.
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

// Goexited is true if this result captures [runtime.Goexit].
func (r Result[T]) Goexited() bool {
	return r.started && !r.returned && !r.recovered
}

// Panicked is true if this result captures a panic.
func (r Result[T]) Panicked() bool {
	return r.started && !r.returned && r.recovered
}

// Returned is true if this result captures a normal return.
func (r Result[T]) Returned() bool {
	return !r.started || r.returned
}

// Recovered returns any panic value captured by this result.
//
// If the GODEBUG panicnil=1 setting is enabled, a nil Recovered value may
// represent a true "panic(nil)". [Result.Panicked] distinguishes nil panics
// from non-panic results.
func (r Result[T]) Recovered() any {
	return r.panicval
}
