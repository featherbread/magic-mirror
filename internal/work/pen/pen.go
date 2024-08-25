// Package pen confines the effects of panics and [runtime.Goexit] calls.
package pen

import (
	"runtime"
	"sync"
)

// Do runs fn in an independent goroutine and captures its exit behavior,
// isolating the caller from any panic or [runtime.Goexit].
func Do[T any](fn func() (T, error)) (result Result[T]) {
	result = Goexit[T]{}
	var wg sync.WaitGroup
	wg.Go(func() { result = DoOrExit(fn) })
	wg.Wait()
	return
}

// DoOrExit runs fn in the current goroutine and captures a [Return] or [Panic].
// Unlike [Do], it propagates [runtime.Goexit] from fn rather than returning.
func DoOrExit[T any](fn func() (T, error)) Result[T] {
	var (
		complete bool
		result   T
		err      error
		panicval any
	)
	func() {
		defer func() { panicval = recover() }()
		result, err = fn()
		complete = true
	}()
	if complete {
		return Return[T]{Value: result, Err: err}
	} else {
		return Panic[T]{Value: panicval}
	}
}

// Result holds a [Return], [Panic], or [Goexit] that encapsulates the exit
// behavior of an isolated function. Get returns the same values as the
// function, or propagates its abnormal exit to the current goroutine.
type Result[T any] interface {
	Get() (T, error)
	private()
}

func (Return[T]) private() {}
func (Panic[T]) private()  {}
func (Goexit[T]) private() {}

// Return indicates that the function returned normally.
type Return[T any] struct {
	Value T
	Err   error
}

// Panic indicates that the function panicked.
type Panic[T any] struct{ Value any }

// Goexit indicates that the function called [runtime.Goexit].
type Goexit[T any] struct{}

func (r Return[T]) Get() (T, error) {
	return r.Value, r.Err
}
func (p Panic[T]) Get() (T, error) {
	panic(p.Value)
}
func (Goexit[T]) Get() (T, error) {
	runtime.Goexit()
	panic("continued after Goexit")
}
