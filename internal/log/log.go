package log

import (
	"log"
	"sync/atomic"
)

var verbose atomic.Bool

// EnableVerbose enables the printing of verbose logs.
func EnableVerbose() {
	verbose.Store(true)
}

// Printf prints to the standard logger provided by the log package regardless
// of whether verbose logging is enabled.
func Printf(fmt string, v ...any) {
	log.Printf(fmt, v...)
}

// Verbosef prints to the standard logger provided by the log package if verbose
// logging is enabled. Otherwise, it does nothing.
func Verbosef(fmt string, v ...any) {
	if verbose.Load() {
		log.Printf(fmt, v...)
	}
}
