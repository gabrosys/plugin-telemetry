// Package safe runs functions that handle outside data so that a panic in one is logged, not fatal.
package safe

import (
	"fmt"
	"runtime/debug"

	"github.com/go-logr/logr"
)

// Call runs fn; a panic in fn is logged with its value and the stack, and Call returns true.
func Call(logger logr.Logger, what string, fn func()) (panicked bool) {
	defer func() {
		if p := recover(); p != nil {
			panicked = true
			logger.Error(fmt.Errorf("panic: %v", p), "recovered from a panic", "in", what, "stack", string(debug.Stack()))
		}
	}()
	fn()
	return false
}

// Go runs fn through Call on a new goroutine.
func Go(logger logr.Logger, what string, fn func()) {
	go Call(logger, what, fn)
}
