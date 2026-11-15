// Package safe runs functions that handle outside data so that a panic in one is logged, not fatal.
package safe

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/go-logr/logr"
)

// last holds, per site, the panic text logged last; the site's next normal return forgets it.
var (
	mu   sync.Mutex // guards last
	last = map[string]string{}
)

// Call runs fn; a panic in fn is logged with its value and the stack, and Call returns true. The value
// is printed with %v, so never panic with Secret content. A site (what) that panics again with the same
// text logs nothing until a Call of it returns normally.
func Call(logger logr.Logger, what string, fn func()) (panicked bool) {
	defer func() {
		p := recover()
		if p == nil {
			forget(what)
			return
		}
		panicked = true
		text := fmt.Sprintf("panic: %v", p)
		if repeated(what, text) {
			return
		}
		logger.Error(errors.New(text), "recovered from a panic", "in", what, "stack", string(debug.Stack()))
	}()
	fn()
	return false
}

// repeated records text as the panic of what and reports whether it was the one logged last.
func repeated(what, text string) bool {
	mu.Lock()
	defer mu.Unlock()
	if last[what] == text {
		return true
	}
	last[what] = text
	return false
}

// Reset forgets every panic logged, so specs don't depend on each other's order.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	clear(last)
}

func forget(what string) {
	mu.Lock()
	defer mu.Unlock()
	delete(last, what)
}

// Go runs fn through Call on a new goroutine.
func Go(logger logr.Logger, what string, fn func()) {
	go Call(logger, what, fn)
}
