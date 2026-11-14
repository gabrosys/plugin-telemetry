package sources

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchOption configures Watch.
type WatchOption func(*watchConfig)

type watchConfig struct {
	onError func(error)
	onReady func()
}

// WithErrorHandler sets the function that receives the watcher's asynchronous errors.
func WithErrorHandler(fn func(error)) WatchOption {
	return func(c *watchConfig) { c.onError = fn }
}

// WithReady sets a function called once, after every directory is added and before events are read.
func WithReady(fn func()) WatchOption {
	return func(c *watchConfig) { c.onReady = fn }
}

// WatchHooks returns the ready and error functions opts set, or no-ops, for Watch implementations outside this package.
func WatchHooks(opts ...WatchOption) (ready func(), onError func(error)) {
	var cfg watchConfig
	for _, o := range opts {
		o(&cfg)
	}
	ready, onError = cfg.onReady, cfg.onError
	if ready == nil {
		ready = func() {}
	}
	if onError == nil {
		onError = func(error) {}
	}
	return ready, onError
}

// Watch calls notify, debounced, after any change in the directories holding files. It watches each
// directory, not the files, because editors and Kubernetes replace files by rename or symlink swap.
// Callers start Watch, wait for WithReady, then Load, so no change falls between the two.
// It returns nil when ctx ends.
func Watch(ctx context.Context, files []string, debounce time.Duration, notify func(), opts ...WatchOption) error {
	var cfg watchConfig
	for _, o := range opts {
		o(&cfg)
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer w.Close() //nolint:errcheck // nothing to do with a close failure on return

	seen := map[string]struct{}{}
	for _, f := range files {
		dir := filepath.Dir(f)
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}

		if err := w.Add(dir); err != nil {
			return fmt.Errorf("watch %s: %w", dir, err)
		}
	}

	if cfg.onReady != nil {
		cfg.onReady()
	}

	timer := time.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-w.Events:
			if !ok {
				return nil
			}
			timer.Reset(debounce)
		case err, ok := <-w.Errors:
			if !ok {
				return nil
			}
			if cfg.onError != nil {
				cfg.onError(fmt.Errorf("watch event: %w", err))
			}
		case <-timer.C:
			notify()
		}
	}
}
