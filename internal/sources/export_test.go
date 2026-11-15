package sources

import (
	"context"
	"fmt"

	"github.com/fsnotify/fsnotify"
	"k8s.io/utils/clock"
)

// WatchRetry is how often Watch re-reads its files and retries what failed.
const WatchRetry = watchRetry

// WatchWith is Watch on clk.
func WatchWith(ctx context.Context, clk clock.Clock, files func() []string, notify func()) error {
	return watch(ctx, clk, fsnotify.NewWatcher, files, notify)
}

// WatchWithErrors is WatchWith whose watchers report the errors sent on errs instead of their own.
func WatchWithErrors(ctx context.Context, clk clock.Clock, files func() []string, notify func(), errs chan error) error {
	return watch(ctx, clk, func() (*fsnotify.Watcher, error) {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			return nil, fmt.Errorf("create the watcher: %w", err)
		}
		w.Errors = errs
		return w, nil
	}, files, notify)
}
