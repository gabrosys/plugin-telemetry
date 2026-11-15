package sources

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/fsnotify/fsnotify"
	"k8s.io/utils/clock"
)

const (
	// Debounce is the quiet time after a change before Watch notifies.
	Debounce = time.Second
	// watchRetry is how often Watch re-reads its files and retries what failed.
	watchRetry = 5 * time.Second
)

var errEventsClosed = errors.New("the watcher's event channel closed")

// Watch watches the directories of files(), not the files, because editors and Kubernetes replace
// files by rename or symlink swap. It calls notify once it is watching, then, debounced, after any
// change in those directories. It re-reads files() after each notify and every 5 s, adds and removes
// directories itself and notifies once after its set grows. A failure to create the watcher or add a
// directory, and an fsnotify error, is logged once until it recovers and retried every 5 s; an fsnotify
// error is followed by a notify. It returns nil when ctx ends.
func Watch(ctx context.Context, files func() []string, notify func()) error {
	return watch(ctx, clock.RealClock{}, fsnotify.NewWatcher, files, notify)
}

// dirWatch is the state of one Watch call.
type dirWatch struct {
	logger     log.Logger
	newWatcher func() (*fsnotify.Watcher, error)
	files      func() []string
	notify     func()

	w        *fsnotify.Watcher // nil until it is created, and after it is lost
	failing  string            // the failure last logged; empty while healthy
	eventErr bool              // an fsnotify error arrived since the last retry
}

func watch(ctx context.Context, clk clock.Clock, newWatcher func() (*fsnotify.Watcher, error), files func() []string, notify func()) error {
	d := &dirWatch{logger: log.FromContext(ctx), newWatcher: newWatcher, files: files, notify: notify}
	defer d.close()

	debounce := clk.NewTimer(Debounce)
	if !debounce.Stop() {
		<-debounce.C()
	}
	defer debounce.Stop()
	retry := clk.NewTimer(watchRetry)
	defer retry.Stop()

	d.sync()
	if d.w != nil {
		d.notifyAndSync()
	}
	for {
		var events <-chan fsnotify.Event
		var errs <-chan error
		if d.w != nil {
			events, errs = d.w.Events, d.w.Errors
		}
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-events:
			if !ok {
				d.lost()
				continue
			}
			debounce.Reset(Debounce)
		case err, ok := <-errs:
			if !ok {
				d.lost()
				continue
			}
			// An fsnotify error, such as an overflow, can mean lost events.
			d.eventErr = true
			d.fail(fmt.Errorf("watch event: %w", err))
			d.notify()
		case <-debounce.C():
			d.notifyAndSync()
		case <-retry.C():
			if d.sync() {
				d.notifyAndSync()
			}
			d.eventErr = false
			retry.Reset(watchRetry)
		}
	}
}

// notifyAndSync notifies, then re-reads files() and notifies again for as long as the set grows.
func (d *dirWatch) notifyAndSync() {
	for {
		d.notify()
		if !d.sync() {
			return
		}
	}
}

// sync makes the watcher cover the directories of files(), creating it first if needed, and reports
// whether it added a directory.
func (d *dirWatch) sync() (grew bool) {
	if d.w == nil {
		w, err := d.newWatcher()
		if err != nil {
			d.fail(fmt.Errorf("create the watcher: %w", err))
			return false
		}
		d.w = w
	}

	want := map[string]bool{}
	var dirs []string
	for _, f := range d.files() {
		dir := filepath.Dir(f)
		if !want[dir] {
			want[dir] = true
			dirs = append(dirs, dir)
		}
	}
	// fsnotify drops the watch of a directory that is deleted or renamed, so the list is the truth.
	have := map[string]bool{}
	for _, dir := range d.w.WatchList() {
		have[dir] = true
	}

	var errs []error
	for dir := range have {
		if want[dir] {
			continue
		}
		if err := d.w.Remove(dir); err != nil && !errors.Is(err, fsnotify.ErrNonExistentWatch) {
			errs = append(errs, fmt.Errorf("unwatch %s: %w", dir, err))
		}
	}
	for _, dir := range dirs {
		if have[dir] {
			continue
		}
		if err := d.w.Add(dir); err != nil {
			errs = append(errs, fmt.Errorf("watch %s: %w", dir, err))
			continue
		}
		grew = true
	}

	if err := errors.Join(errs...); err != nil {
		d.fail(err)
	} else if !d.eventErr {
		d.recovered()
	}
	return grew
}

// lost drops a watcher whose channels closed; the next retry creates another.
func (d *dirWatch) lost() {
	d.close()
	d.fail(errEventsClosed)
}

func (d *dirWatch) close() {
	if d.w == nil {
		return
	}
	if err := d.w.Close(); err != nil {
		d.logger.Debug("close the watcher", "error", err.Error())
	}
	d.w = nil
}

// fail logs err unless it is the failure already logged.
func (d *dirWatch) fail(err error) {
	if err.Error() == d.failing {
		return
	}
	d.failing = err.Error()
	d.logger.Error(err, "watch the configuration files", "retryEvery", watchRetry)
}

// recovered logs the end of a failure once.
func (d *dirWatch) recovered() {
	if d.failing == "" {
		return
	}
	d.failing = ""
	d.logger.Info("watching the configuration files again")
}
