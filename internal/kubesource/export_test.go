package kubesource

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type (
	Watcher = watcher
	Handler = handler
)

// ErrNotSynced is what ReadFile wraps for an object whose watch has not reported yet.
var ErrNotSynced = errNotSynced

// Set reports the object, or nil when it doesn't exist.
func (h handler) Set(obj *unstructured.Unstructured) { h.set(obj) }

// Fail reports a watch error.
func (h handler) Fail(err error) { h.fail(err) }

// RunWith watches with w until ctx ends.
func (s *Source) RunWith(ctx context.Context, w Watcher) { s.run(ctx, w) }

// SnapshotWith is Snapshot over client, with no token wait.
func SnapshotWith(ctx context.Context, o Options, client dynamic.Interface) (*Source, error) {
	return New(o).snapshot(ctx, client)
}

// DynamicWatcher is the watcher the sidecar runs, over client.
func DynamicWatcher(client dynamic.Interface) Watcher { return dynamicWatcher(client) }

// WaitForToken blocks until the token file exists or ctx ends.
func (s *Source) WaitForToken(ctx context.Context) bool { return s.waitForToken(ctx) }

// NewHandler returns a handler that calls set and fail.
func NewHandler(set func(*unstructured.Unstructured), fail func(error)) Handler {
	return handler{set: set, fail: fail}
}
