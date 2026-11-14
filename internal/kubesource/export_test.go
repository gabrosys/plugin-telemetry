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

// Set reports the object, or nil when it doesn't exist.
func (h handler) Set(obj *unstructured.Unstructured) { h.set(obj) }

// Fail reports a watch error.
func (h handler) Fail(err error) { h.fail(err) }

// Begin starts the watches with w and returns.
func (s *Source) Begin(ctx context.Context, w Watcher) { s.begin(ctx, w) }

// DynamicWatcher is the watcher the sidecar runs, over client.
func DynamicWatcher(client dynamic.Interface) Watcher { return dynamicWatcher(client) }

// WaitForToken blocks until the token file exists or ctx ends.
func (s *Source) WaitForToken(ctx context.Context) bool { return s.waitForToken(ctx) }

// NewHandler returns a handler that calls set and fail.
func NewHandler(set func(*unstructured.Unstructured), fail func(error)) Handler {
	return handler{set: set, fail: fail}
}
