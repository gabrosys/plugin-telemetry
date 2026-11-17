package kubesource

import (
	"context"
	"fmt"
	"sync"

	"github.com/cloudnative-pg/machinery/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/gabrosys/plugin-telemetry/internal/safe"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// watcher starts one watch on one object by name and reports it; tests replace it.
type watcher func(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string, h handler) (stop func())

type handler struct {
	set     func(obj *unstructured.Unstructured) // after sync: the object, or nil when it doesn't exist
	fail    func(err error)                      // a watch error; a 403 is reported as wrapping sources.ErrDenied
	crashed func()                               // the informer died: the watch is gone
}

// dynamicWatcher runs one informer per object, listing and watching with a metadata.name field selector,
// which is the only selector RBAC authorizes for a Role limited by resourceNames.
func dynamicWatcher(client dynamic.Interface) watcher {
	return func(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string, h handler) func() {
		ctx, cancel := context.WithCancel(ctx)
		selector := fields.OneTermEqualSelector("metadata.name", name).String()
		informer := dynamicinformer.NewFilteredDynamicInformer(client, gvr, namespace, 0, cache.Indexers{},
			func(o *metav1.ListOptions) { o.FieldSelector = selector }).Informer()

		var (
			mu        sync.Mutex // covers published and last, and orders the calls to h.set
			published bool
			last      *unstructured.Unstructured
		)
		logger := log.FromContext(ctx).GetLogger()
		report := func() {
			if !informer.HasSynced() {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			item, exists, err := informer.GetStore().GetByKey(namespace + "/" + name)
			if err != nil {
				h.fail(fmt.Errorf("read %s %s from the cache: %w", gvr.Resource, name, err))
				return
			}
			var obj *unstructured.Unstructured
			if exists {
				var ok bool
				if obj, ok = item.(*unstructured.Unstructured); !ok {
					h.fail(fmt.Errorf("read %s %s from the cache: unexpected type %T", gvr.Resource, name, item))
					return
				}
			}
			if published && obj == last {
				return
			}
			h.set(obj)
			published, last = true, obj
		}
		// The informer's listener goroutine crashes the process on a panic.
		publish := func() { safe.Call(logger, "report of "+gvr.Resource+" "+name, report) }

		if err := informer.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
			if apierrors.IsForbidden(err) {
				h.fail(fmt.Errorf("%w: %w", sources.ErrDenied, err))
				return
			}
			h.fail(err)
		}); err != nil {
			cancel()
			h.fail(fmt.Errorf("set watch error handler for %s %s: %w", gvr.Resource, name, err))
			return func() {}
		}
		if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { publish() },
			UpdateFunc: func(_, _ any) { publish() },
			DeleteFunc: func(any) { publish() },
		}); err != nil {
			cancel()
			h.fail(fmt.Errorf("add event handler for %s %s: %w", gvr.Resource, name, err))
			return func() {}
		}

		// A panic on the goroutine that runs the informer cancels the watch and reports crashed, so the
		// Source starts it again; a panic on client-go's own goroutines, such as the reflector's, is not
		// recovered.
		go func() {
			if safe.Call(logger, "informer of "+gvr.Resource+" "+name, func() { informer.RunWithContext(ctx) }) {
				cancel()
				h.crashed()
			}
		}()
		safe.Go(logger, "first report of "+gvr.Resource+" "+name, func() {
			if cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
				publish()
			}
		})

		return cancel
	}
}
