package kubesource_test

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/gabrosys/plugin-telemetry/internal/kubesource"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

var _ = Describe("the dynamic watcher", func() {
	configMaps := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

	var (
		ctx       context.Context
		client    *dynfake.FakeDynamicClient
		mu        sync.Mutex
		listed    []string
		watched   []string
		events    chan *unstructured.Unstructured
		failures  chan error
		handler   kubesource.Handler
		startWith func(name string)
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)

		listed, watched = nil, nil
		events = make(chan *unstructured.Unstructured, 16)
		failures = make(chan error, 16)
		client = dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{configMaps: "ConfigMapList"},
			configMap("a", map[string]any{"k": "v"}), configMap("b", map[string]any{"k": "other"}))
		client.PrependReactor("list", "configmaps", func(a clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			la, ok := a.(clienttesting.ListAction)
			Expect(ok).To(BeTrue())
			listed = append(listed, la.GetListRestrictions().Fields.String())
			return false, nil, nil
		})
		client.PrependWatchReactor("configmaps", func(a clienttesting.Action) (bool, watch.Interface, error) {
			mu.Lock()
			defer mu.Unlock()
			wa, ok := a.(clienttesting.WatchAction)
			Expect(ok).To(BeTrue())
			watched = append(watched, wa.GetWatchRestrictions().Fields.String())
			return false, nil, nil
		})
		handler = kubesource.NewHandler(func(o *unstructured.Unstructured) { events <- o }, func(err error) { failures <- err })
		startWith = func(name string) { kubesource.DynamicWatcher(client)(ctx, configMaps, "ns", name, handler) }
	})

	It("lists and watches one object with a metadata.name field selector", func() {
		startWith("a")

		var got *unstructured.Unstructured
		Eventually(events).Should(Receive(&got))
		Expect(got.GetName()).To(Equal("a"))

		Eventually(func() []string { mu.Lock(); defer mu.Unlock(); return watched }).ShouldNot(BeEmpty())
		mu.Lock()
		defer mu.Unlock()
		Expect(listed).To(ConsistOf("metadata.name=a"))
		Expect(watched).To(HaveEach("metadata.name=a"))
	})

	It("reports nil for an object that doesn't exist, then the object when it appears", func() {
		startWith("c")
		Eventually(events).Should(Receive(BeNil()))

		_, err := client.Resource(configMaps).Namespace("ns").Create(ctx, configMap("c", map[string]any{"k": "v"}), metaCreate())
		Expect(err).NotTo(HaveOccurred())
		var got *unstructured.Unstructured
		Eventually(events).Should(Receive(&got))
		Expect(got.GetName()).To(Equal("c"))
	})

	It("reports a 403 as ErrDenied", func() {
		client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "a", errForbidden)
		})
		startWith("a")
		Eventually(failures).Should(Receive(MatchError(sources.ErrDenied)))
	})
})
