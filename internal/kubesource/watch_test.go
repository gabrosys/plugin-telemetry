package kubesource_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/engine"
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

		mu.Lock()
		listed, watched = nil, nil
		mu.Unlock()
		events = make(chan *unstructured.Unstructured, 16)
		failures = make(chan error, 16)
		client = dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{configMaps: "ConfigMapList"},
			configMap("a", map[string]any{"k": "v"}), configMap("b", map[string]any{"k": "other"}))
		client.PrependReactor("list", "configmaps", func(a clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if la, ok := a.(clienttesting.ListAction); ok {
				listed = append(listed, la.GetListRestrictions().Fields.String())
			}
			return false, nil, nil
		})
		client.PrependWatchReactor("configmaps", func(a clienttesting.Action) (bool, watch.Interface, error) {
			mu.Lock()
			defer mu.Unlock()
			if wa, ok := a.(clienttesting.WatchAction); ok {
				watched = append(watched, wa.GetWatchRestrictions().Fields.String())
			}
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

	It("recovers a panic in a handler and keeps reporting", func() {
		var once sync.Once
		panicked := make(chan struct{})
		handler = kubesource.NewHandler(func(o *unstructured.Unstructured) {
			once.Do(func() {
				close(panicked)
				panic("handler broke")
			})
			events <- o
		}, func(err error) { failures <- err })
		startWith("c")
		Eventually(panicked).Should(BeClosed())

		_, err := client.Resource(configMaps).Namespace("ns").Create(ctx, configMap("other", map[string]any{"k": "v"}), metaCreate())
		Expect(err).NotTo(HaveOccurred())
		Eventually(events).Should(Receive(BeNil()), "the report that panicked is made again")

		_, err = client.Resource(configMaps).Namespace("ns").Create(ctx, configMap("c", map[string]any{"k": "v"}), metaCreate())
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

var _ = Describe("Snapshot", func() {
	var (
		ctx    context.Context
		client *dynfake.FakeDynamicClient
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(logr.NewContext(context.Background(), logr.Discard()))
		DeferCleanup(cancel)
		gvrs := map[schema.GroupVersionResource]string{
			{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}:                           "ClusterList",
			{Group: "telemetry.gabrosys.github.io", Version: "v1alpha1", Resource: "telemetryexporters"}: "TelemetryExporterList",
			{Group: "telemetry.gabrosys.github.io", Version: "v1alpha1", Resource: "metricquerysets"}:    "MetricQuerySetList",
			{Version: "v1", Resource: "configmaps"}:                                                      "ConfigMapList",
			{Version: "v1", Resource: "secrets"}:                                                         "SecretList",
		}
		client = dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrs,
			cluster(clusterOpts{
				plugins:    []any{pluginEntry(map[string]any{"auth": "password", "passwordSecret": "pw"}, nil)},
				monitoring: map[string]any{"customQueriesConfigMap": []any{map[string]any{"name": "mon-cm", "key": "q.yaml"}}},
			}),
			exporter(map[string]any{
				"exporter": map[string]any{"endpoint": "http://c:4318", "headers": map[string]any{"name": "hdr", "key": "h"}},
				"envFrom":  map[string]any{"configMap": map[string]any{"name": "envcm"}},
				"metrics":  map[string]any{"querySets": []any{"extra"}},
			}),
			secret("pw", map[string]string{"username": "monitor", "password": "p"}),
			secret("hdr", map[string]string{"h": "k=v"}),
			configMap("mon-cm", map[string]any{"q.yaml": "a: 1\n"}),
			querySet("extra", map[string]any{"q": map[string]any{"query": "SELECT 1"}}),
		)
	})

	snapshot := func(wait time.Duration) (*kubesource.Source, error) {
		dir := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(dir, "token"), []byte("t"), 0o600)).To(Succeed())
		return kubesource.SnapshotWith(ctx, kubesource.Options{
			Namespace: "ns", Cluster: "pg", APIServer: "https://x", TokenDir: dir,
		}, wait, kubesource.DynamicWatcher(client))
	}

	It("waits for every referenced object and treats a missing one as absent", func() {
		src, err := snapshot(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		s, _, err := src.Settings()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Postgres.User).To(Equal("monitor"))
		Expect(src.ReadFile(clusterconfig.SecretFile("hdr", "h"))).To(Equal([]byte("k=v")))
		Expect(src.ReadFile(clusterconfig.QuerySetFile("extra"))).To(MatchJSON(`{"q":{"query":"SELECT 1"}}`))
		_, err = src.ReadFile(clusterconfig.ConfigMapFile("envcm", "k"))
		Expect(err).To(MatchError(fs.ErrNotExist))
	})

	It("reports a 403 as ErrDenied", func() {
		client.PrependReactor("list", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "hdr", errForbidden)
		})
		src, err := snapshot(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
		_, err = src.ReadFile(clusterconfig.SecretFile("hdr", "h"))
		Expect(err).To(MatchError(sources.ErrDenied))
	})

	It("retries any other error until the wait passes, then says the objects are not ready", func() {
		client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewInternalError(errors.New("etcd is down"))
		})
		start := time.Now()
		_, err := snapshot(500 * time.Millisecond)
		Expect(time.Since(start)).To(BeNumerically(">=", 400*time.Millisecond))
		Expect(err).To(MatchError(engine.ErrNotReady))
		Expect(err).To(MatchError(context.DeadlineExceeded))
		Expect(err).To(MatchError(ContainSubstring("etcd is down")))
		Expect(err).To(MatchError(ContainSubstring("configmap envcm")))
	})

	It("returns one context error promptly when ctx has ended", func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
		done := make(chan error, 1)
		go func() {
			_, err := snapshot(time.Minute)
			done <- err
		}()
		var err error
		Eventually(done, "1s").Should(Receive(&err))
		Expect(err).To(MatchError(context.Canceled))
		Expect(strings.Count(err.Error(), context.Canceled.Error())).To(Equal(1))
	})
})
