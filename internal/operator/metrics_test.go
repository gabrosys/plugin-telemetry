package operator_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	cnpgoperator "github.com/cloudnative-pg/cnpg-i/pkg/operator"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/test/otlptest"
)

// metricByName collects m and returns the metric called name.
func metricByName(m *operator.PluginMetrics, name string) (metricdata.Metrics, bool) {
	GinkgoHelper()

	rm, err := operator.CollectMetrics(m, context.Background())
	Expect(err).NotTo(HaveOccurred())
	for _, sm := range rm.ScopeMetrics {
		for _, got := range sm.Metrics {
			if got.Name == name {
				return got, true
			}
		}
	}

	return metricdata.Metrics{}, false
}

// counts returns the value of each point of the counter name, keyed by its attributes as k=v,k=v.
func counts(m *operator.PluginMetrics, name string) map[string]int64 {
	GinkgoHelper()

	got, ok := metricByName(m, name)
	Expect(ok).To(BeTrue(), name)
	sum, ok := got.Data.(metricdata.Sum[int64])
	Expect(ok).To(BeTrue(), name)

	out := map[string]int64{}
	for _, p := range sum.DataPoints {
		out[attrKey(p.Attributes)] = p.Value
	}

	return out
}

func attrKey(set attribute.Set) string {
	key := ""
	for i, kv := range set.ToSlice() {
		if i > 0 {
			key += ","
		}
		key += string(kv.Key) + "=" + kv.Value.AsString()
	}

	return key
}

const (
	hookCalls     = "cnpg_otel_plugin_hook_calls_total"
	hookDuration  = "cnpg_otel_plugin_hook_duration_seconds"
	identityCount = "cnpg_otel_plugin_identity_reconciles_total"
)

var _ = Describe("PluginMetrics", func() {
	It("counts hook calls by service, operation and outcome and records their duration", func(ctx context.Context) {
		m := operator.NewPluginMetrics()

		m.Hook(ctx, "lifecycle", "create", "patched", 20*time.Millisecond)
		m.Hook(ctx, "lifecycle", "create", "patched", 40*time.Millisecond)
		m.Hook(ctx, "operator", "set_status", "status", time.Second)

		Expect(counts(m, hookCalls)).To(Equal(map[string]int64{
			"operation=create,outcome=patched,service=lifecycle":   2,
			"operation=set_status,outcome=status,service=operator": 1,
		}))

		got, ok := metricByName(m, hookDuration)
		Expect(ok).To(BeTrue())
		Expect(got.Unit).To(Equal("s"))
		hist, ok := got.Data.(metricdata.Histogram[float64])
		Expect(ok).To(BeTrue())
		Expect(hist.DataPoints).To(HaveLen(2))
		for _, p := range hist.DataPoints {
			Expect(p.Bounds).To(BeEmpty())
			Expect(p.BucketCounts).To(HaveLen(1))
			switch attrKey(p.Attributes) {
			case "operation=create,service=lifecycle":
				Expect(p.Count).To(BeEquivalentTo(2))
				Expect(p.Sum).To(BeNumerically("~", 0.06, 1e-9))
			case "operation=set_status,service=operator":
				Expect(p.Count).To(BeEquivalentTo(1))
			default:
				Fail("unexpected attributes " + attrKey(p.Attributes))
			}
		}
	})

	It("counts identity reconciles by outcome and, for errors, by reason", func(ctx context.Context) {
		m := operator.NewPluginMetrics()

		m.IdentityReconcile(ctx, nil)
		m.IdentityReconcile(ctx, nil)
		m.IdentityReconcile(ctx, errors.New("boom"))
		m.IdentityReconcile(ctx, fmt.Errorf("get cluster: %w", context.DeadlineExceeded))
		m.IdentityReconcile(ctx, fmt.Errorf("ensure: %w", apierrors.NewForbidden(schema.GroupResource{Resource: "rolebindings"}, "pg", errors.New("denied"))))

		Expect(counts(m, identityCount)).To(Equal(map[string]int64{
			"outcome=ok":                     2,
			"outcome=error,reason=Unknown":   1,
			"outcome=error,reason=Timeout":   1,
			"outcome=error,reason=Forbidden": 1,
		}))
	})

	It("records Unknown for a server-supplied reason outside the API's list", func(ctx context.Context) {
		m := operator.NewPluginMetrics()

		m.IdentityReconcile(ctx, fmt.Errorf("ensure: %w", &apierrors.StatusError{ErrStatus: metav1.Status{Reason: "SomethingCustom"}}))
		m.IdentityReconcile(ctx, fmt.Errorf("ensure: %w", apierrors.NewForbidden(schema.GroupResource{Resource: "roles"}, "pg", errors.New("denied"))))

		Expect(counts(m, identityCount)).To(Equal(map[string]int64{
			"outcome=error,reason=Unknown":   1,
			"outcome=error,reason=Forbidden": 1,
		}))
	})

	It("records nothing on a nil receiver", func(ctx context.Context) {
		var m *operator.PluginMetrics

		Expect(func() {
			m.Hook(ctx, "lifecycle", "create", "patched", time.Second)
			m.IdentityReconcile(ctx, errors.New("boom"))
		}).NotTo(Panic())
	})

	It("is safe for concurrent use", func(ctx context.Context) {
		m := operator.NewPluginMetrics()
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 50 {
					m.Hook(ctx, "operator", "set_status", "noop", time.Millisecond)
					m.IdentityReconcile(ctx, nil)
				}
			})
		}
		wg.Wait()

		Expect(counts(m, hookCalls)).To(Equal(map[string]int64{"operation=set_status,outcome=noop,service=operator": 400}))
		Expect(counts(m, identityCount)).To(Equal(map[string]int64{"outcome=ok": 400}))
	})

	It("exports to --metrics-endpoint with the plugin resource", func(ctx SpecContext) {
		GinkgoT().Setenv("POD_NAMESPACE", "cnpg-system")
		GinkgoT().Setenv("POD_NAME", "plugin-0")
		srv := otlptest.NewHTTP(nil)
		DeferCleanup(srv.Close)

		s, enabled, err := operator.MetricsExporter(srv.URL, time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(enabled).To(BeTrue())

		m := operator.NewPluginMetrics()
		m.Hook(ctx, "lifecycle", "evaluate", "unchanged", time.Millisecond)

		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- m.Run(runCtx, s) }()
		DeferCleanup(func() {
			cancel()
			Eventually(done).Should(Receive(Succeed()))
		})

		Eventually(srv.Requests).WithTimeout(10 * time.Second).ShouldNot(BeEmpty())
		req := srv.Requests()[0]
		Expect(req.Path).To(Equal("/v1/metrics"))

		attrs := map[string]string{}
		var names []string
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, kv := range rm.GetResource().GetAttributes() {
				attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
			}
			for _, sm := range rm.GetScopeMetrics() {
				for _, met := range sm.GetMetrics() {
					names = append(names, met.GetName())
				}
			}
		}
		Expect(attrs).To(Equal(map[string]string{
			"service.name":        "plugin-telemetry",
			"service.namespace":   "cnpg-system",
			"service.instance.id": "cnpg-system.plugin-0.plugin-telemetry",
			"k8s.namespace.name":  "cnpg-system",
			"k8s.pod.name":        "plugin-0",
		}))
		Expect(names).To(ContainElements(hookCalls, hookDuration))
	}, SpecTimeout(30*time.Second))
})

var _ = Describe("MetricsExporter", func() {
	It("starts no exporter without --metrics-endpoint", func() {
		_, enabled, err := operator.MetricsExporter("", 30*time.Second)

		Expect(err).NotTo(HaveOccurred())
		Expect(enabled).To(BeFalse())
	})

	It("appends v1/metrics to the base endpoint after ensuring a trailing slash", func() {
		for _, base := range []string{"http://collector:4318", "http://collector:4318/"} {
			s, enabled, err := operator.MetricsExporter(base, 15*time.Second)

			Expect(err).NotTo(HaveOccurred())
			Expect(enabled).To(BeTrue())
			Expect(s.Endpoint).To(Equal("http://collector:4318/v1/metrics"))
			Expect(s.Interval).To(Equal(15 * time.Second))
			Expect(s.Timeout).To(Equal(settings.DefaultExporterTimeout(15 * time.Second)))
			Expect(s.Protocol).To(Equal(settings.ProtocolHTTP))
		}
	})

	It("rejects an invalid --metrics-endpoint at start", func() {
		_, _, err := operator.MetricsExporter("ftp://collector", 30*time.Second)

		Expect(err).To(MatchError(ContainSubstring("exporter.endpoint: must be an http or https URL with a host")))
	})

	It("rejects an interval below the minimum", func() {
		_, _, err := operator.MetricsExporter("http://collector:4318", time.Millisecond)

		Expect(err).To(MatchError(ContainSubstring("exporter.interval")))
	})
})

var _ = Describe("hooks recording metrics", func() {
	var m *operator.PluginMetrics

	BeforeEach(func() { m = operator.NewPluginMetrics() })

	It("records the lifecycle hook by operation and outcome", func(ctx context.Context) {
		l := &operator.Lifecycle{Images: fakeImages(testImage), Metrics: m}
		pod := fixturePod("pod-main")

		_, err := l.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_CREATE,
			clusterJSON(ours(nil)), pod))
		Expect(err).NotTo(HaveOccurred())
		_, err = l.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, clusterJSON(), pod))
		Expect(err).NotTo(HaveOccurred())
		_, err = l.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, []byte(`{"spec":`), pod))
		Expect(err).To(HaveOccurred())

		Expect(counts(m, hookCalls)).To(Equal(map[string]int64{
			"operation=create,outcome=patched,service=lifecycle":   1,
			"operation=evaluate,outcome=skipped,service=lifecycle": 1,
			"operation=evaluate,outcome=error,service=lifecycle":   1,
		}))
	})

	It("records SetStatusInCluster as status, noop or error", func(ctx context.Context) {
		c := newClient(nil, interceptor.Funcs{})
		s := &operator.Status{Reader: c, Identity: &identity.Ensurer{Reader: c, Writer: c}, Metrics: m}

		_, err := s.SetStatusInCluster(ctx, hookStatusRequest(newCluster("ns", "pg", ours(nil))))
		Expect(err).NotTo(HaveOccurred())
		_, err = s.SetStatusInCluster(ctx, hookStatusRequest(newCluster("ns", "pg")))
		Expect(err).NotTo(HaveOccurred())
		_, err = s.SetStatusInCluster(ctx, &cnpgoperator.SetStatusInClusterRequest{Cluster: []byte("{")})
		Expect(err).NotTo(HaveOccurred())

		Expect(counts(m, hookCalls)).To(Equal(map[string]int64{
			"operation=set_status,outcome=status,service=operator": 1,
			"operation=set_status,outcome=noop,service=operator":   1,
			"operation=set_status,outcome=error,service=operator":  1,
		}))
	})

	DescribeTable("counts EVALUATE as an error when it can't inject the sidecar",
		func(ctx context.Context, images string, cluster []byte) {
			l := &operator.Lifecycle{Images: fakeImages(images), Metrics: m}

			_, _ = l.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, fixturePod("pod-main")))

			Expect(counts(m, hookCalls)).To(Equal(map[string]int64{"operation=evaluate,outcome=error,service=lifecycle": 1}))
		},
		Entry("no image", "", clusterJSON(ours(nil))),
		Entry("auth ldap", testImage, clusterJSON(ours(map[string]string{"auth": "ldap"}))),
		Entry("malformed Cluster", testImage, []byte(`{"spec":`)),
	)
})
