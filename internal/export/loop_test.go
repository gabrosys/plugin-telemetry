package export_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/export/otlptest"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

// fixedProducer produces one gauge of n points, and err when set.
type fixedProducer struct {
	mu  sync.Mutex // guards n and err
	n   int
	err error

	panics int // the next Produce calls that panic
	size   int // bytes of each of the 8 labels of a point; 0 gives one small label
}

func (p *fixedProducer) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.panics > 0 {
		p.panics--
		panic("producer broke")
	}
	if p.size > 0 {
		return wide(p.n, 8, p.size).ScopeMetrics, p.err
	}
	return batch(p.n, 1).ScopeMetrics, p.err
}

// wide returns one gauge of n points, each with labels attributes of size bytes.
func wide(n, labels, size int) *metricdata.ResourceMetrics {
	v := strings.Repeat("x", size)
	dps := make([]metricdata.DataPoint[float64], n)
	for i := range dps {
		kvs := []attribute.KeyValue{attribute.Int("i", i)}
		for l := range labels {
			kvs = append(kvs, attribute.String("l"+strconv.Itoa(l), v))
		}
		dps[i] = metricdata.DataPoint[float64]{Attributes: attribute.NewSet(kvs...), Time: time.Unix(1, 0), Value: float64(i)}
	}
	return &metricdata.ResourceMetrics{
		Resource: loopResource,
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope:   instrumentation.Scope{Name: "s"},
			Metrics: []metricdata.Metrics{{Name: "g", Data: metricdata.Gauge[float64]{DataPoints: dps}}},
		}},
	}
}

func (p *fixedProducer) set(n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n, p.err = n, err
}

var loopResource = resource.NewWithAttributes("", attribute.String("service.name", "pg"))

// loopHarness runs a Loop on a fake clock over a MeterProvider with the self-metrics,
// and reads them back through a second reader.
type loopHarness struct {
	clk   *testingclock.FakeClock
	prod  *fixedProducer
	probe *sdkmetric.ManualReader
	loop  *export.Loop

	cancel context.CancelFunc
	done   chan error
}

func newLoopHarness(seed uint64) *loopHarness {
	GinkgoHelper()
	h := &loopHarness{
		clk:   testingclock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
		prod:  &fixedProducer{n: 3},
		probe: sdkmetric.NewManualReader(),
	}
	stats := selfmetrics.NewStats()
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(h.prod))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("sdk", "default"))),
		sdkmetric.WithReader(reader),
		sdkmetric.WithReader(h.probe),
		sdkmetric.WithView(selfmetrics.Views()...),
	)
	DeferCleanup(mp.Shutdown)
	Expect(selfmetrics.Register(mp.Meter("self"), stats)).To(Succeed())
	rnd := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // seeded for reproducible timing
	h.loop = export.NewLoop(reader, loopResource, h.clk, rnd, stats)
	return h
}

func (h *loopHarness) start() {
	GinkgoHelper()
	var ctx context.Context
	ctx, h.cancel = context.WithCancel(context.Background())
	h.done = make(chan error, 1)
	go func() { h.done <- h.loop.Run(ctx) }()
	DeferCleanup(h.stop)
}

func (h *loopHarness) stop() {
	GinkgoHelper()
	if h.cancel == nil {
		return
	}
	h.cancel()
	Eventually(h.done).Should(Receive(BeNil()))
	h.cancel = nil
}

// step advances the clock by d while the loop waits for its next tick, and returns once it waits again.
func (h *loopHarness) step(d time.Duration) {
	GinkgoHelper()
	Eventually(h.clk.HasWaiters).Should(BeTrue())
	h.clk.Step(d)
	Eventually(h.clk.HasWaiters).Should(BeTrue())
}

// stepUntilRequest steps by one second until srv has a request and returns the steps taken.
func (h *loopHarness) stepUntilRequest(srv *otlptest.Server, maxSteps int) int {
	GinkgoHelper()
	for i := 1; i <= maxSteps; i++ {
		h.step(time.Second)
		if len(srv.Requests()) > 0 {
			return i
		}
	}
	Fail("no export")
	return 0
}

func (h *loopHarness) collect() map[string]metricdata.Metrics {
	GinkgoHelper()
	var rm metricdata.ResourceMetrics
	Expect(h.probe.Collect(context.Background(), &rm)).To(Succeed())
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// value returns the counter or gauge value, or the histogram count, of the point with exactly attrs.
func (h *loopHarness) value(name string, attrs ...attribute.KeyValue) float64 {
	GinkgoHelper()
	want := attribute.NewSet(attrs...)
	switch d := h.collect()[name].Data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			if p.Attributes.Equals(&want) {
				return float64(p.Value)
			}
		}
	case metricdata.Gauge[float64]:
		for _, p := range d.DataPoints {
			if p.Attributes.Equals(&want) {
				return p.Value
			}
		}
	case metricdata.Histogram[float64]:
		for _, p := range d.DataPoints {
			if p.Attributes.Equals(&want) {
				return float64(p.Count)
			}
		}
	}
	return 0
}

func (h *loopHarness) exports(outcome string) float64 {
	GinkgoHelper()
	return h.value("cnpg_otel_exports_total", attribute.String("outcome", outcome))
}

func httpExporter(srv *otlptest.Server) *export.Exporter {
	GinkgoHelper()
	return newExporter(exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics"))
}

// componentAttrs are the attributes otel.sdk.exporter.* carry for an HTTP exporter sending to srv.
func componentAttrs(srv *otlptest.Server, extra ...attribute.KeyValue) []attribute.KeyValue {
	GinkgoHelper()
	u, err := url.Parse(srv.URL)
	Expect(err).NotTo(HaveOccurred())
	port, err := strconv.Atoi(u.Port())
	Expect(err).NotTo(HaveOccurred())
	return append([]attribute.KeyValue{
		attribute.String("otel.component.type", "otlp_http_metric_exporter"),
		attribute.String("otel.component.name", "otlp_http_metric_exporter/0"),
		attribute.String("server.address", u.Hostname()),
		attribute.Int("server.port", port),
	}, extra...)
}

func pointsOf(reqs []otlptest.Request) int {
	n := 0
	for _, r := range reqs {
		n += r.Points()
	}
	return n
}

var _ = Describe("Loop", func() {
	It("waits a random offset below one interval before the first export", func() {
		first := func(seed uint64) int {
			srv := newHTTPServer(nil)
			h := newLoopHarness(seed)
			h.loop.SetExporter(context.Background(), httpExporter(srv), 100*time.Second, 1000)
			h.start()
			return h.stepUntilRequest(srv, 100)
		}

		a, b := first(1), first(2)

		Expect(a).To(BeNumerically("<=", 100))
		Expect(b).To(BeNumerically("<=", 100))
		Expect(a).NotTo(Equal(b))
	})

	It("exports once per interval, independent of query intervals", func() {
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()
		h.stepUntilRequest(srv, 10)

		for k := 1; k <= 3; k++ {
			for i := 1; i < 10; i++ {
				h.prod.set(3+i, nil)
				h.step(time.Second)
				Expect(srv.Requests()).To(HaveLen(k))
			}
			h.step(time.Second)
			Expect(srv.Requests()).To(HaveLen(k + 1))
		}
	})

	It("skips a tick while an export runs and counts it", func() {
		srv := newHTTPServer(nil)
		release := make(chan struct{})
		srv.Respond(otlptest.Response{Block: release}, otlptest.Response{})
		h := newLoopHarness(1)
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()

		for i := 0; len(srv.Requests()) == 0; i++ {
			Expect(i).To(BeNumerically("<", 10), "no export within one interval")
			Eventually(h.clk.HasWaiters).Should(BeTrue())
			h.clk.Step(time.Second)
			Eventually(func() bool { return h.clk.HasWaiters() || len(srv.Requests()) > 0 }).Should(BeTrue())
		}
		h.clk.Step(25 * time.Second)
		Consistently(srv.Requests, 100*time.Millisecond).Should(HaveLen(1))
		close(release)
		Eventually(h.clk.HasWaiters).Should(BeTrue())

		Expect(h.exports("ok")).To(Equal(1.0))
		Expect(h.exports("skipped")).To(Equal(2.0))
		h.step(4 * time.Second)
		Expect(srv.Requests()).To(HaveLen(1))
		h.step(time.Second)
		Expect(srv.Requests()).To(HaveLen(2))
	})

	It("replaces the SDK resource with its own, whatever OTEL_RESOURCE_ATTRIBUTES says", func() {
		GinkgoT().Setenv("OTEL_RESOURCE_ATTRIBUTES", "evil=1")
		GinkgoT().Setenv("OTEL_SERVICE_NAME", "evil")
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		var sdk metricdata.ResourceMetrics
		Expect(h.probe.Collect(context.Background(), &sdk)).To(Succeed())
		_, found := sdk.Resource.Set().Value("evil")
		Expect(found).To(BeTrue())

		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()
		h.stepUntilRequest(srv, 10)

		got := map[string]string{}
		for _, kv := range srv.Requests()[0].Metrics.GetResourceMetrics()[0].GetResource().GetAttributes() {
			got[kv.GetKey()] = kv.GetValue().GetStringValue()
		}
		Expect(got).To(Equal(map[string]string{"service.name": "pg"}))
	})

	It("records exports_total, the last success time and otel.sdk.exporter.* per request", func() {
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 2)
		h.start()
		exported := "otel.sdk.exporter.metric_data_point.exported"
		duration := "otel.sdk.exporter.operation.duration"

		h.stepUntilRequest(srv, 10)
		tick1 := h.clk.Now()
		first := srv.Requests()
		Expect(first).To(HaveLen(2))
		Expect(pointsOf(first)).To(Equal(3))
		Expect(h.exports("ok")).To(Equal(2.0))
		Expect(h.value(exported, componentAttrs(srv)...)).To(Equal(3.0))
		Expect(h.value(duration, componentAttrs(srv, attribute.Int("http.response.status_code", 200))...)).To(Equal(2.0))
		lastOK := float64(tick1.UnixNano()) / float64(time.Second)
		Expect(h.value("cnpg_otel_last_export_success_timestamp_seconds")).To(Equal(lastOK))

		srv.Respond(otlptest.Response{HTTPStatus: 400})
		h.step(10 * time.Second)
		failed := srv.Requests()[len(first):]
		Expect(failed).NotTo(BeEmpty())
		Expect(h.exports("error")).To(Equal(float64(len(failed))))
		Expect(h.value(exported, componentAttrs(srv, attribute.String("error.type", "400"))...)).To(Equal(float64(pointsOf(failed))))
		Expect(h.value(duration, componentAttrs(srv,
			attribute.String("error.type", "400"), attribute.Int("http.response.status_code", 400))...)).To(Equal(float64(len(failed))))
		Expect(h.value("cnpg_otel_last_export_success_timestamp_seconds")).To(Equal(lastOK))

		srv.Respond(otlptest.Response{Rejected: 1})
		h.step(10 * time.Second)
		partial := srv.Requests()[len(first)+len(failed):]
		Expect(partial).NotTo(BeEmpty())
		Expect(h.exports("ok")).To(Equal(float64(2 + len(partial))))
		Expect(h.value(exported, componentAttrs(srv, attribute.String("error.type", "rejected"))...)).To(Equal(float64(len(partial))))
		Expect(h.value(exported, componentAttrs(srv)...)).To(Equal(float64(3 + pointsOf(partial) - len(partial))))
	})

	It("records a failed collection as a failed export with no points", func() {
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		h.prod.set(3, errors.New("producer failed"))
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()

		for i := 0; i < 10 && h.exports("error") == 0; i++ {
			h.step(time.Second)
		}

		Expect(h.exports("error")).To(Equal(1.0))
		Expect(srv.Requests()).To(BeEmpty())
		Expect(h.value("otel.sdk.exporter.operation.duration", componentAttrs(srv, attribute.String("error.type", "_OTHER"))...)).To(Equal(1.0))
		Expect(h.value("otel.sdk.exporter.metric_data_point.exported", componentAttrs(srv, attribute.String("error.type", "_OTHER"))...)).To(BeZero())
	})

	It("counts a panicking tick as one failed export and exports at the next one", func() {
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		h.prod.mu.Lock()
		h.prod.panics = 1
		h.prod.mu.Unlock()
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()

		for i := 0; i < 10 && h.exports("error") == 0; i++ {
			h.step(time.Second)
		}
		Expect(h.exports("error")).To(Equal(1.0))
		Expect(srv.Requests()).To(BeEmpty())
		Expect(h.value("otel.sdk.exporter.operation.duration", componentAttrs(srv, attribute.String("error.type", "_OTHER"))...)).To(Equal(1.0))

		h.stepUntilRequest(srv, 11)
		Expect(h.exports("error")).To(Equal(1.0))
		Expect(h.exports("ok")).To(Equal(1.0))
	})

	It("switches to a new exporter at the next tick and shuts the old one down", func(ctx SpecContext) {
		srvA, srvB := newHTTPServer(nil), newHTTPServer(nil)
		a, b := httpExporter(srvA), httpExporter(srvB)
		h := newLoopHarness(1)
		h.loop.SetExporter(ctx, a, 10*time.Second, 1000)
		h.start()
		h.stepUntilRequest(srvA, 10)

		h.loop.SetExporter(ctx, b, 20*time.Second, 1000)
		Expect(a.Export(ctx, batch(1, 1))).To(Succeed())
		for range 9 {
			h.step(time.Second)
		}
		Expect(srvB.Requests()).To(BeEmpty())
		h.step(time.Second)

		Expect(srvA.Requests()).To(HaveLen(2))
		Expect(srvB.Requests()).To(HaveLen(1))
		Expect(a.Export(ctx, batch(1, 1))).To(MatchError(export.ErrExporterShutdown))
		for range 19 {
			h.step(time.Second)
		}
		Expect(srvB.Requests()).To(HaveLen(1))
		h.step(time.Second)
		Expect(srvB.Requests()).To(HaveLen(2))
	})

	It("shuts down an exporter replaced before its first tick", func(ctx SpecContext) {
		srvA, srvB := newHTTPServer(nil), newHTTPServer(nil)
		a, b := httpExporter(srvA), httpExporter(srvB)
		h := newLoopHarness(1)
		h.loop.SetExporter(ctx, a, 10*time.Second, 1000)
		h.loop.SetExporter(ctx, b, 10*time.Second, 1000)

		Expect(a.Export(ctx, batch(1, 1))).To(MatchError(export.ErrExporterShutdown))
		h.start()
		h.stepUntilRequest(srvB, 10)

		Expect(srvA.Requests()).To(BeEmpty())
	})

	It("keeps the exporter it is using when that one is set again and then replaced", func(ctx SpecContext) {
		srvA, srvB := newHTTPServer(nil), newHTTPServer(nil)
		a, b := httpExporter(srvA), httpExporter(srvB)
		h := newLoopHarness(1)
		h.loop.SetExporter(ctx, a, 10*time.Second, 1000)
		h.start()
		h.stepUntilRequest(srvA, 10)

		h.loop.SetExporter(ctx, a, 20*time.Second, 1000)
		h.loop.SetExporter(ctx, b, 10*time.Second, 1000)
		Expect(a.Export(ctx, batch(1, 1))).To(Succeed(), "the exporter in use stays up until the next tick")

		for i := 0; len(srvB.Requests()) == 0; i++ {
			Expect(i).To(BeNumerically("<", 10), "no export to the new exporter within one interval")
			h.step(time.Second)
		}
		Expect(a.Export(ctx, batch(1, 1))).To(MatchError(export.ErrExporterShutdown))
	})

	It("shuts down the last exporter when Run returns", func(ctx SpecContext) {
		srv := newHTTPServer(nil)
		e := httpExporter(srv)
		h := newLoopHarness(1)
		h.loop.SetExporter(ctx, e, 10*time.Second, 1000)
		h.start()
		h.stepUntilRequest(srv, 10)

		h.stop()
		Expect(e.Export(ctx, batch(1, 1))).To(MatchError(export.ErrExporterShutdown))
	})

	Context("with a request cap of 64 KiB", func() {
		BeforeEach(func() { DeferCleanup(export.SetMaxRequestSize(64<<10, 64<<10)) })

		for _, protocol := range []string{settings.ProtocolHTTP, settings.ProtocolGRPC} {
			newSrv := func() *otlptest.Server {
				if protocol == settings.ProtocolGRPC {
					return newGRPCServer(nil)
				}
				return newHTTPServer(nil)
			}

			It("delivers a tick over the cap in parts with every point once ("+protocol+")", func() {
				srv := newSrv()
				h := newLoopHarness(1)
				h.prod.set(2000, nil)
				h.prod.size = 300
				h.loop.SetExporter(context.Background(), newExporter(exporterSettings(protocol, srv.URL)), 10*time.Second, 5000)
				h.start()

				for i := 0; i < 10 && h.exports("ok") == 0 && len(srv.Requests()) == 0; i++ {
					Eventually(h.clk.HasWaiters).Should(BeTrue())
					h.clk.Step(time.Second)
					Eventually(func() bool { return h.clk.HasWaiters() || len(srv.Requests()) > 0 }).Should(BeTrue())
				}
				Eventually(func() int { return pointsOf(srv.Requests()) }, 20*time.Second).Should(Equal(2000))
				Eventually(h.clk.HasWaiters, 20*time.Second).Should(BeTrue())

				Expect(len(srv.Requests())).To(BeNumerically(">=", 2))
				Expect(pointsOf(srv.Requests())).To(Equal(2000))
				Expect(h.exports("error")).To(BeZero())
				Expect(h.exports("ok")).To(Equal(float64(len(srv.Requests()))))
			})

			It("counts one point over the cap as request_too_large once ("+protocol+")", func() {
				srv := newSrv()
				h := newLoopHarness(1)
				h.prod.set(1, nil)
				h.prod.size = 70 << 10
				h.loop.SetExporter(context.Background(), newExporter(exporterSettings(protocol, srv.URL)), 10*time.Second, 5000)
				h.start()

				for i := 0; i < 10 && h.exports("error") == 0; i++ {
					h.step(time.Second)
				}

				Expect(h.exports("error")).To(Equal(1.0))
				Expect(h.exports("ok")).To(BeZero())
				Expect(srv.Requests()).To(BeEmpty())
			})
		}
	})
})
