package export_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc/codes"
	testingclock "k8s.io/utils/clock/testing"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

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

	panics   int  // the next Produce calls that panic
	numbered bool // one point whose value is the number of the call
	calls    int
	size     int // bytes of each of the 8 labels of a point; 0 gives one small label
}

func (p *fixedProducer) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.panics > 0 {
		p.panics--
		panic("producer broke")
	}
	p.calls++
	if p.numbered {
		rm := batch(1, 1)
		rm.ScopeMetrics[0].Metrics[0].Data = metricdata.Gauge[float64]{DataPoints: []metricdata.DataPoint[float64]{{
			Attributes: attribute.NewSet(attribute.String("k", "v")), Time: time.Unix(1, 0), Value: float64(p.calls),
		}}}
		return rm.ScopeMetrics, p.err
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

	mu    sync.Mutex // guards lines
	lines []string   // the log lines of the loop
}

// logged returns the log lines so far.
func (h *loopHarness) logged() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.lines)
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
	logger := funcr.New(func(_, args string) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.lines = append(h.lines, args)
	}, funcr.Options{})
	ctx, h.cancel = context.WithCancel(ctrllog.IntoContext(context.Background(), logger))
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

// stepUntil steps the clock by one 10 s interval until cond holds, within a bound.
func (h *loopHarness) stepUntil(cond func() bool) {
	GinkgoHelper()
	Eventually(func() bool {
		if cond() {
			return true
		}
		h.step(10 * time.Second)
		return cond()
	}).WithTimeout(time.Minute).WithPolling(0).Should(BeTrue())
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
	return protocolAttrs(settings.ProtocolHTTP, srv, extra...)
}

func protocolAttrs(protocol string, srv *otlptest.Server, extra ...attribute.KeyValue) []attribute.KeyValue {
	GinkgoHelper()
	u, err := url.Parse(srv.URL)
	Expect(err).NotTo(HaveOccurred())
	port, err := strconv.Atoi(u.Port())
	Expect(err).NotTo(HaveOccurred())
	component := "otlp_http_metric_exporter"
	if protocol == settings.ProtocolGRPC {
		component = "otlp_grpc_metric_exporter"
	}
	return append([]attribute.KeyValue{
		attribute.String("otel.component.type", component),
		attribute.String("otel.component.name", component+"/0"),
		attribute.String("server.address", u.Hostname()),
		attribute.Int("server.port", port),
	}, extra...)
}

// idsOf returns the value of the "i" attribute of every delivered point.
func idsOf(reqs []otlptest.Request) []int {
	var ids []int
	for _, r := range reqs {
		for _, rm := range r.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					for _, dp := range m.GetGauge().GetDataPoints() {
						for _, kv := range dp.GetAttributes() {
							if kv.GetKey() == "i" {
								ids = append(ids, int(kv.GetValue().GetIntValue()))
							}
						}
					}
				}
			}
		}
	}
	return ids
}

// stepUntilTick steps the clock by step until srv has a request or an export failed,
// then waits for the loop to idle.
func (h *loopHarness) stepUntilTick(srv *otlptest.Server, step time.Duration) {
	GinkgoHelper()
	for i := 0; i < 300 && len(srv.Requests()) == 0 && h.exports("error") == 0; i++ {
		Eventually(h.clk.HasWaiters).Should(BeTrue())
		h.clk.Step(step)
		Eventually(func() bool { return h.clk.HasWaiters() || len(srv.Requests()) > 0 || h.exports("error") > 0 }).Should(BeTrue())
	}
	Eventually(h.clk.HasWaiters, 5*time.Second).Should(BeTrue())
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

	It("logs a long run of export errors once at the start and once at recovery, holding no growing state", func() {
		srv := newHTTPServer(nil)
		srv.Respond(otlptest.Response{HTTPStatus: 503})
		h := newLoopHarness(1)
		s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
		s.Timeout = 9*time.Second - time.Nanosecond // the retry budget is spent after the first attempt
		h.loop.SetExporter(context.Background(), newExporter(s), 10*time.Second, 1000)
		h.start()

		h.stepUntil(func() bool { return h.exports("error") >= 100 })
		srv.Respond(otlptest.Response{})
		h.stepUntil(func() bool { return h.exports("ok") > 0 })

		lines := h.logged()
		Expect(lines).To(HaveLen(2))
		Expect(lines[0]).To(And(ContainSubstring(`"msg"="export metrics"`), ContainSubstring(`"error.type"="503"`)))
		Expect(lines[1]).To(ContainSubstring(`"msg"="export recovered"`))
		reqs := srv.Requests()
		Expect(len(reqs)).To(BeNumerically(">", 100))
		for _, r := range reqs[1:] { // the first lacks the self-metrics a failed export adds
			Expect(r.Points()).To(Equal(reqs[1].Points()), "a request never grows with the failures before it")
		}
	})

	It("logs no recovery for a tick that panics, and one at the next good tick", func() {
		srv := newHTTPServer(nil)
		srv.Respond(otlptest.Response{HTTPStatus: 400})
		h := newLoopHarness(1)
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()
		recoveries := func() int {
			n := 0
			for _, l := range h.logged() {
				if strings.Contains(l, `"msg"="export recovered"`) {
					n++
				}
			}
			return n
		}

		h.stepUntil(func() bool { return h.exports("error") >= 1 })
		h.prod.mu.Lock()
		h.prod.panics = 1
		h.prod.mu.Unlock()
		srv.Respond(otlptest.Response{})
		h.stepUntil(func() bool { return h.exports("error") >= 2 })
		Expect(recoveries()).To(BeZero())

		h.stepUntil(func() bool { return h.exports("ok") > 0 })
		Expect(recoveries()).To(Equal(1))
		Expect(h.logged()).To(HaveLen(3), "the failure, the panic and the recovery")
	})

	It("logs a new line when the error type changes", func() {
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()

		for _, status := range []int{400, 400, 413, 400} {
			srv.Respond(otlptest.Response{HTTPStatus: status})
			want := h.exports("error") + 1
			h.stepUntil(func() bool { return h.exports("error") >= want })
		}
		lines := h.logged()
		Expect(lines).To(HaveLen(3))
		Expect(lines[0]).To(ContainSubstring(`"error.type"="400"`))
		Expect(lines[1]).To(ContainSubstring(`"error.type"="413"`))
		Expect(lines[2]).To(ContainSubstring(`"error.type"="400"`))
	})

	It("logs a collection failure and a rejection once each, and the recovery once", func() {
		srv := newHTTPServer(nil)
		h := newLoopHarness(1)
		h.prod.set(3, errors.New("producer failed"))
		h.loop.SetExporter(context.Background(), httpExporter(srv), 10*time.Second, 1000)
		h.start()

		h.stepUntil(func() bool { return h.exports("error") >= 3 })
		h.prod.set(3, nil)
		srv.Respond(otlptest.Response{Rejected: 1})
		h.stepUntil(func() bool { return len(srv.Requests()) >= 3 })
		srv.Respond(otlptest.Response{})
		sent := len(srv.Requests())
		h.stepUntil(func() bool { return len(srv.Requests()) > sent })

		lines := h.logged()
		Expect(lines).To(HaveLen(3))
		Expect(lines[0]).To(ContainSubstring(`"msg"="collect metrics"`))
		Expect(lines[1]).To(ContainSubstring(`"msg"="export partially rejected"`))
		Expect(lines[2]).To(ContainSubstring(`"msg"="export recovered"`))
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

	It("sends only the latest collection after an outage, holding nothing back", func() {
		srv := newHTTPServer(nil)
		srv.Respond(otlptest.Response{HTTPStatus: http.StatusServiceUnavailable})
		s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
		s.Interval, s.Timeout = time.Second, 400*time.Millisecond
		h := newLoopHarness(1)
		h.prod.numbered = true
		h.loop.SetExporter(context.Background(), newExporter(s), time.Second, 1000)
		h.start()

		for i := 0; i < 100 && h.exports("error") < 20; i++ {
			h.step(time.Second)
		}
		Expect(h.exports("error")).To(Equal(20.0))
		srv.Respond(otlptest.Response{})
		for i := 0; i < 100 && h.exports("ok") == 0; i++ {
			h.step(time.Second)
		}

		reqs := srv.Requests()
		Expect(h.exports("ok")).To(Equal(1.0))
		var values []float64
		for _, rm := range reqs[len(reqs)-1].Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() != "g" {
						continue
					}
					for _, dp := range m.GetGauge().GetDataPoints() {
						values = append(values, dp.GetAsDouble())
					}
				}
			}
		}
		Expect(values).To(Equal([]float64{21}))
	})

	Context("with a request cap of 16 KiB", func() {
		const points = 150
		BeforeEach(func() { DeferCleanup(export.SetMaxRequestSize(16<<10, 16<<10)) })

		for _, protocol := range []string{settings.ProtocolHTTP, settings.ProtocolGRPC} {
			// A fresh server and a harness whose tick carries 150 points of 2.4 KiB.
			setup := func(s settings.Exporter, interval time.Duration) (*otlptest.Server, *loopHarness) {
				var srv *otlptest.Server
				if protocol == settings.ProtocolGRPC {
					srv = newGRPCServer(nil)
				} else {
					srv = newHTTPServer(nil)
				}
				s.Protocol, s.Endpoint = protocol, srv.URL
				h := newLoopHarness(1)
				h.prod.set(points, nil)
				h.prod.size = 300
				h.loop.SetExporter(context.Background(), newExporter(s), interval, 5000)
				h.start()
				return srv, h
			}
			complete := func() []int {
				ids := make([]int, points)
				for i := range ids {
					ids[i] = i
				}
				return ids
			}

			It("delivers a tick over the cap in parts with every point once ("+protocol+")", func() {
				srv, h := setup(exporterSettings(protocol, ""), 10*time.Second)

				h.stepUntilTick(srv, time.Second)

				Expect(len(srv.Requests())).To(BeNumerically(">=", 4))
				Expect(idsOf(srv.Requests())).To(ConsistOf(complete()))
				Expect(h.exports("error")).To(BeZero())
				Expect(h.exports("ok")).To(Equal(float64(len(srv.Requests()))))
			})

			It("counts one point over the cap as request_too_large once ("+protocol+")", func() {
				srv, h := setup(exporterSettings(protocol, ""), 10*time.Second)
				h.prod.set(1, nil)
				h.prod.size = 70 << 10

				h.stepUntilTick(srv, time.Second)

				Expect(h.exports("error")).To(Equal(1.0))
				Expect(h.exports("ok")).To(BeZero())
				Expect(srv.Requests()).To(BeEmpty())
				Expect(h.value("otel.sdk.exporter.metric_data_point.exported",
					protocolAttrs(protocol, srv, attribute.String("error.type", "request_too_large"))...)).To(Equal(1.0))
			})

			It("delivers the other parts when one part fails ("+protocol+")", func() {
				srv, h := setup(exporterSettings(protocol, ""), 10*time.Second)
				// A 400 is not retried; a 503 would be retried within the tick and succeed.
				srv.Respond(otlptest.Response{HTTPStatus: http.StatusBadRequest, Code: codes.InvalidArgument}, otlptest.Response{})

				h.stepUntilTick(srv, time.Second)

				reqs := srv.Requests()
				Expect(len(reqs)).To(BeNumerically(">=", 4))
				Expect(h.exports("error")).To(Equal(1.0))
				Expect(h.exports("ok")).To(Equal(float64(len(reqs) - 1)))
				Expect(idsOf(reqs[1:])).To(ConsistOf(slices.DeleteFunc(complete(), func(i int) bool {
					return slices.Contains(idsOf(reqs[:1]), i)
				})))
			})

			It("stops splitting once the tick deadline has passed ("+protocol+")", func() {
				srv, h := setup(exporterSettings(protocol, ""), 200*time.Millisecond)
				srv.Respond(otlptest.Response{Delay: time.Second})

				h.stepUntilTick(srv, time.Millisecond)

				Expect(srv.Requests()).To(HaveLen(1))
				Expect(h.exports("ok")).To(BeZero())
				Expect(h.exports("error")).To(BeNumerically(">=", 2))
				Expect(h.exports("error")).To(BeNumerically("<=", 10)) // one per halving level, not one per part
			})
		}
	})
})
