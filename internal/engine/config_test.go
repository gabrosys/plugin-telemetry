package engine_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math/big"
	mathrand "math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
	"github.com/gabrosys/plugin-telemetry/test/otlptest"
)

// memConfig is an engine.Config held in memory. Its paths exist nowhere on disk.
type memConfig struct {
	mu          sync.Mutex // guards every field below
	doc         string
	noConfig    string // when set, Settings fails with ErrNoConfiguration for this reason
	notReady    string // when set, Settings fails with ErrNotReady for this reason
	files       map[string]string
	fileErrs    map[string]error
	reads       map[string]int
	calls       int             // of Settings
	watchErr    []error         // returned at once by the next Watch calls, one each
	panics      int             // the next Settings calls that panic
	watchPanics int             // the next Watch calls that panic
	watches     int             // Watch calls
	watched     func() []string // the files Watch was given
	notify      func()          // the notify Watch was given
}

func newMemConfig(doc string, files map[string]string) *memConfig {
	return &memConfig{doc: doc, files: files, fileErrs: map[string]error{}, reads: map[string]int{}}
}

func (c *memConfig) Name() string { return "mem" }

func (c *memConfig) Settings() (settings.Settings, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.panics > 0 {
		c.panics--
		panic("settings broke")
	}
	if c.noConfig != "" {
		return settings.Settings{}, nil, fmt.Errorf("%w: %s", engine.ErrNoConfiguration, c.noConfig)
	}
	if c.notReady != "" {
		return settings.Settings{}, nil, fmt.Errorf("%w: %s", engine.ErrNotReady, c.notReady)
	}
	s, err := settings.Parse([]byte(c.doc), "/mem")
	return s, []byte(c.doc), err
}

func (c *memConfig) ReadFile(name string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads[name]++
	if err := c.fileErrs[name]; err != nil {
		return nil, fmt.Errorf("get %s: %w", name, err)
	}
	data, ok := c.files[name]
	if !ok {
		return nil, fmt.Errorf("read %s: %w", name, fs.ErrNotExist)
	}
	return []byte(data), nil
}

// Watch returns the next error of watchErr, if any; else it records files and notify, notifies once
// and waits for ctx to end.
func (c *memConfig) Watch(ctx context.Context, files func() []string, notify func()) error {
	c.mu.Lock()
	c.watches++
	if c.watchPanics > 0 {
		c.watchPanics--
		defer c.mu.Unlock()
		panic("watch broke badly")
	}
	if len(c.watchErr) > 0 {
		err := c.watchErr[0]
		c.watchErr = c.watchErr[1:]
		c.mu.Unlock()
		return err
	}
	c.watched, c.notify = files, notify
	c.mu.Unlock()
	notify()
	<-ctx.Done()
	return nil
}

func (c *memConfig) watching() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.notify != nil
}

// Notify tells the engine that something changed, as a watch does.
func (c *memConfig) Notify() {
	c.mu.Lock()
	notify := c.notify
	c.mu.Unlock()
	notify()
}

// Files returns the files the engine asks the watch to cover now.
func (c *memConfig) Files() []string {
	c.mu.Lock()
	files := c.watched
	c.mu.Unlock()
	return files()
}

func (c *memConfig) watchCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watches
}

func (c *memConfig) set(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f()
}

func (c *memConfig) settingsCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *memConfig) readCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads[name]
}

func (c *memConfig) readNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.reads))
}

// panickyError is an error whose text can't be read.
type panickyError struct{}

func (panickyError) Error() string { panic("error text broke") }

// memSettings exports over TLS to a port nothing listens on.
const memSettings = `cluster: mem
postgres:
  host: 127.0.0.1
  port: 1
  passwordFile: /mem/pw
defaultInterval: 30s
sources:
  - /mem/queries.yaml
exporter:
  endpoint: https://127.0.0.1:1/v1/metrics
  headersFile: /mem/headers
  tls:
    caFile: /mem/ca.pem
    certFile: /mem/tls.crt
    keyFile: /mem/tls.key
`

// httpSettings exports to endpoint every second, with the headers of /mem/headers.
func httpSettings(endpoint string) string {
	return fmt.Sprintf(`cluster: mem
postgres:
  host: 127.0.0.1
  port: 1
  passwordFile: /mem/pw
defaultInterval: 30s
sources:
  - /mem/queries.yaml
exporter:
  endpoint: %s/v1/metrics
  interval: 1s
  headersFile: /mem/headers
`, endpoint)
}

// selfSigned returns a certificate and its key, PEM encoded.
func selfSigned() (certPEM, keyPEM string) {
	GinkgoHelper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "mem"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	Expect(err).NotTo(HaveOccurred())
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

func memFiles() map[string]string {
	GinkgoHelper()
	cert, key := selfSigned()
	return map[string]string{
		"/mem/queries.yaml": alphaBeta,
		"/mem/pw":           "secret\n",
		"/mem/headers":      "authorization=Bearer x",
		"/mem/ca.pem":       cert,
		"/mem/tls.crt":      cert,
		"/mem/tls.key":      key,
	}
}

func httpFiles() map[string]string {
	return map[string]string{"/mem/queries.yaml": alphaBeta, "/mem/pw": "secret\n", "/mem/headers": "a=1"}
}

var memPrimary = planner.Facts{Version: *semver.MustParse("18.0.7"), Databases: []string{"app", "postgres"}}

// factsFunc answers the facts round numbered round, from 0.
type factsFunc func(ctx context.Context, round int) (planner.Facts, error)

func primary(context.Context, int) (planner.Facts, error) { return memPrimary, nil }

func unreachable(context.Context, int) (planner.Facts, error) {
	return planner.Facts{}, fmt.Errorf("acquire a connection: %w", fs.ErrNotExist)
}

// fastSettings is httpSettings with a planning cycle and a query interval of one second.
func fastSettings(endpoint string) string {
	return strings.Replace(httpSettings(endpoint), "defaultInterval: 30s\n", "defaultInterval: 1s\nplanInterval: 1s\n", 1)
}

// harness runs the engine through RunWith on a fake clock, exporting to an OTLP/HTTP receiver.
type harness struct {
	cfg *memConfig
	clk *clocktesting.FakeClock
	srv *otlptest.Server

	mu          sync.Mutex // guards lines, rounds and factsTimers
	lines       []string
	rounds      []time.Time     // when each facts round started
	factsTimers []time.Duration // what the engine armed its facts timer with, in order
}

// factsClock is the fake clock; it records each timer the engine's setFactsTimer creates.
type factsClock struct {
	*clocktesting.FakeClock
	h *harness
}

func (c factsClock) NewTimer(d time.Duration) clock.Timer {
	pcs := make([]uintptr, 8)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, "engine.(*engine).setFactsTimer") {
			c.h.mu.Lock()
			c.h.factsTimers = append(c.h.factsTimers, d)
			c.h.mu.Unlock()
			break
		}
		if !more {
			break
		}
	}
	return c.FakeClock.NewTimer(d)
}

// run starts the engine on the Config cfg returns for the receiver's URL; facts answers its facts rounds.
func run(cfg func(url string) *memConfig, facts factsFunc) *harness {
	GinkgoHelper()
	return runWith(cfg, facts, runOptions{})
}

// runOptions change what run gives the engine.
type runOptions struct {
	env    map[string]string              // what Getenv reads; unset variables are empty
	config func(*memConfig) engine.Config // the Config the engine runs on; nil means the memConfig
}

// runWith is run with o.
func runWith(cfg func(url string) *memConfig, facts factsFunc, o runOptions) *harness {
	GinkgoHelper()
	h := &harness{clk: clocktesting.NewFakeClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)), srv: otlptest.NewHTTP(nil)}
	DeferCleanup(h.srv.Close)
	h.cfg = cfg(h.srv.URL)
	logger := funcr.New(func(_, args string) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.lines = append(h.lines, args)
	}, funcr.Options{})
	gather := func(ctx context.Context, _ settings.Settings) (planner.Facts, error) {
		h.mu.Lock()
		round := len(h.rounds)
		h.rounds = append(h.rounds, h.clk.Now())
		h.mu.Unlock()
		return facts(ctx, round)
	}

	ctx, cancel := context.WithCancel(ctrllog.IntoContext(context.Background(), logger))
	done := make(chan error, 1)
	var config engine.Config = h.cfg
	if o.config != nil {
		config = o.config(h.cfg)
	}
	go func() {
		done <- engine.RunWith(ctx, engine.Options{
			Config: config,
			Clock:  factsClock{FakeClock: h.clk, h: h},
			Rand:   mathrand.New(mathrand.NewPCG(1, 2)), //nolint:gosec // seeded jitter for the test
			Getenv: func(name string) string { return o.env[name] },
		}, gather)
	}()
	DeferCleanup(func() {
		cancel()
		Eventually(done).WithTimeout(10 * time.Second).Should(Receive(BeNil()))
	})
	Eventually(h.cfg.watchCalls).Should(BeNumerically(">", 0))
	return h
}

// logged counts the log lines that contain text.
func (h *harness) logged(text string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, l := range h.lines {
		if strings.Contains(l, text) {
			n++
		}
	}
	return n
}

// loggedAll counts the log lines that contain every one of texts.
func (h *harness) loggedAll(texts ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, l := range h.lines {
		all := true
		for _, t := range texts {
			all = all && strings.Contains(l, t)
		}
		if all {
			n++
		}
	}
	return n
}

// roundTimes returns when each facts round started.
func (h *harness) roundTimes() []time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.rounds)
}

// advance moves the fake clock forward in steps of 100 ms until cond holds.
func (h *harness) advance(cond func() bool) {
	GinkgoHelper()
	h.advanceBy(100*time.Millisecond, cond)
}

// advanceBy moves the fake clock forward in steps of step until cond holds.
func (h *harness) advanceBy(step time.Duration, cond func() bool) {
	GinkgoHelper()
	Eventually(func() bool {
		h.clk.Step(step)
		return cond()
	}).WithTimeout(20 * time.Second).WithPolling(2 * time.Millisecond).Should(BeTrue())
}

// timersArmed returns the durations the engine armed its facts timer with so far.
func (h *harness) timersArmed() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.factsTimers)
}

// reload notifies the engine and returns once it has handled that notification.
func (h *harness) reload() {
	GinkgoHelper()
	Eventually(h.cfg.watching).Should(BeTrue())
	for range 2 {
		// The second notification starts only after the first one's reload is done.
		n := h.cfg.settingsCalls()
		h.cfg.Notify()
		Eventually(h.cfg.settingsCalls).Should(BeNumerically(">", n))
	}
}

// lastValue returns the last value of the series of name whose attributes include want.
func (h *harness) lastValue(name string, want map[string]string) (float64, bool) {
	return last(h.srv, name, want)
}

// has reports whether the last value of the series of name with want is value.
func (h *harness) has(name string, want map[string]string, value float64) func() bool {
	return func() bool {
		v, ok := h.lastValue(name, want)
		return ok && v == value
	}
}

// requests returns the requests received so far.
func (h *harness) requests() []otlptest.Request { return h.srv.Requests() }

// point is one exported data point.
type point struct {
	attrs map[string]string
	value float64
	at    time.Time
	start uint64 // StartTimeUnixNano
}

func attrMap(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		switch v := kv.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			out[kv.GetKey()] = v.StringValue
		case *commonpb.AnyValue_IntValue:
			out[kv.GetKey()] = strconv.FormatInt(v.IntValue, 10)
		default:
			out[kv.GetKey()] = kv.GetValue().String()
		}
	}
	return out
}

func numberValue(p *metricpb.NumberDataPoint) float64 {
	if v, ok := p.GetValue().(*metricpb.NumberDataPoint_AsInt); ok {
		return float64(v.AsInt)
	}
	return p.GetAsDouble()
}

// requestPoints returns the data points of the metric called name in req.
func requestPoints(req otlptest.Request, name string) []point {
	var out []point
	for _, rm := range req.Metrics.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if m.GetName() != name {
					continue
				}
				dps := append(m.GetGauge().GetDataPoints(), m.GetSum().GetDataPoints()...)
				for _, dp := range dps {
					out = append(out, point{
						attrs: attrMap(dp.GetAttributes()), value: numberValue(dp), at: req.Received, start: dp.GetStartTimeUnixNano(),
					})
				}
			}
		}
	}
	return out
}

// points returns the data points of the metric called name in the requests received at or after since, in order.
func points(srv *otlptest.Server, name string, since time.Time) []point {
	var out []point
	for _, req := range srv.Requests() {
		if req.Received.Before(since) {
			continue
		}
		out = append(out, requestPoints(req, name)...)
	}
	return out
}

// last returns the last value of the series of name whose attributes include want.
func last(srv *otlptest.Server, name string, want map[string]string) (float64, bool) {
	value, found := 0.0, false
	for _, p := range points(srv, name, time.Time{}) {
		if matches(p.attrs, want) {
			value, found = p.value, true
		}
	}
	return value, found
}

// metricMeta returns the unit and the description of the metric called name in the last request that has it.
func metricMeta(srv *otlptest.Server, name string) (unit, description string, found bool) {
	for _, req := range srv.Requests() {
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() == name {
						unit, description, found = m.GetUnit(), m.GetDescription(), true
					}
				}
			}
		}
	}
	return unit, description, found
}

// metricNames lists the names of the metrics received, sorted.
func metricNames(srv *otlptest.Server) []string {
	seen := map[string]bool{}
	for _, req := range srv.Requests() {
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					seen[m.GetName()] = true
				}
			}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// attrValues returns the value of the attribute key of each of pts.
func attrValues(pts []point, key string) []string {
	out := make([]string, 0, len(pts))
	for _, p := range pts {
		out = append(out, p.attrs[key])
	}
	return out
}

// values returns the value of each of pts.
func values(pts []point) []float64 {
	out := make([]float64, 0, len(pts))
	for _, p := range pts {
		out = append(out, p.value)
	}
	return out
}

func matches(attrs, want map[string]string) bool {
	for k, v := range want {
		if attrs[k] != v {
			return false
		}
	}
	return true
}

var _ = Describe("An engine on an in-memory Config", func() {
	It("hands a new idle timeout to the kept pool", func(ctx SpecContext) {
		kept, idle, err := engine.IdleTimeoutAfterIdleOnlyApply(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(kept).To(BeTrue())
		Expect(idle).To(Equal(2 * time.Minute))
	})

	generation := func(g string) map[string]string { return map[string]string{"generation": g} }

	It("reloads on a settings or sources change and not on identical bytes", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, unreachable)
		loads := map[string]string{"source": "mem", "outcome": "ok"}
		h.advance(h.has("cnpg_otel_config_loads_total", loads, 1))
		h.advance(h.has("cnpg_otel_config_info", generation("1"), 1))

		h.reload()
		since := len(h.requests())
		h.advance(func() bool { return len(h.requests()) > since+1 })
		value, _ := h.lastValue("cnpg_otel_config_loads_total", loads)
		Expect(value).To(Equal(1.0), "identical bytes load nothing")

		h.cfg.set(func() { h.cfg.doc += "# same settings, other bytes\n" })
		h.reload()
		h.advance(h.has("cnpg_otel_config_loads_total", loads, 2))
		Expect(h.logged(`"msg"="configuration applied"`)).To(Equal(1), "the same settings make no generation")

		h.cfg.set(func() { h.cfg.files["/mem/queries.yaml"] = alphaBeta + gamma })
		h.reload()
		h.advance(h.has("cnpg_otel_config_info", generation("2"), 1))

		h.cfg.set(func() { h.cfg.doc = strings.Replace(h.cfg.doc, "defaultInterval: 30s", "defaultInterval: 45s", 1) })
		h.reload()
		h.advance(h.has("cnpg_otel_config_info", generation("3"), 1))
		Expect(h.logged(`"msg"="configuration applied"`)).To(Equal(3))
	})

	It("keeps the pool when only the idle timeout changes", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, primary)
		h.advance(func() bool { return h.logged(`"msg"="plan updated"`) == 1 && len(h.timersArmed()) == 2 })
		timers, rounds := len(h.timersArmed()), len(h.roundTimes())

		h.cfg.set(func() { h.cfg.doc = strings.Replace(h.cfg.doc, "defaultInterval: 30s", "defaultInterval: 45s", 1) })
		h.reload()
		h.advance(h.has("cnpg_otel_query_interval_seconds", map[string]string{"query": "alpha"}, 45))
		Expect(h.timersArmed()).To(HaveLen(timers), "a kept pool arms no facts timer")
		Expect(h.roundTimes()).To(HaveLen(rounds), "a kept pool starts no round")

		h.cfg.set(func() { h.cfg.doc = strings.Replace(h.cfg.doc, "port: 1\n", "port: 2\n", 1) })
		h.reload()
		Eventually(func() int { return len(h.timersArmed()) }).Should(BeNumerically(">", timers), "a new pool arms the timer at once")
		Expect(h.timersArmed()[timers]).To(BeZero())
		h.advance(func() bool { return len(h.roundTimes()) > rounds })
	})

	It("keeps the last good configuration when the settings become invalid", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, primary)
		alpha := map[string]string{"query": "alpha"}
		h.advance(h.has("cnpg_otel_query_interval_seconds", alpha, 30))

		h.cfg.set(func() { h.cfg.doc = "cluster: \"\"\n" })
		h.reload()
		Expect(h.logged("invalid configuration, keeping the one in force")).To(Equal(1))
		since := time.Now()
		h.advance(func() bool { return len(points(h.srv, "cnpg_otel_query_interval_seconds", since)) > 0 })
		Expect(attrValues(points(h.srv, "cnpg_otel_config_info", since), "generation")).To(HaveEach("1"))
		Expect(attrValues(points(h.srv, "cnpg_otel_query_interval_seconds", since), "query")).To(ContainElement("alpha"))
	})

	It("logs a not-ready reason once at info, keeps the configuration in force and counts no load", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, primary)
		alpha := map[string]string{"query": "alpha"}
		h.advance(h.has("cnpg_otel_query_interval_seconds", alpha, 30))
		loads := map[string]string{"source": "mem"}
		before, _ := h.lastValue("cnpg_otel_config_loads_total", loads)

		for _, reason := range []string{"reason one", "reason one", "reason two", "reason two", "reason one"} {
			h.cfg.set(func() { h.cfg.notReady = reason })
			h.reload()
		}
		Expect(h.loggedAll("configuration not ready, waiting", "reason one")).To(Equal(2))
		Expect(h.loggedAll("configuration not ready, waiting", "reason two")).To(Equal(1))
		Expect(h.loggedAll("configuration not ready, waiting", `"level"=0`)).To(Equal(3), "logged at info")
		Expect(h.logged("invalid configuration")).To(BeZero())
		Expect(h.logged("no configuration")).To(BeZero())

		h.cfg.set(func() { h.cfg.notReady = "" })
		h.reload()
		h.advance(h.has("cnpg_otel_query_interval_seconds", alpha, 30))
		after, _ := h.lastValue("cnpg_otel_config_loads_total", loads)
		Expect(after).To(Equal(before), "a not-ready input counts no load, and the same settings load nothing")
		Expect(h.logged(`"msg"="configuration applied"`)).To(Equal(1), "the configuration in force was kept")
	})

	It("idles on ErrNoConfiguration, logs its reason once per change and resumes", func() {
		var hold atomic.Bool // set, a facts round waits for release
		release := make(chan struct{})
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(ctx context.Context, round int) (planner.Facts, error) {
				if hold.Load() {
					select {
					case <-release:
					case <-ctx.Done():
						return planner.Facts{}, ctx.Err()
					}
				}
				return primary(ctx, round)
			})
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "mem"}, 1))

		for _, reason := range []string{"reason one", "reason one", "reason two", "reason two", "reason one"} {
			h.cfg.set(func() { h.cfg.noConfig = reason })
			h.reload()
		}
		Expect(h.logged("reason one")).To(Equal(2))
		Expect(h.logged("reason two")).To(Equal(1))
		Expect(h.cfg.Files()).To(BeEmpty())

		idle := len(h.requests())
		for range 30 {
			h.clk.Step(100 * time.Millisecond)
		}
		Consistently(func() int { return len(h.requests()) }).WithTimeout(300*time.Millisecond).
			Should(BeNumerically("<=", idle+1), "the export loop stops; one export may have been in flight")

		hold.Store(true)
		h.cfg.set(func() { h.cfg.noConfig = "" })
		h.reload()
		resumed := len(h.requests())
		h.advance(func() bool { return len(h.requests()) >= resumed+2 })
		for _, req := range h.requests()[resumed:] {
			Expect(requestPoints(req, "cnpg_collector_up")).To(BeEmpty(), "the idle forgot cnpg_collector_up")
		}

		close(release)
		h.advance(func() bool {
			reqs := h.requests()
			up := requestPoints(reqs[len(reqs)-1], "cnpg_collector_up")
			return len(up) > 0 && up[0].value == 1
		})
		Expect(h.logged(`"msg"="configuration applied"`)).To(Equal(2))

		h.cfg.set(func() { h.cfg.noConfig = "reason one" })
		h.reload()
		Expect(h.logged("reason one")).To(Equal(3))
	})

	It("marks a denied source denied and keeps its last good version", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, primary)
		alpha := map[string]string{"query": "alpha"}
		h.advance(h.has("cnpg_otel_query_interval_seconds", alpha, 30))

		h.cfg.set(func() { h.cfg.fileErrs["/mem/queries.yaml"] = sources.ErrDenied })
		h.reload()
		h.advance(h.has("cnpg_otel_config_loads_total", map[string]string{"source": "/mem/queries.yaml", "outcome": "denied"}, 1))
		Expect(h.loggedAll("query source denied", `"source"="/mem/queries.yaml"`, `"usingPrevious"=true`)).To(Equal(1))
		since := time.Now()
		h.advance(func() bool { return len(points(h.srv, "cnpg_otel_query_interval_seconds", since)) > 0 })
		Expect(attrValues(points(h.srv, "cnpg_otel_query_interval_seconds", since), "query")).To(ContainElement("alpha"))
	})

	It("reads sources, the password, headers and TLS material through Config.ReadFile only", func() {
		h := run(func(string) *memConfig { return newMemConfig(memSettings, memFiles()) }, primary)
		h.advance(func() bool { return h.cfg.readCount("/mem/pw") > 0 })
		Expect(h.cfg.readNames()).To(ConsistOf(
			"/mem/queries.yaml", "/mem/pw", "/mem/headers", "/mem/ca.pem", "/mem/tls.crt", "/mem/tls.key"))
		Expect(h.logged("configuration applied")).To(Equal(1))
	})

	It("watches the files of the configuration in force", func() {
		h := run(func(string) *memConfig { return newMemConfig(memSettings, memFiles()) }, unreachable)
		Eventually(h.cfg.Files).Should(ConsistOf(
			"/mem/queries.yaml", "/mem/pw", "/mem/headers", "/mem/ca.pem", "/mem/tls.crt", "/mem/tls.key"))

		h.cfg.set(func() {
			h.cfg.files["/mem/other.yaml"] = gamma
			h.cfg.doc = strings.Replace(h.cfg.doc, "/mem/queries.yaml", "/mem/other.yaml", 1)
		})
		h.reload()
		Expect(h.logged("configuration applied")).To(Equal(2))
		Expect(h.cfg.Files()).To(ConsistOf(
			"/mem/other.yaml", "/mem/pw", "/mem/headers", "/mem/ca.pem", "/mem/tls.crt", "/mem/tls.key"))
	})

	It("logs a Watch that returns early and restarts it after 5 s", func() {
		h := run(func(url string) *memConfig {
			c := newMemConfig(httpSettings(url), httpFiles())
			c.watchErr = []error{errors.New("watch broke")}
			return c
		}, unreachable)
		Eventually(func() int { return h.logged("watch broke") }).Should(Equal(1))
		Eventually(func() int { return h.logged("configuration applied") }).Should(Equal(1), "the engine reloads without a watch")
		h.clk.Step(4900 * time.Millisecond)
		Consistently(h.cfg.watchCalls, 200*time.Millisecond).Should(Equal(1))
		h.clk.Step(100 * time.Millisecond)
		Eventually(h.cfg.watching).Should(BeTrue())
		Expect(h.cfg.watchCalls()).To(Equal(2))
	})

	It("keeps handling events after a reload panics", func() {
		h := run(func(url string) *memConfig {
			c := newMemConfig(httpSettings(url), httpFiles())
			c.panics = 1
			return c
		}, unreachable)
		Eventually(func() int { return h.logged("recovered from a panic") }).Should(Equal(1))
		h.reload()
		Eventually(func() int { return h.logged(`"msg"="configuration applied"`) }).Should(Equal(1))
	})

	It("runs the next facts round after factsWait(1) when a round panics, and keeps handling events", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(_ context.Context, round int) (planner.Facts, error) {
				if round == 0 {
					panic("gather broke")
				}
				return memPrimary, nil
			})
		h.advance(func() bool { return h.logged("recovered from a panic") == 1 })
		h.advance(func() bool { return len(h.roundTimes()) >= 2 })
		rounds := h.roundTimes()
		wait := engine.FactsWait(1, 30*time.Second)
		Expect(rounds[1].Sub(rounds[0])).To(And(BeNumerically(">=", wait), BeNumerically("<", wait+200*time.Millisecond)))
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "mem"}, 1))

		n := h.cfg.settingsCalls()
		h.reload()
		Expect(h.cfg.settingsCalls()).To(BeNumerically(">", n))
	})

	It("backs off a facts round that gathers but panics after its success path", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(context.Context, int) (planner.Facts, error) {
				return planner.Facts{Version: memPrimary.Version, Databases: memPrimary.Databases, ChecksErr: panickyError{}}, nil
			})
		h.advance(func() bool { return len(h.roundTimes()) >= 4 })
		gotGaps := gaps(h.roundTimes())
		for i, wait := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
			Expect(gotGaps[i]).To(And(BeNumerically(">=", wait), BeNumerically("<", wait+200*time.Millisecond)), "gap %d", i)
		}
	})

	It("sets cnpg_collector_up to 0 when a facts round panics", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(_ context.Context, round int) (planner.Facts, error) {
				if round > 0 {
					panic("gather broke")
				}
				return memPrimary, nil
			})
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "mem"}, 1))
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "mem"}, 0))
	})

	It("restarts a Watch that panics after 5 s", func() {
		h := run(func(url string) *memConfig {
			c := newMemConfig(httpSettings(url), httpFiles())
			c.watchPanics = 1
			return c
		}, unreachable)
		Eventually(func() int { return h.logged("recovered from a panic") }).Should(Equal(1))
		h.clk.Step(5 * time.Second)
		Eventually(h.cfg.watching).Should(BeTrue())
		Expect(h.cfg.watchCalls()).To(Equal(2))
	})

	It("logs a Watch that keeps returning early once, whatever its error text, and its recovery once", func() {
		h := run(func(url string) *memConfig {
			c := newMemConfig(httpSettings(url), httpFiles())
			c.watchErr = []error{errors.New("watch broke"), errors.New("watch broke again"), errors.New("watch broke")}
			return c
		}, unreachable)
		h.advanceBy(time.Second, func() bool { return h.cfg.watchCalls() == 4 })
		Eventually(h.cfg.watching).Should(BeTrue())
		Eventually(func() int { return h.logged("watching the configuration again") }).Should(Equal(1))
		Expect(h.logged(`"error"="watch broke"`)).To(Equal(1))
		Expect(h.logged(`"error"="watch broke again"`)).To(BeZero())
	})

	It("logs failed facts rounds once whatever their messages, and the first success after them", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(_ context.Context, round int) (planner.Facts, error) {
				if round < 6 {
					return planner.Facts{}, fmt.Errorf("acquire a connection: dial tcp 10.0.0.1:%d: refused", 40000+round)
				}
				return memPrimary, nil
			})
		h.advance(func() bool { return h.logged("gathered facts again") == 1 })
		Expect(h.logged(`"msg"="gather facts"`)).To(Equal(1))
	})

	It("logs unreadable live checks once until their error changes, and their recovery once", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(_ context.Context, round int) (planner.Facts, error) {
				f := memPrimary
				switch {
				case round < 3:
					f.ChecksErr = errors.New("check x broke")
				case round == 3:
					f.ChecksErr = errors.New("check y broke")
				}
				return f, nil
			})
		h.advanceBy(time.Second, func() bool { return h.logged("live checks readable again") == 1 })
		Expect(h.logged("some live checks could not be read")).To(Equal(2))
		Expect(h.logged(`"error"="check x broke"`)).To(Equal(1))
		Expect(h.logged(`"error"="check y broke"`)).To(Equal(1))
		Expect(len(h.roundTimes())).To(BeNumerically(">=", 5))
	})

	header := func(h *harness, value string) func() bool {
		return func() bool {
			reqs := h.requests()
			return len(reqs) > 0 && reqs[len(reqs)-1].Header.Get("a") == value
		}
	}

	It("rebuilds the exporter when its header file changes and keeps the generation", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, unreachable)
		h.advance(header(h, "1"))

		h.cfg.set(func() { h.cfg.files["/mem/headers"] = "a=2" })
		h.reload()
		h.advance(header(h, "2"))
		Expect(h.logged("reloaded the exporter files")).To(Equal(1))
		Expect(h.logged(`"msg"="configuration applied"`)).To(Equal(1))
		h.advance(h.has("cnpg_otel_config_info", generation("1"), 1))
		Expect(attrValues(points(h.srv, "cnpg_otel_config_info", time.Time{}), "generation")).To(HaveEach("1"))
	})

	It("keeps the exporter in force when a changed exporter file is invalid", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, unreachable)
		h.advance(header(h, "1"))

		h.cfg.set(func() { h.cfg.files["/mem/headers"] = "a" })
		h.reload()
		Expect(h.logged(`"msg"="reload the exporter files"`)).To(Equal(1))
		since := len(h.requests())
		h.advance(func() bool { return len(h.requests()) >= since+3 })
		for _, req := range h.requests()[since:] {
			Expect(req.Header.Get("a")).To(Equal("1"))
		}
		Expect(h.logged(`"msg"="reload the exporter files"`)).To(Equal(1))
	})
})
