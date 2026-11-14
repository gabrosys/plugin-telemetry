//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"go.yaml.in/yaml/v4"
	"k8s.io/utils/clock"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/export/otlptest"
	"github.com/gabrosys/plugin-telemetry/internal/featuregate"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/test/pgtest"
)

var server *pgtest.Postgres

var _ = BeforeSuite(func(ctx SpecContext) {
	image := os.Getenv("POSTGRES_IMAGE")
	Expect(image).NotTo(BeEmpty(), "POSTGRES_IMAGE must name the PostgreSQL image (make test-integration sets it)")
	pg.ClearEnv()
	export.ClearEnv()

	var err error
	server, err = pgtest.Start(ctx, image)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) {
		Expect(server.Terminate(ctx)).To(Succeed())
	})
}, NodeTimeout(5*time.Minute))

const (
	queryInterval  = time.Second
	exportInterval = time.Second
	// slack covers the work between the timers: the reload, the run and the request.
	slack = time.Second
)

// defaultKeys are CNPG v1.30.1's default queries planned on a PostgreSQL 18 primary, plus the builtin one.
var defaultKeys = []string{
	"backends", "backends_waiting", "collector", "pg_database", "pg_extensions", "pg_postmaster",
	"pg_replication", "pg_replication_slots", "pg_settings", "pg_stat_archiver", "pg_stat_bgwriter_17",
	"pg_stat_checkpointer", "pg_stat_database", "pg_stat_replication",
}

const extraQuery = `extra_probe:
  query: SELECT 42 AS value
  metrics:
    - value:
        usage: GAUGE
        description: A probe.
`

func writeFile(path, content string) {
	GinkgoHelper()
	Expect(os.WriteFile(path, []byte(content), 0o600)).To(Succeed())
}

// defaultQueries returns data.queries of CNPG v1.30.1's default-monitoring ConfigMap.
func defaultQueries() string {
	GinkgoHelper()
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	raw, err := os.ReadFile("../queries/testdata/default-monitoring-v1.30.1.yaml")
	Expect(err).NotTo(HaveOccurred())
	Expect(yaml.Unmarshal(raw, &cm)).To(Succeed())
	Expect(cm.Data).To(HaveKey("queries"))
	return cm.Data["queries"]
}

// fixture is a directory with an engine configuration, CNPG's default queries and an extra
// source file, next to an OTLP/HTTP receiver.
type fixture struct {
	config, extra string
	valid         string // a valid configuration for the receiver
	srv           *otlptest.Server
	done          chan error
}

func newFixture(planInterval time.Duration) *fixture {
	GinkgoHelper()
	dir := GinkgoT().TempDir()
	f := &fixture{
		config: filepath.Join(dir, "engine.yaml"),
		extra:  filepath.Join(dir, "extra.yaml"),
		srv:    otlptest.NewHTTP(nil),
	}
	DeferCleanup(f.srv.Close)
	writeFile(filepath.Join(dir, "default-queries.yaml"), defaultQueries())
	writeFile(f.extra, "")
	f.valid = fmt.Sprintf(`cluster: it
namespace: ns
postgres:
  host: %s
  port: %d
  user: %s
  passwordFile: %s
planInterval: %s
defaultInterval: %s
sources:
  - default-queries.yaml
  - extra.yaml
exporter:
  endpoint: %s/v1/metrics
  interval: %s
`, server.Host, server.Port, pgtest.MonitorUser, server.MonitorConfig(dir).PasswordFile,
		planInterval, queryInterval, f.srv.URL, exportInterval)
	return f
}

// run starts the engine and stops it at cleanup.
func (f *fixture) run() {
	GinkgoHelper()
	ctx, cancel := context.WithCancel(ctrllog.IntoContext(context.Background(), GinkgoLogr))
	f.done = make(chan error, 1)
	go func() {
		f.done <- engine.Run(ctx, engine.Options{
			ConfigPath: f.config,
			Gates:      featuregate.NewRegistry(),
			Clock:      clock.RealClock{},
			Rand:       rand.New(rand.NewPCG(1, 2)), //nolint:gosec // seeded jitter for the test
			Getenv:     func(string) string { return "" },
		})
	}()
	DeferCleanup(func() {
		cancel()
		Eventually(f.done).WithTimeout(20 * time.Second).Should(Receive(BeNil()))
	})
}

// point is one exported data point.
type point struct {
	attrs map[string]string
	value float64
	at    time.Time
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

// points returns the data points of the metric called name in the requests received at or after since, in order.
func points(srv *otlptest.Server, name string, since time.Time) []point {
	var out []point
	for _, req := range srv.Requests() {
		if req.Received.Before(since) {
			continue
		}
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() != name {
						continue
					}
					dps := append(m.GetGauge().GetDataPoints(), m.GetSum().GetDataPoints()...)
					for _, dp := range dps {
						out = append(out, point{attrs: attrMap(dp.GetAttributes()), value: numberValue(dp), at: req.Received})
					}
				}
			}
		}
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

func matches(attrs, want map[string]string) bool {
	for k, v := range want {
		if attrs[k] != v {
			return false
		}
	}
	return true
}

func expectUp(srv *otlptest.Server, want float64) func(Gomega) {
	return func(g Gomega) {
		up, ok := last(srv, "cnpg_collector_up", map[string]string{"cluster": "it"})
		g.Expect(ok).To(BeTrue(), "cnpg_collector_up{cluster=it} was not exported; got %d requests with %v",
			len(srv.Requests()), metricNames(srv))
		g.Expect(up).To(Equal(want))
	}
}

var _ = Describe("Run", Ordered, func() {
	var f *fixture

	BeforeAll(func() {
		f = newFixture(time.Second)
		writeFile(f.config, f.valid)
		f.run()
	})

	It("exports CNPG's default queries, cnpg_collector_up and the self-metrics", func() {
		Eventually(func(g Gomega) {
			expectUp(f.srv, 1)(g)
			for _, name := range []string{
				"cnpg_backends_total", "cnpg_pg_database_size_bytes", "cnpg_pg_postmaster_start_time",
				"cnpg_pg_settings_setting", "cnpg_pg_stat_database_xact_commit", "cnpg_pg_replication_in_recovery",
				"cnpg_collector_lo_pages",
			} {
				g.Expect(points(f.srv, name, time.Time{})).NotTo(BeEmpty(), name)
			}
			for _, q := range defaultKeys {
				runs, ok := last(f.srv, "cnpg_otel_query_runs_total", map[string]string{"query": q, "outcome": "ok"})
				g.Expect(ok).To(BeTrue(), q)
				g.Expect(runs).To(BeNumerically(">=", 1), q)
			}
			for _, name := range []string{"cnpg_otel_config_info", "cnpg_otel_series", "cnpg_otel_query_interval_seconds", "go.goroutine.count"} {
				g.Expect(points(f.srv, name, time.Time{})).NotTo(BeEmpty(), name)
			}
		}).WithTimeout(30 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())

		for _, p := range points(f.srv, "cnpg_otel_query_runs_total", time.Time{}) {
			Expect(p.attrs["outcome"]).NotTo(BeElementOf("error", "timeout"), "%v", p.attrs)
		}
		info := points(f.srv, "cnpg_otel_config_info", time.Time{})
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "1"))
		req := f.srv.Requests()[0]
		Expect(attrMap(req.Metrics.GetResourceMetrics()[0].GetResource().GetAttributes())).To(And(
			HaveKeyWithValue("service.name", "it"), HaveKeyWithValue("service.namespace", "ns")))
	})

	It("exports a query appended to a source file within debounce + interval + export interval", func() {
		bound := engine.WatchDebounce + queryInterval + exportInterval + slack
		start := time.Now()
		writeFile(f.extra, extraQuery)

		Eventually(func() []point { return points(f.srv, "cnpg_extra_probe_value", start) }).
			WithTimeout(bound).WithPolling(50 * time.Millisecond).ShouldNot(BeEmpty())
		first := points(f.srv, "cnpg_extra_probe_value", start)[0]
		Expect(first.value).To(Equal(42.0))
		Expect(first.at.Sub(start)).To(BeNumerically("<=", bound))
		info := points(f.srv, "cnpg_otel_config_info", start)
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "2"))
	})

	It("keeps the metric of a source that becomes invalid and counts a load error", func() {
		writeFile(f.extra, extraQuery+"broken: [\n")
		Eventually(func(g Gomega) {
			errs, ok := last(f.srv, "cnpg_otel_config_loads_total", map[string]string{"source": f.extra, "outcome": "error"})
			g.Expect(ok).To(BeTrue())
			g.Expect(errs).To(BeNumerically(">=", 1))
		}).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		since := time.Now()
		Eventually(func() []point { return points(f.srv, "cnpg_extra_probe_value", since) }).
			WithTimeout(5 * time.Second).ShouldNot(BeEmpty())
		info := points(f.srv, "cnpg_otel_config_info", since)
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "2"))
	})
})

// resourceHas reports whether a request received at or after since has a resource attribute key=value.
func resourceHas(srv *otlptest.Server, since time.Time, key, value string) bool {
	for _, req := range srv.Requests() {
		if req.Received.Before(since) {
			continue
		}
		for _, rm := range req.Metrics.GetResourceMetrics() {
			if attrMap(rm.GetResource().GetAttributes())[key] == value {
				return true
			}
		}
	}
	return false
}

var _ = Describe("Run on a settings change", Ordered, func() {
	var (
		f      *fixture
		second *otlptest.Server
		valid  string // f.valid pointed at second
	)

	BeforeAll(func() {
		f = newFixture(time.Second)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		second = otlptest.NewHTTP(nil)
		DeferCleanup(second.Close)
		valid = strings.Replace(f.valid, f.srv.URL, second.URL, 1)
	})

	It("sends to a new endpoint and stops sending to the old one", func() {
		writeFile(f.config, valid)
		Eventually(expectUp(second, 1)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		info := points(second, "cnpg_otel_config_info", time.Time{})
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "2"))

		n := len(f.srv.Requests())
		Consistently(func() int { return len(f.srv.Requests()) }).WithTimeout(3 * time.Second).Should(Equal(n))
	})

	It("connects with changed postgres settings", func() {
		port := fmt.Sprintf("port: %d\n", server.Port)
		Expect(valid).To(ContainSubstring(port))
		writeFile(f.config, strings.Replace(valid, port, "port: 1\n", 1))
		Eventually(expectUp(second, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		writeFile(f.config, valid)
		Eventually(expectUp(second, 1)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})

	It("exports with changed resource attributes", func() {
		since := time.Now()
		writeFile(f.config, valid+"resourceAttributes:\n  team: db\n")
		Eventually(func() bool { return resourceHas(second, since, "team", "db") }).
			WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())
		after := time.Now()
		Eventually(func() []point { return points(second, "cnpg_backends_total", after) }).
			WithTimeout(5 * time.Second).ShouldNot(BeEmpty())
	})
})

var _ = Describe("Run when the exporter files change", func() {
	It("sends the new headers without reloading the configuration", func() {
		f := newFixture(time.Second)
		headers := filepath.Join(filepath.Dir(f.config), "headers")
		writeFile(headers, "x-probe=one")
		writeFile(f.config, f.valid+"  headersFile: headers\n")
		f.run()

		header := func(value string) func() bool {
			return func() bool {
				reqs := f.srv.Requests()
				return len(reqs) > 0 && reqs[len(reqs)-1].Header.Get("X-Probe") == value
			}
		}
		Eventually(header("one")).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

		writeFile(headers, "x-probe=two")
		Eventually(header("two")).WithTimeout(engine.WatchDebounce + exportInterval + slack + time.Second).
			WithPolling(50 * time.Millisecond).Should(BeTrue())
		info := points(f.srv, "cnpg_otel_config_info", time.Time{})
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "1"))
	})
})

const slowQuery = `slow_probe:
  query: SELECT 7 AS value FROM pg_sleep(1.5)
  metrics:
    - value:
        usage: GAUGE
        description: Slower than its interval.
`

// requestsSince counts the requests received at or after since, and those that carry the metric called name.
func requestsSince(srv *otlptest.Server, since time.Time, name string) (total, with int) {
	for _, req := range srv.Requests() {
		if req.Received.Before(since) {
			continue
		}
		total++
		found := false
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					found = found || m.GetName() == name
				}
			}
		}
		if found {
			with++
		}
	}
	return total, with
}

var _ = Describe("Run with a query slower than its interval", func() {
	It("keeps the fresh entry of each slow run and counts the skips", func() {
		f := newFixture(time.Second)
		writeFile(filepath.Join(filepath.Dir(f.config), "default-queries.yaml"), "")
		writeFile(f.extra, slowQuery)
		writeFile(f.config, f.valid)
		f.run()

		Eventually(func(g Gomega) {
			skips, ok := last(f.srv, "cnpg_otel_query_runs_total", map[string]string{"query": "slow_probe", "outcome": "skipped"})
			g.Expect(ok).To(BeTrue())
			g.Expect(skips).To(BeNumerically(">=", 2))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		since := time.Now()
		Eventually(func() int {
			total, _ := requestsSince(f.srv, since, "cnpg_slow_probe_value")
			return total
		}).WithTimeout(10 * time.Second).Should(BeNumerically(">=", 3))
		total, with := requestsSince(f.srv, since, "cnpg_slow_probe_value")
		Expect(with).To(Equal(total))
	})
})

var _ = Describe("Run with an invalid configuration at start", func() {
	It("idles until the configuration is fixed", func() {
		f := newFixture(time.Second)
		writeFile(f.config, "cluster: it\nexporter: [\n")
		f.run()

		Consistently(f.srv.Requests).WithTimeout(3 * time.Second).Should(BeEmpty())
		Expect(f.done).NotTo(Receive())

		writeFile(f.config, f.valid)
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		errs, ok := last(f.srv, "cnpg_otel_config_loads_total", map[string]string{"source": f.config, "outcome": "error"})
		Expect(ok).To(BeTrue())
		Expect(errs).To(BeNumerically(">=", 1))
	})
})

var _ = Describe("Run while PostgreSQL is not up yet", func() {
	It("retries the facts with backoff and plans once PostgreSQL comes up", func(ctx SpecContext) {
		// Nothing is planned, so no run can ask for an early round, and the planning cycle is far
		// longer than the spec: only the facts backoff can bring cnpg_collector_up to 1.
		f := newFixture(time.Hour)
		writeFile(f.config, f.valid)
		Expect(server.StopServer(ctx)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) })
		f.run()
		Eventually(expectUp(f.srv, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StartServer(ctx)).To(Succeed())
		Eventually(expectUp(f.srv, 1)).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		since := time.Now()
		Eventually(func() []point { return points(f.srv, "cnpg_backends_total", since) }).
			WithTimeout(10 * time.Second).ShouldNot(BeEmpty())
	}, SpecTimeout(2*time.Minute))
})

var _ = Describe("Run when PostgreSQL stops", func() {
	It("gathers facts right after a run loses its connection", func(ctx SpecContext) {
		// The planning cycle is far longer than the spec: after a successful round only an early round, asked
		// for by a run that lost its connection, can bring cnpg_collector_up to 0. The way back to 1 comes from
		// the facts backoff or further early rounds, bounded by the pool's reconnect backoff.
		f := newFixture(time.Hour)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StopServer(ctx)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) })
		Eventually(expectUp(f.srv, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StartServer(ctx)).To(Succeed())
		Eventually(expectUp(f.srv, 1)).WithTimeout(45 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())
	}, SpecTimeout(2*time.Minute))
})

var _ = Describe("Run when PostgreSQL comes back after an outage", func() {
	It("reports cnpg_collector_up 1 within a planning cycle although the runs drove the reconnect backoff up", func(ctx SpecContext) {
		// The runs on the default database fail every second during the 17s outage: with a 30s cap their
		// reconnect backoff would hold every connect to that database until about 32s after the stop.
		planInterval := 5 * time.Second
		f := newFixture(planInterval)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StopServer(ctx)).To(Succeed())
		stopped := time.Now()
		DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) })
		Eventually(expectUp(f.srv, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		time.Sleep(time.Until(stopped.Add(17 * time.Second)))

		Expect(server.StartServer(ctx)).To(Succeed())
		back := time.Now()
		Eventually(expectUp(f.srv, 1)).WithTimeout(planInterval + exportInterval + 2*slack).
			WithPolling(100 * time.Millisecond).Should(Succeed())
		GinkgoWriter.Printf("cnpg_collector_up was 1 again %s after the restart\n", time.Since(back))
	}, SpecTimeout(2*time.Minute))
})
