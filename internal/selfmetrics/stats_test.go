package selfmetrics_test

import (
	"context"
	"slices"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

var _ = Describe("Stats", func() {
	var (
		stats  *selfmetrics.Stats
		reader *sdkmetric.ManualReader
	)

	BeforeEach(func() {
		stats = selfmetrics.NewStats()
		reader = sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithView(selfmetrics.Views()...),
		)
		DeferCleanup(provider.Shutdown)
		Expect(selfmetrics.Register(provider.Meter("test"), stats)).To(Succeed())
	})

	collect := func() map[string]metricdata.Metrics {
		GinkgoHelper()
		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(context.Background(), &rm)).To(Succeed())
		out := map[string]metricdata.Metrics{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				out[m.Name] = m
			}
		}
		return out
	}

	// points returns attribute set -> value for a counter or gauge.
	points := func(m metricdata.Metrics) map[string]float64 {
		GinkgoHelper()
		out := map[string]float64{}
		add := func(set attribute.Set, v float64) { out[set.Encoded(attribute.DefaultEncoder())] = v }
		switch d := m.Data.(type) {
		case metricdata.Sum[int64]:
			for _, p := range d.DataPoints {
				add(p.Attributes, float64(p.Value))
			}
		case metricdata.Gauge[float64]:
			for _, p := range d.DataPoints {
				add(p.Attributes, p.Value)
			}
		default:
			Fail("unexpected data type for " + m.Name)
		}
		return out
	}

	at := time.Unix(1_700_000_000, 0)

	It("reports every catalogue metric under exactly its attributes", func() {
		stats.QueryRun("q", "app", selfmetrics.OutcomeOK, 1500*time.Millisecond, at)
		stats.QueryCapped("q", "app", 120)
		stats.SeriesDropped("q", "app", "cap", 3)
		stats.QueryInterval("q", 30*time.Second)
		stats.Series(42)
		stats.ConfigLoad("file", selfmetrics.OutcomeOK, 2)
		stats.ConfigGeneration(7, "abc")
		stats.SetChecks(map[string]bool{"pg_monitor": true})
		export := selfmetrics.ExportRecord{
			Outcome: selfmetrics.OutcomeOK, Duration: 20 * time.Millisecond, At: at, Points: 10,
			ComponentType: "otlp_grpc_metric_exporter", ServerAddress: "collector", ServerPort: 4317,
		}
		stats.Export(export)
		export.Outcome = selfmetrics.OutcomeError
		export.ErrorType = "timeout"
		stats.Export(export)

		got := collect()
		optional := []string{"error.type", "http.response.status_code"}
		for _, m := range selfmetrics.ByComponent(selfmetrics.ComponentEngine) {
			Expect(got).To(HaveKey(m.Name))
			var sets []attribute.Set
			switch d := got[m.Name].Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			}
			Expect(sets).NotTo(BeEmpty(), m.Name)
			Expect(got[m.Name].Unit).To(Equal(m.Unit), m.Name)
			Expect(got[m.Name].Description).To(Equal(m.Description), m.Name)
			for _, set := range sets {
				keys := []string{}
				for _, kv := range set.ToSlice() {
					keys = append(keys, string(kv.Key))
				}
				for _, a := range m.Attributes {
					if !slices.Contains(optional, a) {
						Expect(keys).To(ContainElement(a), m.Name)
					}
				}
				for _, k := range keys {
					Expect(m.Attributes).To(ContainElement(k), m.Name)
				}
			}
		}
	})

	It("records query values", func() {
		stats.QueryRun("q", "app", selfmetrics.OutcomeOK, 1500*time.Millisecond, at)
		stats.QueryRun("q", "app", selfmetrics.OutcomeOK, 500*time.Millisecond, at.Add(time.Minute))
		stats.QueryRun("q", "app", selfmetrics.OutcomeError, time.Second, at.Add(2*time.Minute))
		stats.QueryRun("q", "app", selfmetrics.OutcomeSkipped, 0, at.Add(3*time.Minute))
		stats.QueryCapped("q", "app", 120)
		stats.SeriesDropped("q", "app", "cap", 3)
		stats.SeriesDropped("q", "app", "cap", 2)
		stats.QueryInterval("q", 30*time.Second)
		stats.Series(42)

		got := collect()
		Expect(points(got["cnpg_otel_query_runs_total"])).To(Equal(map[string]float64{
			"database=app,outcome=ok,query=q":      2,
			"database=app,outcome=error,query=q":   1,
			"database=app,outcome=skipped,query=q": 1,
		}))
		Expect(points(got["cnpg_otel_query_last_duration_seconds"])).To(Equal(map[string]float64{"database=app,query=q": 1}))
		Expect(points(got["cnpg_otel_query_last_success_timestamp_seconds"])).
			To(Equal(map[string]float64{"database=app,query=q": float64(at.Add(time.Minute).Unix())}))
		Expect(points(got["cnpg_otel_query_capped_series"])).To(Equal(map[string]float64{"database=app,query=q": 120}))
		Expect(points(got["cnpg_otel_query_dropped_series_total"])).
			To(Equal(map[string]float64{"database=app,query=q,reason=cap": 5}))
		Expect(points(got["cnpg_otel_query_interval_seconds"])).To(Equal(map[string]float64{"query=q": 30}))
		Expect(points(got["cnpg_otel_series"])).To(Equal(map[string]float64{"": 42}))
	})

	It("counts a denied load under outcome=denied", func() {
		stats.ConfigLoad("configmap", selfmetrics.OutcomeDenied, 0)
		stats.ConfigLoad("configmap", selfmetrics.OutcomeDenied, 0)

		Expect(points(collect()["cnpg_otel_config_loads_total"])).To(Equal(map[string]float64{
			"outcome=denied,source=configmap": 2,
		}))
	})

	It("counts a run that was denied under outcome=denied", func() {
		stats.QueryRun("q", "app", selfmetrics.OutcomeDenied, time.Second, at)

		got := collect()
		Expect(points(got["cnpg_otel_query_runs_total"])).To(Equal(map[string]float64{
			"database=app,outcome=denied,query=q": 1,
		}))
		Expect(got).NotTo(HaveKey("cnpg_otel_query_last_success_timestamp_seconds"))
	})

	It("exports cnpg_otel_check{check} as 1 or 0 after each facts round and stops exporting a check that is gone", func() {
		Expect(collect()).NotTo(HaveKey("cnpg_otel_check"))

		stats.SetChecks(map[string]bool{"pg_monitor": true, "read_only": false, "track_io_timing": true})
		Expect(points(collect()["cnpg_otel_check"])).To(Equal(map[string]float64{
			"check=pg_monitor": 1, "check=read_only": 0, "check=track_io_timing": 1,
		}))

		stats.SetChecks(map[string]bool{"pg_monitor": false})
		Expect(points(collect()["cnpg_otel_check"])).To(Equal(map[string]float64{"check=pg_monitor": 0}))
	})

	It("counts a reload error under outcome=error", func() {
		stats.ConfigLoad("file", selfmetrics.OutcomeOK, 0)
		stats.ConfigLoad("file", selfmetrics.OutcomeError, 0)
		stats.ConfigLoad("file", selfmetrics.OutcomeError, 0)
		stats.ConfigLoad("configmap", selfmetrics.OutcomeOK, 3)
		stats.ConfigGeneration(12, "deadbeef")

		got := collect()
		Expect(points(got["cnpg_otel_config_loads_total"])).To(Equal(map[string]float64{
			"outcome=ok,source=file":      1,
			"outcome=error,source=file":   2,
			"outcome=ok,source=configmap": 1,
		}))
		Expect(points(got["cnpg_otel_config_duplicate_queries_total"])).To(Equal(map[string]float64{
			"source=file": 0, "source=configmap": 3,
		}))
		Expect(points(got["cnpg_otel_config_info"])).To(Equal(map[string]float64{"generation=12,hash=deadbeef": 1}))
	})

	It("records exports", func() {
		base := selfmetrics.ExportRecord{
			Duration: 20 * time.Millisecond, At: at, Points: 10,
			ComponentType: "otlp_grpc_metric_exporter", ServerAddress: "collector", ServerPort: 4317,
		}
		ok := base
		ok.Outcome = selfmetrics.OutcomeOK
		ok.Rejected = 4
		stats.Export(ok)
		failed := base
		failed.Outcome = selfmetrics.OutcomeError
		failed.ErrorType = "unavailable"
		stats.Export(failed)
		skipped := base
		skipped.Outcome = selfmetrics.OutcomeSkipped
		stats.Export(skipped)

		got := collect()
		Expect(points(got["cnpg_otel_exports_total"])).To(Equal(map[string]float64{
			"outcome=ok": 1, "outcome=error": 1, "outcome=skipped": 1,
		}))
		Expect(points(got["cnpg_otel_last_export_success_timestamp_seconds"])).
			To(Equal(map[string]float64{"": float64(at.Unix())}))

		const common = "otel.component.name=otlp_grpc_metric_exporter/0,otel.component.type=otlp_grpc_metric_exporter," +
			"server.address=collector,server.port=4317"
		Expect(points(got["otel.sdk.exporter.metric_data_point.exported"])).To(Equal(map[string]float64{
			common:                             6,
			"error.type=rejected," + common:    4,
			"error.type=unavailable," + common: 10,
		}))

		hist, isHist := got["otel.sdk.exporter.operation.duration"].Data.(metricdata.Histogram[float64])
		Expect(isHist).To(BeTrue())
		Expect(hist.DataPoints).To(HaveLen(2))
		for _, p := range hist.DataPoints {
			Expect(p.Bounds).To(BeEmpty())
			Expect(p.BucketCounts).To(HaveLen(1))
			Expect(p.Count).To(Equal(uint64(1)))
			Expect(p.Sum).To(BeNumerically("~", 0.02))
		}
	})

	It("moves the export heartbeat only when the backend accepted at least one point", func() {
		heartbeat := func() map[string]float64 {
			GinkgoHelper()
			m, found := collect()["cnpg_otel_last_export_success_timestamp_seconds"]
			if !found {
				return nil
			}
			return points(m)
		}
		export := func(at time.Time, pts, rejected int) {
			stats.Export(selfmetrics.ExportRecord{
				Outcome: selfmetrics.OutcomeOK, At: at, Points: pts, Rejected: rejected,
				ComponentType: "otlp_http_metric_exporter", ServerAddress: "collector", ServerPort: 4318,
			})
		}

		export(at, 5, 5)
		Expect(heartbeat()).To(BeEmpty(), "a request with every point rejected")
		export(at.Add(time.Minute), 5, 4)
		Expect(heartbeat()).To(Equal(map[string]float64{"": float64(at.Add(time.Minute).Unix())}))
		export(at.Add(2*time.Minute), 5, 5)
		Expect(heartbeat()).To(Equal(map[string]float64{"": float64(at.Add(time.Minute).Unix())}))
		Expect(points(collect()["cnpg_otel_exports_total"])).To(Equal(map[string]float64{"outcome=ok": 3}))
	})

	It("does not move the last success timestamp on a capped run", func() {
		stats.QueryRun("q", "app", selfmetrics.OutcomeOK, time.Second, at)
		stats.QueryRun("q", "app", selfmetrics.OutcomeCapped, time.Second, at.Add(time.Hour))

		got := collect()
		Expect(points(got["cnpg_otel_query_last_success_timestamp_seconds"])).
			To(Equal(map[string]float64{"database=app,query=q": float64(at.Unix())}))
	})

	It("stops reporting the capped series of a query that recovered", func() {
		capped := func() map[string]float64 {
			GinkgoHelper()
			return points(collect()["cnpg_otel_query_capped_series"])
		}
		for _, o := range []selfmetrics.Outcome{selfmetrics.OutcomeOK, selfmetrics.OutcomeError, selfmetrics.OutcomeTimeout} {
			stats.QueryCapped("q", "app", 5)
			stats.QueryRun("q", "app", selfmetrics.OutcomeCapped, time.Second, at)
			stats.QueryCapped("q", "other", 7)
			stats.QueryRun("q", "other", selfmetrics.OutcomeCapped, time.Second, at)
			Expect(capped()).To(Equal(map[string]float64{"database=app,query=q": 5, "database=other,query=q": 7}), string(o))

			stats.QueryRun("q", "app", o, time.Second, at)
			Expect(capped()).To(Equal(map[string]float64{"database=other,query=q": 7}), string(o))
		}
	})

	It("keeps the capped series across a skipped run, which is only counted", func() {
		stats.QueryCapped("q", "app", 5)
		stats.QueryRun("q", "app", selfmetrics.OutcomeCapped, time.Second, at)
		stats.QueryRun("q", "app", selfmetrics.OutcomeSkipped, 0, at)
		Expect(points(collect()["cnpg_otel_query_capped_series"])).To(Equal(map[string]float64{"database=app,query=q": 5}))
	})

	It("emits the HTTP status code on the duration only when non-zero", func() {
		base := selfmetrics.ExportRecord{
			Outcome: selfmetrics.OutcomeOK, Duration: time.Millisecond, At: at, Points: 1,
			ComponentType: "otlp_http_metric_exporter", ServerAddress: "collector", ServerPort: 4318,
		}
		withCode := base
		withCode.HTTPStatusCode = 200
		stats.Export(base)
		stats.Export(withCode)

		hist, isHist := collect()["otel.sdk.exporter.operation.duration"].Data.(metricdata.Histogram[float64])
		Expect(isHist).To(BeTrue())
		Expect(hist.DataPoints).To(HaveLen(2))
		var withKey, withoutKey int
		for _, p := range hist.DataPoints {
			if v, ok := p.Attributes.Value("http.response.status_code"); ok {
				Expect(v.AsInt64()).To(Equal(int64(200)))
				withKey++
			} else {
				withoutKey++
			}
		}
		Expect([]int{withKey, withoutKey}).To(Equal([]int{1, 1}))
	})

	It("drops the series of queries not kept", func() {
		for _, q := range []string{"gone", "kept"} {
			stats.QueryRun(q, "app", selfmetrics.OutcomeOK, time.Second, at)
			stats.QueryCapped(q, "app", 5)
			stats.SeriesDropped(q, "app", "cap", 1)
			stats.QueryInterval(q, time.Minute)
		}
		stats.Series(9)
		stats.ConfigLoad("file", selfmetrics.OutcomeOK, 0)

		stats.Retain(map[selfmetrics.QueryDatabase]bool{{Query: "kept", Database: "app"}: true})

		got := collect()
		for _, name := range []string{
			"cnpg_otel_query_runs_total",
			"cnpg_otel_query_last_duration_seconds",
			"cnpg_otel_query_last_success_timestamp_seconds",
			"cnpg_otel_query_interval_seconds",
			"cnpg_otel_query_capped_series",
			"cnpg_otel_query_dropped_series_total",
		} {
			for key := range points(got[name]) {
				Expect(key).To(ContainSubstring("query=kept"), name)
				Expect(key).NotTo(ContainSubstring("query=gone"), name)
			}
			Expect(points(got[name])).NotTo(BeEmpty(), name)
		}
		Expect(points(got["cnpg_otel_series"])).To(HaveLen(1))
		Expect(points(got["cnpg_otel_config_loads_total"])).To(HaveLen(1))
	})

	It("drops the series of a database that leaves the plan and keeps the interval of a planned query", func() {
		for _, db := range []string{"app", "gone"} {
			stats.QueryRun("q", db, selfmetrics.OutcomeOK, time.Second, at)
			stats.QueryRun("q", db, selfmetrics.OutcomeError, time.Second, at)
			stats.QueryCapped("q", db, 5)
			stats.SeriesDropped("q", db, "cap", 1)
		}
		stats.QueryInterval("q", time.Minute)
		stats.QueryInterval("unplanned", time.Minute)

		stats.Retain(map[selfmetrics.QueryDatabase]bool{{Query: "q", Database: "app"}: true})

		got := collect()
		for _, name := range []string{
			"cnpg_otel_query_runs_total",
			"cnpg_otel_query_last_duration_seconds",
			"cnpg_otel_query_last_success_timestamp_seconds",
			"cnpg_otel_query_capped_series",
			"cnpg_otel_query_dropped_series_total",
		} {
			Expect(points(got[name])).NotTo(BeEmpty(), name)
			for key := range points(got[name]) {
				Expect(key).To(ContainSubstring("database=app,"), name)
			}
		}
		Expect(points(got["cnpg_otel_query_interval_seconds"])).To(Equal(map[string]float64{"query=q": 60}))
	})

	It("is safe for concurrent use", func() {
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				for range 100 {
					stats.QueryRun("q", "app", selfmetrics.OutcomeOK, time.Millisecond, at)
					stats.Export(selfmetrics.ExportRecord{Outcome: selfmetrics.OutcomeOK, Duration: time.Millisecond, Points: i})
					stats.Retain(map[selfmetrics.QueryDatabase]bool{{Query: "q", Database: "app"}: true})
					collect()
				}
			}()
		}
		wg.Wait()
	})
})

var _ = Describe("Register", func() {
	newMeter := func() (*sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
		GinkgoHelper()
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithView(selfmetrics.Views()...),
		)
		DeferCleanup(provider.Shutdown)
		return reader, provider
	}

	It("fails at start for a catalogue entry without a reader", func() {
		_, provider := newMeter()
		ms := append(selfmetrics.ByComponent(selfmetrics.ComponentEngine), selfmetrics.Metric{
			Component:  selfmetrics.ComponentEngine,
			Name:       "cnpg_otel_test_orphan",
			Instrument: string(selfmetrics.InstrumentCounter),
		})

		err := selfmetrics.RegisterMetrics(provider.Meter("test"), selfmetrics.NewStats(), ms)

		Expect(err).To(MatchError(ContainSubstring("metric cnpg_otel_test_orphan: no reader")))
	})

	It("collects the engine catalogue without an error", func() {
		reader, provider := newMeter()
		stats := selfmetrics.NewStats()
		Expect(selfmetrics.Register(provider.Meter("test"), stats)).To(Succeed())

		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(context.Background(), &rm)).To(Succeed())
	})
})
