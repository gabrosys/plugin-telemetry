package engine_test

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

var _ = Describe("A run result", func() {
	DescribeTable("asks for an early facts round only when the run lost its connection",
		func(ctx SpecContext, res executor.Result, early bool) {
			res.Job = planner.Job{Query: "backends", Database: "app", Fingerprint: "f"}
			got, err := engine.EarlyFactsAfter(ctx, res)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(early))
		},
		Entry("a refused password", executor.Result{
			Outcome: selfmetrics.OutcomeError, Err: &pgconn.PgError{Code: "28P01"}, ConnectionLost: true,
		}, true),
		Entry("a backoff", executor.Result{
			Outcome: selfmetrics.OutcomeError, Err: fmt.Errorf("acquire: %w", pg.ErrBackoff), ConnectionLost: true,
		}, true),
		Entry("a column mismatch", executor.Result{
			Outcome: selfmetrics.OutcomeError, Err: queries.ErrColumnMismatch,
		}, false),
		Entry("a statement timeout", executor.Result{
			Outcome: selfmetrics.OutcomeTimeout, Err: &pgconn.PgError{Code: "57014"},
		}, false),
		Entry("a success", executor.Result{Outcome: selfmetrics.OutcomeOK}, false),
	)
})

var _ = Describe("The meter provider", func() {
	It("exports 3000 series of one self-metric without the overflow attribute", func(ctx SpecContext) {
		reader := sdkmetric.NewManualReader()
		mp := engine.NewMeterProvider(reader)
		DeferCleanup(mp.Shutdown)
		stats := selfmetrics.NewStats()
		Expect(selfmetrics.Register(mp.Meter("test"), stats)).To(Succeed())
		for i := range 3000 {
			stats.QueryRun(fmt.Sprintf("q%04d", i), "app", selfmetrics.OutcomeOK, time.Millisecond, time.Now())
		}

		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(ctx, &rm)).To(Succeed())
		queries := map[string]bool{}
		for _, req := range export.Split(&rm, 2000) {
			for _, sm := range req.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "cnpg_otel_query_runs_total" {
						continue
					}
					sum, ok := m.Data.(metricdata.Sum[int64])
					Expect(ok).To(BeTrue())
					for _, dp := range sum.DataPoints {
						_, overflow := dp.Attributes.Value("otel.metric.overflow")
						Expect(overflow).To(BeFalse())
						q, _ := dp.Attributes.Value("query")
						queries[q.AsString()] = true
					}
				}
			}
		}
		Expect(queries).To(HaveLen(3000))
	})
})
