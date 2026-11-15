package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/scheduler"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// ErrorLog is errorLog.
type ErrorLog = errorLog

// MaxMessages is how many distinct messages errorLog remembers per failing query and database.
const MaxMessages = maxMessages

// WatchDebounce is the quiet time after a file change before the engine reacts.
const WatchDebounce = sources.Debounce

// NewMeterProvider returns the engine's MeterProvider over reader.
func NewMeterProvider(reader sdkmetric.Reader) *sdkmetric.MeterProvider {
	return newMeterProvider(reader)
}

// FactsWait is the wait before the next facts round after failures consecutive failed ones.
func FactsWait(failures int, planInterval time.Duration) time.Duration {
	return factsWait(failures, planInterval)
}

// RunWithBrokenBookkeeping runs job n times through a scheduler over the engine's RunFunc with
// bookkeeping that panics, and returns the runs counted with outcome error and whether the job's
// snapshot entry is still there.
func RunWithBrokenBookkeeping(ctx context.Context, job planner.Job, n int) (errorRuns int64, kept bool, err error) {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return 0, false, err
	}
	s := settings.Settings{
		Postgres: settings.Postgres{Host: "127.0.0.1", Port: 1, User: "u", Database: "app", MaxConnections: 2},
		Limits:   settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
	}
	e.swapPool(ctx, s, time.Minute)
	// record dereferences the replanner first, so a nil one makes it panic.
	e.plan = nil
	key := snapshot.Key{Query: job.Query, Database: job.Database}
	sample := queries.Sample{Metric: "cnpg_" + job.Query + "_value", Kind: queries.KindGauge, Value: 1}
	if err := e.store.Put(key, job.Fingerprint, []queries.Sample{sample}, time.Now()); err != nil {
		return 0, false, errors.Join(err, e.mp.Shutdown(ctx))
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var runs atomic.Int32
	e.sched = scheduler.New(e.clk, e.newRand(), func(c context.Context, j planner.Job) {
		defer func() {
			if int(runs.Add(1)) == n {
				cancel()
			}
		}()
		e.runJob(c, j)
	}, func(planner.Job, int) {})
	job.Interval = 10 * time.Millisecond
	e.sched.Update(planner.Plan{Jobs: []planner.Job{job}})
	if err := e.sched.Run(rctx); err != nil {
		return 0, false, errors.Join(err, e.mp.Shutdown(ctx))
	}

	points, _ := e.store.Points()
	for _, p := range points {
		kept = kept || p.Key == key
	}
	errorRuns, err = queryRuns(ctx, e, job, selfmetrics.OutcomeError)
	if r := e.setRunner(nil); r != nil {
		r.pool.Close()
	}
	return errorRuns, kept, errors.Join(err, e.mp.Shutdown(ctx))
}

// RecordResult records res for its planned job, whose snapshot entry holds one sample, and reports
// whether that entry is still there and the runs counted with res's outcome.
func RecordResult(ctx context.Context, res executor.Result) (kept bool, runs int64, err error) {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return false, 0, err
	}
	job := res.Job
	key := snapshot.Key{Query: job.Query, Database: job.Database}
	e.plan.planned = map[snapshot.Key]string{key: job.Fingerprint}
	sample := queries.Sample{Metric: "cnpg_" + job.Query + "_value", Kind: queries.KindGauge, Value: 1}
	if err := e.store.Put(key, job.Fingerprint, []queries.Sample{sample}, time.Now()); err != nil {
		return false, 0, errors.Join(err, e.mp.Shutdown(ctx))
	}

	e.record(ctx, res)
	points, _ := e.store.Points()
	for _, p := range points {
		kept = kept || p.Key == key
	}
	runs, err = queryRuns(ctx, e, job, res.Outcome)
	return kept, runs, errors.Join(err, e.mp.Shutdown(ctx))
}

// queryRuns returns cnpg_otel_query_runs_total for job with outcome, from one collection of e.
func queryRuns(ctx context.Context, e *engine, job planner.Job, outcome selfmetrics.Outcome) (int64, error) {
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(ctx, &rm); err != nil {
		return 0, err
	}
	want := attribute.NewSet(attribute.String("query", job.Query), attribute.String("database", job.Database),
		attribute.String("outcome", string(outcome)))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != "cnpg_otel_query_runs_total" || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					return dp.Value, nil
				}
			}
		}
	}
	return 0, nil
}
