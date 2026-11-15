package engine

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
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

// RunWithBrokenBookkeeping runs job n times through the scheduler's RunFunc with bookkeeping that
// panics, and returns the runs counted with outcome error.
func RunWithBrokenBookkeeping(ctx context.Context, job planner.Job, n int) (int64, error) {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return 0, err
	}
	s := settings.Settings{
		Postgres: settings.Postgres{Host: "127.0.0.1", Port: 1, User: "u", Database: "app", MaxConnections: 2},
		Limits:   settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
	}
	e.swapPool(ctx, s, time.Minute)
	// record dereferences the replanner first, so a nil one makes it panic.
	e.plan = nil
	for range n {
		e.runJob(ctx, job)
	}

	var rm metricdata.ResourceMetrics
	err = e.reader.Collect(ctx, &rm)
	var runs int64
	want := attribute.NewSet(attribute.String("query", job.Query), attribute.String("database", job.Database),
		attribute.String("outcome", string(selfmetrics.OutcomeError)))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != "cnpg_otel_query_runs_total" || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					runs = dp.Value
				}
			}
		}
	}
	if r := e.setRunner(nil); r != nil {
		r.pool.Close()
	}
	return runs, errors.Join(err, e.mp.Shutdown(ctx))
}
