package engine

import (
	"context"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

// ErrorLog is errorLog.
type ErrorLog = errorLog

// MaxMessages is how many distinct messages errorLog remembers per failing query and database.
const MaxMessages = maxMessages

// WatchDebounce is the quiet time after a file change before the engine reacts.
const WatchDebounce = watchDebounce

// Replanner is the load and planning core without a watcher, a pool or an exporter.
type Replanner struct{ r *replanner }

// NewReplanner returns a Replanner for the settings file at path; update receives every new plan.
func NewReplanner(path string, stats *selfmetrics.Stats, store *snapshot.Store, update func(planner.Plan)) *Replanner {
	return &Replanner{r: newReplanner(path, stats, store, &errorLog{}, update, noComponents{})}
}

// Reload loads the settings and the sources and replans when they changed.
func (r *Replanner) Reload(ctx context.Context) error { return r.r.reload(ctx) }

// SetFacts replans when f differs from the last facts.
func (r *Replanner) SetFacts(ctx context.Context, f planner.Facts) { r.r.setFacts(ctx, f) }

type noComponents struct{}

func (noComponents) watch(context.Context, []string) {}

func (noComponents) apply(context.Context, *configuration, *configuration) error { return nil }

// EarlyFactsAfter records res for its job, planned, and reports whether that asks for an early facts round.
func EarlyFactsAfter(ctx context.Context, res executor.Result) (bool, error) {
	e, err := newEngine(ctx, Options{ConfigPath: "engine.yaml"})
	if err != nil {
		return false, err
	}
	e.plan.planned = map[snapshot.Key]string{{Query: res.Job.Query, Database: res.Job.Database}: res.Job.Fingerprint}
	e.record(ctx, res)
	var early bool
	select {
	case <-e.early:
		early = true
	default:
	}
	return early, e.mp.Shutdown(ctx)
}

// NewMeterProvider returns the engine's MeterProvider over reader.
func NewMeterProvider(reader sdkmetric.Reader) *sdkmetric.MeterProvider {
	return newMeterProvider(reader)
}
