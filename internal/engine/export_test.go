package engine

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
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

// collect returns the metric called name from one collection of e, and whether it was there.
func (e *engine) collect(ctx context.Context, name string) (metricdata.Metrics, bool, error) {
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(ctx, &rm); err != nil {
		return metricdata.Metrics{}, false, err
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true, nil
			}
		}
	}
	return metricdata.Metrics{}, false, nil
}

// CollectorUp records one facts round per entry of rounds, true for a success, and returns
// cnpg_collector_up as the engine reports it for cluster "c", and whether it is reported at all.
func CollectorUp(ctx context.Context, rounds ...bool) (metricdata.Metrics, bool, error) {
	e, err := newEngine(ctx, Options{ConfigPath: "engine.yaml"})
	if err != nil {
		return metricdata.Metrics{}, false, err
	}
	cluster := "c"
	e.cluster.Store(&cluster)
	for _, ok := range rounds {
		e.setUp(ok)
	}
	m, found, err := e.collect(ctx, upMetric)
	return m, found, errors.Join(err, e.mp.Shutdown(ctx))
}

// unreachable is a configuration whose pool never connects: nothing listens on port 1.
func unreachable() settings.Settings {
	return settings.Settings{
		Postgres: settings.Postgres{Host: "127.0.0.1", Port: 1, User: "u", Database: "app", MaxConnections: 2},
		Limits:   settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
	}
}

// RunWithBrokenBookkeeping runs job n times through the scheduler's RunFunc with bookkeeping that
// panics, and returns the runs counted with outcome error.
func RunWithBrokenBookkeeping(ctx context.Context, job planner.Job, n int) (int64, error) {
	e, err := newEngine(ctx, Options{ConfigPath: "engine.yaml"})
	if err != nil {
		return 0, err
	}
	e.setRunner(ctx, unreachable(), time.Minute)
	// record dereferences the replanner first, so a nil one makes it panic.
	e.plan = nil
	for range n {
		e.runJob(ctx, job)
	}

	m, _, err := e.collect(ctx, "cnpg_otel_query_runs_total")
	var runs int64
	if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
		want := attribute.NewSet(attribute.String("query", job.Query), attribute.String("database", job.Database),
			attribute.String("outcome", string(selfmetrics.OutcomeError)))
		for _, dp := range sum.DataPoints {
			if dp.Attributes.Equals(&want) {
				runs = dp.Value
			}
		}
	}
	return runs, errors.Join(err, e.mp.Shutdown(ctx))
}

// Pools changes the engine's runner as configuration changes do, without PostgreSQL.
type Pools struct{ e *engine }

// NewPools returns Pools over a new engine.
func NewPools(ctx context.Context) (*Pools, error) {
	e, err := newEngine(ctx, Options{ConfigPath: "engine.yaml"})
	if err != nil {
		return nil, err
	}
	return &Pools{e: e}, nil
}

// Unreachable is a configuration whose pool never connects.
func Unreachable() settings.Settings { return unreachable() }

// Apply makes the runner match s and reports whether the pool was replaced.
func (p *Pools) Apply(ctx context.Context, s settings.Settings) bool {
	return p.e.setRunner(ctx, s, time.Minute)
}

// Begin starts a run on the current runner as runJob does; it returns the run's pool and the function that ends it.
func (p *Pools) Begin() (*pg.Pool, func()) {
	r, done := p.e.takeRunner()
	return r.pool.Pool, done
}

// Close shuts the engine's meter provider down.
func (p *Pools) Close(ctx context.Context) error { return p.e.mp.Shutdown(ctx) }

// WatchSwapNotifies watches first, then second, and reports whether the swap asked for a look at the files.
func WatchSwapNotifies(ctx context.Context, first, second []string) (bool, error) {
	e, err := newEngine(ctx, Options{ConfigPath: "engine.yaml"})
	if err != nil {
		return false, err
	}
	defer e.stopWatch()
	e.watch(ctx, first)
	select {
	case <-e.changed:
	default:
	}
	e.watch(ctx, second)
	if e.watcher == nil || len(e.watcher.files) != len(second) {
		return false, errors.Join(errors.New("the watcher was not swapped"), e.mp.Shutdown(ctx))
	}
	var notified bool
	select {
	case <-e.changed:
		notified = true
	default:
	}
	return notified, e.mp.Shutdown(ctx)
}

// FactsWait is the wait before the next facts round after failures consecutive failed ones.
func FactsWait(failures int, planInterval time.Duration) time.Duration {
	return factsWait(failures, planInterval)
}

// factsStep is the step by which FactsRounds moves its fake clock.
const factsStep = 100 * time.Millisecond

// FactsRounds drives the facts rounds of an engine whose PostgreSQL never answers, on a fake clock,
// as the control goroutine does.
type FactsRounds struct {
	e   *engine
	clk *clocktesting.FakeClock
}

// NewFactsRounds returns FactsRounds after a first configuration with the planning cycle planInterval.
func NewFactsRounds(ctx context.Context, planInterval time.Duration) (*FactsRounds, error) {
	clk := clocktesting.NewFakeClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	e, err := newEngine(ctx, Options{ConfigPath: "engine.yaml", Clock: clk})
	if err != nil {
		return nil, err
	}
	s := unreachable()
	s.PlanInterval = planInterval
	e.plan.current = &configuration{settings: s}
	e.setRunner(ctx, s, time.Minute)
	e.setFactsTimer(0)
	return &FactsRounds{e: e, clk: clk}, nil
}

// Run moves the fake clock forward by d in steps of factsStep. At each step it runs the round the facts
// timer is due for or, when early is set and earlyFactsGap has passed since the last round, the early
// round a run that lost its connection asks for. It returns the times from the start of the rounds the
// timer drove, and the number of early rounds.
func (f *FactsRounds) Run(ctx context.Context, d time.Duration, early bool) ([]time.Duration, int) {
	start := f.clk.Now()
	var timed []time.Duration
	var earlyRounds int
	for elapsed := time.Duration(0); elapsed <= d; elapsed += factsStep {
		f.clk.SetTime(start.Add(elapsed))
		select {
		case <-timerC(f.e.factsTimer):
			timed = append(timed, elapsed)
			f.e.factsRound(ctx, true)
		default:
			if early && f.clk.Since(f.e.lastFacts) >= earlyFactsGap {
				f.e.requestFacts()
				<-f.e.early
				f.e.onEarlyFacts(ctx)
				earlyRounds++
			}
		}
	}
	return timed, earlyRounds
}

// ReplacePool applies the configuration in force with another unreachable port, as a reload does,
// and reports whether that replaced the pool.
func (f *FactsRounds) ReplacePool(ctx context.Context) (bool, error) {
	prev, before := f.e.plan.current, f.e.runner.pool
	s := prev.settings
	s.Postgres.Port++
	next := &configuration{settings: s}
	if err := f.e.apply(ctx, prev, next); err != nil {
		return false, err
	}
	f.e.plan.current = next
	return f.e.runner.pool != before, nil
}

// Close closes the pools and shuts the meter provider down.
func (f *FactsRounds) Close(ctx context.Context) error {
	stopTimer(&f.e.factsTimer)
	f.e.retired.Wait()
	f.e.runner.pool.Close()
	return f.e.mp.Shutdown(ctx)
}
