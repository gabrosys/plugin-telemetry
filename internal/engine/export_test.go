package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Masterminds/semver/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/scheduler"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

// RunWith is Run with gather in place of the facts round's PostgreSQL queries; nil means those queries.
func RunWith(ctx context.Context, o Options, gather func(context.Context, settings.Settings) (planner.Facts, error)) error {
	e, err := newEngine(ctx, o, gather)
	if err != nil {
		return err
	}
	e.run(ctx)
	return nil
}

// ErrorLog is errorLog.
type ErrorLog = errorLog

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

// RecordAll records results in order for their planned jobs on one engine.
func RecordAll(ctx context.Context, results ...executor.Result) error {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return err
	}
	for _, res := range results {
		e.plan.planned = map[snapshot.Key]string{{Query: res.Job.Query, Database: res.Job.Database}: res.Job.Fingerprint}
		e.record(ctx, res)
	}
	return e.mp.Shutdown(ctx)
}

// RecordFailureBetweenSuccesses records an ok run of a counter at t0, then failed, then an ok run at t0+1m
// with a higher value. It reports whether the job's series were exported after failed and the Start of
// the series after the last run.
func RecordFailureBetweenSuccesses(ctx context.Context, failed executor.Result) (exported bool, start, t0 time.Time, err error) {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return false, time.Time{}, time.Time{}, err
	}
	job := failed.Job
	key := snapshot.Key{Query: job.Query, Database: job.Database}
	e.plan.planned = map[snapshot.Key]string{key: job.Fingerprint}
	ok := func(value float64, started time.Time) executor.Result {
		sample := queries.Sample{Metric: "cnpg_" + job.Query + "_total", Kind: queries.KindCounter, Value: value}
		return executor.Result{Job: job, Outcome: selfmetrics.OutcomeOK, Samples: []queries.Sample{sample}, Started: started}
	}
	exportedKey := func() (found bool, started time.Time) {
		points, _ := e.store.Points()
		for _, p := range points {
			if p.Key == key {
				return true, p.Start
			}
		}
		return false, time.Time{}
	}

	t0 = time.Now()
	e.record(ctx, ok(10, t0))
	e.record(ctx, failed)
	exported, _ = exportedKey()
	e.record(ctx, ok(15, t0.Add(time.Minute)))
	_, start = exportedKey()
	return exported, start, t0, e.mp.Shutdown(ctx)
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

// FactsRoundOnStop runs one timed facts round whose gather stops the engine's context and fails, with
// cnpg_collector_up at 1 before it, and returns the value afterwards.
func FactsRoundOnStop(ctx context.Context) (up int64) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gather := func(context.Context, settings.Settings) (planner.Facts, error) {
		cancel()
		return planner.Facts{}, errors.New("acquire a connection: context canceled")
	}
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, gather)
	if err != nil {
		panic(fmt.Sprintf("FactsRoundOnStop: new engine: %v", err))
	}
	s := settings.Settings{
		Postgres:     settings.Postgres{Host: "127.0.0.1", Port: 1, User: "u", Database: "app", MaxConnections: 2},
		Limits:       settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
		PlanInterval: time.Minute,
	}
	e.swapPool(ctx, s, time.Minute)
	defer func() {
		if r := e.setRunner(nil); r != nil {
			r.pool.Close()
		}
	}()
	e.plan.current = &configuration{settings: s}
	e.up.Store(1)
	e.factsRound(ctx, true)
	return e.up.Load()
}

// FactsFailuresAfterPanicOnStop runs one timed facts round, with 3 failures counted before it, whose
// gather succeeds and stops the engine's context and whose replanning panics, and returns the count after.
func FactsFailuresAfterPanicOnStop(ctx context.Context) int {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gather := func(context.Context, settings.Settings) (planner.Facts, error) {
		cancel()
		return planner.Facts{Version: *semver.MustParse("18.0.7"), Databases: []string{"app"}}, nil
	}
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, gather)
	if err != nil {
		panic(fmt.Sprintf("FactsFailuresAfterPanicOnStop: new engine: %v", err))
	}
	s := settings.Settings{
		Postgres:     settings.Postgres{Host: "127.0.0.1", Port: 1, User: "u", Database: "app", MaxConnections: 2},
		Limits:       settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
		PlanInterval: time.Minute,
	}
	e.swapPool(ctx, s, time.Minute)
	defer func() {
		if r := e.setRunner(nil); r != nil {
			r.pool.Close()
		}
	}()
	e.plan.current = &configuration{settings: s}
	e.plan.update = func(planner.Plan) { panic("replan") }
	e.factsFailures = 3
	e.factsRound(ctx, true)
	return e.factsFailures
}

// IdleTimeoutAfterIdleOnlyApply applies a configuration, then one that differs in the idle timeout only,
// and reports whether the pool is the same and the idle timeout it has.
func IdleTimeoutAfterIdleOnlyApply(ctx context.Context) (kept bool, idle time.Duration, err error) {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return false, 0, err
	}
	s := settings.Settings{
		Postgres: settings.Postgres{Host: "127.0.0.1", Port: 1, User: "u", Database: "app", MaxConnections: 2},
		Limits:   settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
		Exporter: settings.Exporter{
			Protocol: "http/protobuf", Endpoint: "http://127.0.0.1:1/v1/metrics", Compression: "none",
			Interval: time.Second, Timeout: time.Second / 2, MaxPointsPerRequest: 100,
		},
		PlanInterval: time.Minute,
	}
	first := &configuration{settings: s, idleTimeout: time.Minute}
	second := &configuration{settings: s, idleTimeout: 2 * time.Minute}
	defer e.teardown()
	if err := e.apply(ctx, nil, first); err != nil {
		return false, 0, errors.Join(err, e.mp.Shutdown(ctx))
	}
	pool := e.runner.pool
	if err := e.apply(ctx, first, second); err != nil {
		return false, 0, errors.Join(err, e.mp.Shutdown(ctx))
	}
	return e.runner.pool == pool, pool.IdleTimeout(), e.mp.Shutdown(ctx)
}

// RunJobWhileRefused refuses a superuser session, then runs a job on the engine's runner, which a
// listener stands in for PostgreSQL of, and returns how many connections the listener accepted.
func RunJobWhileRefused(ctx context.Context) (connections int, err error) {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	var (
		accepted atomic.Int32
		closeErr error // written by the accept goroutine, read after done is closed
		done     = make(chan struct{})
	)
	go func() {
		defer close(done)
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			if err := c.Close(); err != nil {
				closeErr = fmt.Errorf("close accepted connection: %w", err)
				return
			}
		}
	}()
	defer func() {
		err = errors.Join(err, lis.Close())
		<-done
		err = errors.Join(err, closeErr)
	}()

	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return 0, err
	}
	addr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("listener address %v is not TCP", lis.Addr())
	}
	s := settings.Settings{
		Postgres:     settings.Postgres{Host: "127.0.0.1", Port: addr.AddrPort().Port(), User: "u", Database: "app", MaxConnections: 2},
		Limits:       settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second},
		PlanInterval: time.Minute,
	}
	e.swapPool(ctx, s, time.Minute)
	defer e.teardown()
	cur := &configuration{settings: s, idleTimeout: time.Minute}
	e.plan.current = cur
	e.refuse(ctx, cur, map[string]bool{"superuser": true})
	e.runJob(ctx, planner.Job{Query: "backends", Database: "app", Fingerprint: "f"})
	return int(accepted.Load()), e.mp.Shutdown(ctx)
}
