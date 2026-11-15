// Package engine runs the metrics engine from a configuration file: it watches the files, plans the
// queries from live facts, runs them and exports the results.
package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/safe"
	"github.com/gabrosys/plugin-telemetry/internal/scheduler"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

const (
	// watchRetry is the wait before a Watch that returned early is started again.
	watchRetry = 5 * time.Second
	// connectTimeout bounds a PostgreSQL connection attempt.
	connectTimeout = 5 * time.Second
	// runGrace is the executor's margin over the query timeout.
	runGrace = 2 * time.Second
	// earlyFactsGap is the shortest time between a facts round and one a run that lost its connection asks for.
	earlyFactsGap = time.Second
	// shutdownTimeout bounds each shutdown step that can block.
	shutdownTimeout = 5 * time.Second

	// cardinalityLimit is the most series one instrument reports per collection (the SDK default is 2000).
	cardinalityLimit = 10000

	// upMetric and upDescription are CloudNativePG v1.30.1's (pg_collector.go:130-135), which sets no unit.
	upMetric      = "cnpg_collector_up"
	upDescription = "1 if PostgreSQL is up, 0 otherwise."
)

// ErrNilConfig is returned by Run when Options.Config is nil.
var ErrNilConfig = errors.New("no configuration given")

// errWatchEnded stands for the error of a Watch that returned nil before the engine stopped.
var errWatchEnded = errors.New("the watch ended before the engine stopped")

// errWatchPanicked stands for the error of a Watch that panicked.
var errWatchPanicked = errors.New("the watch panicked")

// Options configure Run.
type Options struct {
	Config Config              // where the settings and the files they name are read
	Clock  clock.Clock         // nil means the real clock
	Rand   *rand.Rand          // seeds the jitter of the scheduler and the export loop; nil means a random seed
	Getenv func(string) string // reads the downward-API variables of the resource; nil means os.Getenv
}

// Run blocks until ctx ends. An invalid configuration at start or on reload is logged;
// at start the engine idles until the settings become valid, later it keeps the last good ones.
// When the Config has nothing to run (ErrNoConfiguration) the engine stops everything and idles
// until it has.
func Run(ctx context.Context, o Options) error {
	return RunWith(ctx, o, nil)
}

// RunWith is Run with gather in place of the facts round's PostgreSQL queries; nil means those queries.
func RunWith(ctx context.Context, o Options, gather func(context.Context, settings.Settings) (planner.Facts, error)) error {
	if o.Config == nil {
		return ErrNilConfig
	}
	e, err := newEngine(ctx, o, gather)
	if err != nil {
		return err
	}
	e.run(ctx)
	return nil
}

// runner runs jobs on a pool with the executor built for one set of limits.
type runner struct {
	spec   poolSpec
	limits settings.Limits
	pool   *pg.Pool
	exec   *executor.Executor
}

// poolSpec is what a pool is built from besides the limits.
type poolSpec struct {
	postgres    settings.Postgres
	idleTimeout time.Duration
	maxBackoff  time.Duration // the planning cycle
}

func newPoolSpec(s settings.Settings, idle time.Duration) poolSpec {
	return poolSpec{postgres: s.Postgres, idleTimeout: idle, maxBackoff: s.PlanInterval}
}

// loopRun is a running export loop.
type loopRun struct {
	l      *export.Loop
	cancel context.CancelFunc
	done   chan struct{}
}

type engine struct {
	cfg    Config
	clk    clock.Clock
	rnd    *rand.Rand // control goroutine only
	getenv func(string) string
	gather func(context.Context, settings.Settings) (planner.Facts, error)

	stats  *selfmetrics.Stats
	store  *snapshot.Store
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
	sched  *scheduler.Scheduler
	plan   *replanner
	errs   errorLog

	up      atomic.Int64                   // 1 or 0 after the last facts round, -1 before the first
	cluster atomic.Pointer[string]         // the cluster label of upMetric; nil before a valid configuration
	server  atomic.Pointer[semver.Version] // from the last successful facts round
	files   atomic.Pointer[[]string]       // what the watch covers besides the settings
	changed chan struct{}                  // the watch saw a change
	early   chan struct{}                  // a run lost its connection

	mu     sync.Mutex // guards runner, which only the control goroutine writes
	runner *runner

	// The fields below belong to the control goroutine.
	loop          *loopRun
	factsTimer    clock.Timer
	earlyTimer    clock.Timer
	lastFacts     time.Time
	factsFailures int      // consecutive failed facts rounds the timer drove
	factsErrs     errorLog // the facts errors logged, keyed by the default database
	idleReason    string   // the ErrNoConfiguration reason last logged
}

func newEngine(ctx context.Context, o Options, gather func(context.Context, settings.Settings) (planner.Facts, error)) (*engine, error) {
	e := &engine{
		cfg:     o.Config,
		clk:     o.Clock,
		rnd:     o.Rand,
		getenv:  o.Getenv,
		gather:  gather,
		stats:   selfmetrics.NewStats(),
		store:   snapshot.New(0),
		changed: make(chan struct{}, 1),
		early:   make(chan struct{}, 1),
	}
	e.up.Store(-1)
	if e.clk == nil {
		e.clk = clock.RealClock{}
	}
	if e.rnd == nil {
		e.rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // jitter, not security
	}
	if e.getenv == nil {
		e.getenv = os.Getenv
	}
	if e.gather == nil {
		e.gather = e.gatherFacts
	}

	e.reader = sdkmetric.NewManualReader(sdkmetric.WithProducer(export.NewProducer(e.store, e.clk, e.duplicates)))
	e.mp = newMeterProvider(e.reader)
	meter := e.mp.Meter(export.ScopeName, metric.WithInstrumentationVersion(version.Version))
	if err := e.instrument(meter); err != nil {
		return nil, errors.Join(err, e.mp.Shutdown(ctx))
	}

	e.sched = scheduler.New(e.clk, e.newRand(), e.runJob, e.skipped)
	e.plan = newReplanner(o.Config, e.stats, e.store, &e.errs, e.sched.Update)
	return e, nil
}

// newMeterProvider returns the engine's MeterProvider over reader.
func newMeterProvider(reader sdkmetric.Reader) *sdkmetric.MeterProvider {
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(selfmetrics.Views()...),
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
		sdkmetric.WithCardinalityLimit(cardinalityLimit),
	)
}

// instrument registers the self-metrics, the Go runtime metrics and upMetric on meter's provider.
func (e *engine) instrument(meter metric.Meter) error {
	if err := selfmetrics.Register(meter, e.stats); err != nil {
		return fmt.Errorf("register self-metrics: %w", err)
	}
	if err := runtime.Start(runtime.WithMeterProvider(e.mp)); err != nil {
		return fmt.Errorf("start runtime metrics: %w", err)
	}
	_, err := meter.Int64ObservableGauge(upMetric,
		metric.WithDescription(upDescription),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			cluster, up := e.cluster.Load(), e.up.Load()
			if cluster == nil || up < 0 {
				return nil
			}
			o.Observe(up, metric.WithAttributes(attribute.String("cluster", *cluster)))
			return nil
		}))
	if err != nil {
		return fmt.Errorf("create %s: %w", upMetric, err)
	}
	return nil
}

// newRand returns a generator seeded from e.rnd. Call it on the control goroutine.
func (e *engine) newRand() *rand.Rand {
	return rand.New(rand.NewPCG(e.rnd.Uint64(), e.rnd.Uint64())) //nolint:gosec // jitter, not security
}

// run is the control goroutine: it reacts to changes, facts rounds and failed runs until ctx ends.
func (e *engine) run(ctx context.Context) {
	logger := log.FromContext(ctx)
	plain := logger.GetLogger()
	logger.Info("starting the metrics engine", "config", e.cfg.Name(), "version", version.Version)

	schedCtx, stopSched := context.WithCancel(ctx)
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		if err := e.sched.Run(schedCtx); err != nil {
			logger.Error(err, "run the scheduler")
		}
	}()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		e.watch(ctx)
	}()
	defer e.shutdown(ctx, stopSched, schedDone, watchDone)

	safe.Call(plain, "reload", func() { e.onChange(ctx) })
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.changed:
			safe.Call(plain, "reload", func() { e.onChange(ctx) })
		case <-timerC(e.factsTimer):
			e.timedFactsRound(ctx, plain)
		case <-e.early:
			safe.Call(plain, "early facts request", func() { e.onEarlyFacts(ctx) })
		case <-timerC(e.earlyTimer):
			safe.Call(plain, "early facts round", func() { e.factsRound(ctx, false) })
		}
	}
}

// timedFactsRound runs the facts round the timer drove. A round that panics counts as a failed one, so
// the timer is armed again.
func (e *engine) timedFactsRound(ctx context.Context, logger logr.Logger) {
	if !safe.Call(logger, "facts round", func() { e.factsRound(ctx, true) }) {
		return
	}
	if cur := e.plan.current; cur != nil {
		e.factsFailures++
		e.setFactsTimer(factsWait(e.factsFailures, cur.settings.PlanInterval))
	}
}

// watch runs the Config's watch until ctx ends, starting it again watchRetry after it returns early.
// A failing Watch is logged once per error text, and once when a Watch notifies again.
func (e *engine) watch(ctx context.Context) {
	logger := log.FromContext(ctx)
	files := func() []string {
		if f := e.files.Load(); f != nil {
			return *f
		}
		return nil
	}
	var wl watchLog
	notify := func() {
		if wl.recovered() {
			logger.Info("watching the configuration again")
		}
		e.notify()
	}
	for {
		var err error
		if safe.Call(logger.GetLogger(), "watch the configuration", func() { err = e.cfg.Watch(ctx, files, notify) }) {
			err = errWatchPanicked
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errWatchEnded
		}
		t := e.clk.NewTimer(watchRetry)
		if wl.failed(err) {
			logger.Error(err, "watch the configuration", "restartEvery", watchRetry)
		}
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
	}
}

// watchLog remembers the Watch error last logged.
type watchLog struct {
	mu      sync.Mutex // guards failing
	failing string     // empty while a Watch runs
}

// failed records err and reports whether it differs from the error last logged.
func (w *watchLog) failed(err error) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err.Error() == w.failing {
		return false
	}
	w.failing = err.Error()
	return true
}

// recovered forgets the error last logged and reports whether there was one.
func (w *watchLog) recovered() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	was := w.failing != ""
	w.failing = ""
	return was
}

// shutdown stops the export loop, the scheduler, the pool, the watch and the meter provider.
func (e *engine) shutdown(ctx context.Context, stopSched context.CancelFunc, schedDone, watchDone <-chan struct{}) {
	logger := log.FromContext(ctx)
	ctx = context.WithoutCancel(ctx)
	stopTimer(&e.factsTimer)
	stopTimer(&e.earlyTimer)

	e.stopLoop()
	stopSched()
	<-schedDone
	if r := e.setRunner(nil); r != nil {
		r.pool.Close()
	}
	<-watchDone

	sctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	if err := e.mp.Shutdown(sctx); err != nil {
		logger.Error(err, "shut down the meter provider")
	}
	logger.Info("metrics engine stopped")
}

// onChange reloads the configuration and logs why it was not applied.
func (e *engine) onChange(ctx context.Context) {
	err := e.reload(ctx)
	logger := log.FromContext(ctx)
	switch {
	case err == nil:
		if e.plan.current != nil {
			e.idleReason = ""
		}
	case errors.Is(err, ErrNoConfiguration):
		if reason := err.Error(); reason != e.idleReason {
			e.idleReason = reason
			logger.Info("no configuration, waiting for one", "config", e.cfg.Name(), "reason", reason)
		}
	case e.plan.current == nil:
		e.idleReason = ""
		logger.Error(err, "invalid configuration, waiting for a valid one", "config", e.cfg.Name())
	default:
		e.idleReason = ""
		logger.Error(err, "invalid configuration, keeping the one in force", "config", e.cfg.Name(), "generation", e.plan.generation)
	}
}

// apply sets up next in place of prev, which is nil the first time: it builds the exporter when its
// settings, its files or the resource changed, swaps the pool when the connection settings, the limits,
// the idle timeout or the planning cycle changed, and hands the exporter to the export loop. On error
// nothing has changed.
func (e *engine) apply(ctx context.Context, prev, next *configuration) error {
	s := next.settings
	resourceChanged := prev == nil || prev.settings.Cluster != s.Cluster || prev.settings.Namespace != s.Namespace ||
		!maps.Equal(prev.settings.ResourceAttributes, s.ResourceAttributes)
	var exp *export.Exporter
	if resourceChanged || prev.settings.Exporter != s.Exporter || prev.exporterDigest != next.exporterDigest {
		var err error
		if exp, err = export.NewExporter(ctx, s.Exporter, e.cfg.ReadFile); err != nil {
			return fmt.Errorf("create the exporter: %w", err)
		}
	}

	// Nothing below fails.
	cluster := s.Cluster
	e.cluster.Store(&cluster)
	e.store.SetMaxSeriesTotal(s.Limits.MaxSeriesTotal)
	newPool := e.runner == nil || e.runner.spec != newPoolSpec(s, next.idleTimeout) || e.runner.limits != s.Limits
	if newPool {
		e.swapPool(ctx, s, next.idleTimeout)
	}
	if exp != nil {
		if resourceChanged {
			// The loop takes the resource at creation; the old loop shuts its exporters down.
			e.startLoop(ctx, export.Resource(s, e.getenv))
		}
		e.loop.l.SetExporter(ctx, exp, s.Exporter.Interval, s.Exporter.MaxPointsPerRequest)
	}
	if newPool {
		e.factsFailures = 0
		e.setFactsTimer(0)
	}
	return nil
}

// swapPool makes a pool and an executor for s the runner and closes the old pool at once: its idle
// connections now, its busy ones when their runs release them.
func (e *engine) swapPool(ctx context.Context, s settings.Settings, idle time.Duration) {
	pc := PostgresConfig(s)
	pc.ReadFile = e.cfg.ReadFile
	pool := pg.NewPool(pc, s.Postgres.MaxConnections, idle, e.clk,
		pg.WithLogger(log.FromContext(ctx).GetLogger()), pg.WithMaxBackoff(s.PlanInterval))
	next := &runner{
		spec:   newPoolSpec(s, idle),
		limits: s.Limits,
		pool:   pool,
		exec:   executor.New(pool, s.Limits, e.serverVersion, e.clk),
	}
	if old := e.setRunner(next); old != nil {
		old.pool.Close()
	}
}

// setRunner makes r the runner and returns the one it replaces.
func (e *engine) setRunner(r *runner) *runner {
	e.mu.Lock()
	defer e.mu.Unlock()
	old := e.runner
	e.runner = r
	return old
}

// currentRunner returns the runner, nil without a configuration.
func (e *engine) currentRunner() *runner {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runner
}

// teardown stops the export loop, the facts rounds and the pool.
func (e *engine) teardown() {
	stopTimer(&e.factsTimer)
	stopTimer(&e.earlyTimer)
	e.stopLoop()
	e.cluster.Store(nil)
	e.up.Store(-1)
	e.server.Store(nil)
	e.factsFailures = 0
	e.factsErrs.retain(func(snapshot.Key) bool { return false })
	if r := e.setRunner(nil); r != nil {
		r.pool.Close()
	}
}

// PostgresConfig is the connection configuration the engine builds its pool from.
func PostgresConfig(s settings.Settings) pg.Config {
	return pg.Config{
		Host:           s.Postgres.Host,
		Port:           s.Postgres.Port,
		User:           s.Postgres.User,
		PasswordFile:   s.Postgres.PasswordFile,
		QueryTimeout:   s.Limits.QueryTimeout,
		LockTimeout:    s.Limits.LockTimeout,
		ConnectTimeout: connectTimeout,
	}
}

func (e *engine) serverVersion() *semver.Version { return e.server.Load() }

// startLoop replaces the export loop with one that exports with res, once it has an exporter.
func (e *engine) startLoop(ctx context.Context, res *resource.Resource) {
	e.stopLoop()
	l := export.NewLoop(e.reader, res, e.clk, e.newRand(), e.stats)
	lctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := l.Run(lctx); err != nil {
			log.FromContext(ctx).Error(err, "run the export loop")
		}
	}()
	e.loop = &loopRun{l: l, cancel: cancel, done: done}
}

// stopLoop stops the export loop and waits until it has shut its exporters down.
func (e *engine) stopLoop() {
	if e.loop == nil {
		return
	}
	e.loop.cancel()
	<-e.loop.done
	e.loop = nil
}

// runJob is the scheduler's RunFunc.
func (e *engine) runJob(ctx context.Context, job planner.Job) {
	r := e.currentRunner()
	if r == nil {
		return
	}
	e.record(ctx, r.exec.Run(ctx, job))
}

// record stores the result of a run that is still planned and counts it.
func (e *engine) record(ctx context.Context, res executor.Result) {
	if ctx.Err() != nil {
		return
	}
	job := res.Job
	logger := log.FromContext(ctx).GetLogger()
	e.plan.whilePlanned(job, func() {
		key := snapshot.Key{Query: job.Query, Database: job.Database}
		for reason, n := range res.Dropped {
			if n > 0 {
				e.stats.SeriesDropped(job.Query, job.Database, string(reason), n)
			}
		}

		outcome := res.Outcome
		switch outcome {
		case selfmetrics.OutcomeOK:
			err := e.store.Put(key, job.Fingerprint, res.Samples, res.Started)
			switch {
			case err == nil:
				e.errs.Succeeded(logger, job.Query, job.Database)
			case errors.Is(err, snapshot.ErrTotalCap):
				outcome = selfmetrics.OutcomeCapped
				e.store.Delete(key)
				e.stats.QueryCapped(job.Query, job.Database, len(res.Samples))
			default:
				outcome = selfmetrics.OutcomeError
				e.store.Delete(key)
				e.errs.Failed(logger, job.Query, job.Database, fmt.Errorf("store the result: %w", err))
			}
		case selfmetrics.OutcomeCapped:
			e.store.Delete(key)
			e.stats.QueryCapped(job.Query, job.Database, res.WouldBe)
		case selfmetrics.OutcomeSkipped:
			// A run caught in a pool swap keeps the entry of the run before it.
		case selfmetrics.OutcomeError, selfmetrics.OutcomeTimeout, selfmetrics.OutcomeDenied:
			e.store.Delete(key)
			if res.Err != nil {
				e.errs.Failed(logger, job.Query, job.Database, res.Err)
			}
		}
		if res.ConnectionLost {
			e.requestFacts()
		}
		e.stats.QueryRun(job.Query, job.Database, outcome, res.Duration, res.Started)
		e.stats.Series(e.store.Series())
	})
}

// skipped is the scheduler's SkipFunc. It only counts: the slow run it follows has just stored its result.
func (e *engine) skipped(job planner.Job, missed int) {
	e.plan.whilePlanned(job, func() {
		now := e.clk.Now()
		for range missed {
			e.stats.QueryRun(job.Query, job.Database, selfmetrics.OutcomeSkipped, 0, now)
		}
	})
}

// duplicates counts the series the producer dropped because an earlier entry has them.
func (e *engine) duplicates(dropped map[snapshot.Key]int) {
	for k, n := range dropped {
		e.stats.SeriesDropped(k.Query, k.Database, string(queries.DropDuplicate), n)
	}
}

// requestFacts asks the control goroutine for an early facts round.
func (e *engine) requestFacts() {
	select {
	case e.early <- struct{}{}:
	default:
	}
}

// onEarlyFacts runs a facts round now, or once earlyFactsGap has passed since the last one.
func (e *engine) onEarlyFacts(ctx context.Context) {
	if e.earlyTimer != nil {
		return
	}
	wait := e.lastFacts.Add(earlyFactsGap).Sub(e.clk.Now())
	if wait <= 0 {
		e.factsRound(ctx, false)
		return
	}
	e.earlyTimer = e.clk.NewTimer(wait)
}

// factsRound gathers the facts, sets upMetric, closes idle connections, replans when the facts changed
// and schedules the next round: a planning cycle after a success, sooner with backoff after a failure
// of a round the timer drove (timed). A failed early round leaves the timer as it is.
func (e *engine) factsRound(ctx context.Context, timed bool) {
	stopTimer(&e.earlyTimer)
	cur := e.plan.current
	if cur == nil || e.runner == nil {
		return
	}
	s := cur.settings
	e.lastFacts = e.clk.Now()

	facts, err := e.gather(ctx, s)
	e.runner.pool.CloseIdle()
	logger := log.FromContext(ctx)
	key := snapshot.Key{Database: s.Postgres.Database}
	if err != nil {
		if timed {
			e.factsFailures++
			e.setFactsTimer(factsWait(e.factsFailures, s.PlanInterval))
		}
		e.setUp(false)
		if ctx.Err() == nil && e.factsErrs.note(key, err.Error()) {
			logger.Error(err, "gather facts", "database", s.Postgres.Database)
		}
		return
	}
	e.factsFailures = 0
	e.setFactsTimer(s.PlanInterval)
	if e.factsErrs.forget(key) {
		logger.Info("gathered facts again", "database", s.Postgres.Database)
	}
	e.setUp(true)
	e.stats.SetChecks(facts.Checks)
	if facts.ChecksErr != nil {
		logger.Info("some live checks could not be read", "error", facts.ChecksErr.Error())
	}
	v := facts.Version
	e.server.Store(&v)
	e.plan.setFacts(ctx, facts)
}

// setUp records the outcome of a facts round for upMetric.
func (e *engine) setUp(ok bool) {
	var up int64
	if ok {
		up = 1
	}
	e.up.Store(up)
}

// gatherFacts reads the facts from PostgreSQL through the runner's pool. Call it on the control goroutine.
func (e *engine) gatherFacts(ctx context.Context, s settings.Settings) (planner.Facts, error) {
	// Long enough to wait for one run to free a connection, connect and run the facts queries.
	ctx, cancel := context.WithTimeout(ctx, connectTimeout+2*(s.Limits.QueryTimeout+runGrace))
	defer cancel()
	// factsWait and earlyFactsGap already space the rounds, so they skip the pool's backoff.
	conn, release, err := e.runner.pool.AcquireNow(ctx, s.Postgres.Database)
	if err != nil {
		return planner.Facts{}, fmt.Errorf("acquire a connection: %w", err)
	}
	facts, err := planner.GatherFacts(ctx, conn)
	release(err != nil)
	if err != nil {
		return planner.Facts{}, fmt.Errorf("gather facts: %w", err)
	}
	return facts, nil
}

// factsWait is the wait before the next facts round after failures consecutive failed ones:
// planInterval after a success, else 1s, 2s, 4s ... capped at planInterval.
func factsWait(failures int, planInterval time.Duration) time.Duration {
	if failures == 0 {
		return planInterval
	}
	wait := time.Second
	for i := 1; i < failures && wait < planInterval; i++ {
		wait *= 2
	}
	return min(wait, planInterval)
}

func (e *engine) setFactsTimer(d time.Duration) {
	stopTimer(&e.factsTimer)
	e.factsTimer = e.clk.NewTimer(d)
}

func stopTimer(t *clock.Timer) {
	if *t != nil {
		(*t).Stop()
		*t = nil
	}
}

func timerC(t clock.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C()
}

// notify tells the control goroutine that the watch saw a change.
func (e *engine) notify() {
	select {
	case e.changed <- struct{}{}:
	default:
	}
}
