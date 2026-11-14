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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/featuregate"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/scheduler"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

const (
	// watchDebounce is the quiet time after a file change before the engine reacts.
	watchDebounce = time.Second
	// watchRetry is the wait before another attempt to watch the configuration files.
	watchRetry = 5 * time.Second
	// connectTimeout bounds a PostgreSQL connection attempt.
	connectTimeout = 5 * time.Second
	// runGrace is the executor's margin over the query timeout.
	runGrace = 2 * time.Second
	// earlyFactsGap is the shortest time between a facts round and one a failed run asks for.
	earlyFactsGap = time.Second
	// shutdownTimeout bounds each shutdown step that can block.
	shutdownTimeout = 5 * time.Second

	// upMetric and upDescription are CloudNativePG v1.30.1's (pg_collector.go:130-135).
	upMetric      = "cnpg_collector_up"
	upDescription = "1 if PostgreSQL is up, 0 otherwise."
)

// ErrNoConfigPath is returned by Run when Options.ConfigPath is empty.
var ErrNoConfigPath = errors.New("no configuration file given")

// Options configure Run.
type Options struct {
	ConfigPath string                // the settings file
	Gates      *featuregate.Registry // nil means no gate is set
	Clock      clock.Clock           // nil means the real clock
	Rand       *rand.Rand            // seeds the jitter of the scheduler and the export loop; nil means a random seed
	Getenv     func(string) string   // reads the downward-API variables of the resource; nil means os.Getenv
}

// Run blocks until ctx ends. An invalid configuration at start or on reload is logged;
// at start the engine idles until the file becomes valid, later it keeps the last good one.
func Run(ctx context.Context, o Options) error {
	if o.ConfigPath == "" {
		return ErrNoConfigPath
	}
	e, err := newEngine(ctx, o)
	if err != nil {
		return err
	}
	e.run(ctx)
	return nil
}

// runner runs jobs on one pool; each run holds inflight, so a replaced pool closes after its runs.
type runner struct {
	spec     poolSpec
	limits   settings.Limits
	pool     *pg.Pool
	exec     *executor.Executor
	inflight sync.WaitGroup
}

// poolSpec is what a pool is built from.
type poolSpec struct {
	postgres                  settings.Postgres
	queryTimeout, lockTimeout time.Duration
	idleTimeout               time.Duration
}

// loopRun is a running export loop.
type loopRun struct {
	l      *export.Loop
	cancel context.CancelFunc
	done   chan struct{}
}

// watchRun is a running file watcher.
type watchRun struct {
	files  []string
	cancel context.CancelFunc
	done   chan struct{}
}

type engine struct {
	path   string
	gates  *featuregate.Registry
	clk    clock.Clock
	rnd    *rand.Rand // control goroutine only
	getenv func(string) string

	stats  *selfmetrics.Stats
	store  *snapshot.Store
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
	sched  *scheduler.Scheduler
	plan   *replanner
	errs   errorLog

	up      atomic.Bool                    // the last facts round succeeded
	cluster atomic.Pointer[string]         // the cluster label of upMetric; nil before a valid configuration
	server  atomic.Pointer[semver.Version] // from the last successful facts round
	changed chan struct{}                  // a watched file changed
	early   chan struct{}                  // a run failed without a server error

	mu      sync.Mutex // guards runner, which only the control goroutine writes
	runner  *runner
	retired sync.WaitGroup // replaced runners waiting for their runs

	// The fields below belong to the control goroutine.
	watcher        *watchRun
	pendingWatch   []string // files to watch once a retry succeeds
	retryTimer     clock.Timer
	factsTimer     clock.Timer
	earlyTimer     clock.Timer
	lastFacts      time.Time
	factsErrs      errorLog // the facts errors logged, keyed by the default database
	exp            *export.Exporter
	exporterFiles  []string
	exporterDigest string
	loop           *loopRun
}

func newEngine(ctx context.Context, o Options) (*engine, error) {
	e := &engine{
		path:    o.ConfigPath,
		gates:   o.Gates,
		clk:     o.Clock,
		rnd:     o.Rand,
		getenv:  o.Getenv,
		stats:   selfmetrics.NewStats(),
		store:   snapshot.New(0),
		changed: make(chan struct{}, 1),
		early:   make(chan struct{}, 1),
	}
	if e.clk == nil {
		e.clk = clock.RealClock{}
	}
	if e.rnd == nil {
		e.rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // jitter, not security
	}
	if e.getenv == nil {
		e.getenv = os.Getenv
	}

	e.reader = sdkmetric.NewManualReader(sdkmetric.WithProducer(export.NewProducer(e.store, e.clk, e.duplicates)))
	e.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(e.reader),
		sdkmetric.WithView(selfmetrics.Views()...),
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	)
	meter := e.mp.Meter(export.ScopeName, metric.WithInstrumentationVersion(version.Version))
	if err := e.instrument(meter); err != nil {
		return nil, errors.Join(err, e.mp.Shutdown(ctx))
	}

	e.sched = scheduler.New(e.clk, e.newRand(), e.runJob, e.skipped)
	e.plan = newReplanner(o.ConfigPath, e.stats, e.store, &e.errs, e.sched.Update, e)
	return e, nil
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
		metric.WithUnit("1"),
		metric.WithDescription(upDescription),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			cluster := e.cluster.Load()
			if cluster == nil {
				return nil
			}
			var up int64
			if e.up.Load() {
				up = 1
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

// run is the control goroutine: it reacts to file changes, facts rounds and failed runs until ctx ends.
func (e *engine) run(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("starting the metrics engine", "config", e.path, "version", version.Version)
	if e.gates != nil {
		for _, g := range e.gates.List() {
			logger.Info("feature gate", "id", g.ID, "stage", g.Stage, "enabled", g.Enabled)
		}
	}

	schedCtx, stopSched := context.WithCancel(ctx)
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		if err := e.sched.Run(schedCtx); err != nil {
			logger.Error(err, "run the scheduler")
		}
	}()
	defer e.shutdown(ctx, stopSched, schedDone)

	e.watch(ctx, []string{e.path})
	e.reload(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.changed:
			e.onChange(ctx)
		case <-timerC(e.factsTimer):
			e.factsRound(ctx)
		case <-e.early:
			e.onEarlyFacts(ctx)
		case <-timerC(e.earlyTimer):
			e.factsRound(ctx)
		case <-timerC(e.retryTimer):
			e.retryWatch(ctx)
		}
	}
}

// shutdown stops the export loop, the scheduler, the pool, the exporter, the watcher and the meter provider.
func (e *engine) shutdown(ctx context.Context, stopSched context.CancelFunc, schedDone <-chan struct{}) {
	logger := log.FromContext(ctx)
	ctx = context.WithoutCancel(ctx)
	for _, t := range []*clock.Timer{&e.factsTimer, &e.earlyTimer, &e.retryTimer} {
		stopTimer(t)
	}

	e.stopLoop()
	stopSched()
	<-schedDone

	e.mu.Lock()
	r := e.runner
	e.runner = nil
	e.mu.Unlock()
	e.retired.Wait()
	if r != nil {
		r.pool.Close()
	}

	if e.exp != nil {
		sctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		if err := e.exp.Shutdown(sctx); err != nil {
			logger.Error(err, "shut down the exporter")
		}
		cancel()
	}
	e.stopWatch()

	sctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	if err := e.mp.Shutdown(sctx); err != nil {
		logger.Error(err, "shut down the meter provider")
	}
	logger.Info("metrics engine stopped")
}

// reload loads the configuration and logs why it was not applied.
func (e *engine) reload(ctx context.Context) {
	err := e.plan.reload(ctx)
	if err == nil {
		return
	}
	logger := log.FromContext(ctx)
	if e.plan.current == nil {
		logger.Error(err, "invalid configuration, waiting for a valid one", "config", e.path)
		return
	}
	logger.Error(err, "invalid configuration, keeping the one in force", "config", e.path, "generation", e.plan.generation)
}

// onChange reloads the exporter's files and the configuration, each only if its files changed.
func (e *engine) onChange(ctx context.Context) {
	if e.exp != nil {
		if d := digestOf(e.exporterFiles); d != e.exporterDigest {
			e.exporterDigest = d
			if err := e.exp.Reload(); err != nil { //nolint:contextcheck // Exporter.Reload takes no context
				log.FromContext(ctx).Error(err, "reload the exporter files")
			} else {
				log.FromContext(ctx).Info("reloaded the exporter files")
			}
		}
	}
	if e.plan.changed() {
		e.reload(ctx)
	}
}

// apply sets up the exporter, the export loop, the pool and the snapshot cap for next.
func (e *engine) apply(ctx context.Context, prev, next *configuration) error {
	s := next.settings
	first := prev == nil
	var old settings.Settings
	if !first {
		old = prev.settings
	}
	resourceChanged := first || old.Cluster != s.Cluster || old.Namespace != s.Namespace ||
		!maps.Equal(old.ResourceAttributes, s.ResourceAttributes)
	exporterChanged := first || old.Exporter != s.Exporter

	var exp *export.Exporter
	files, digest := exporterFiles(s.Exporter), e.exporterDigest
	if resourceChanged || exporterChanged {
		digest = digestOf(files)
		var err error
		if exp, err = export.NewExporter(ctx, s.Exporter); err != nil {
			return fmt.Errorf("create the exporter: %w", err)
		}
	}

	// Nothing below fails.
	cluster := s.Cluster
	e.cluster.Store(&cluster)
	e.store.SetMaxSeriesTotal(s.Limits.MaxSeriesTotal)
	poolReplaced := e.setRunner(ctx, s, next.idleTimeout)
	if exp != nil {
		if resourceChanged {
			// The loop takes the resource at creation; the old loop shuts the old exporter down.
			e.startLoop(ctx, export.Resource(s, e.getenv), exp, s.Exporter)
		} else {
			e.loop.l.SetExporter(exp, s.Exporter.Interval, s.Exporter.MaxPointsPerRequest)
		}
		e.exp, e.exporterFiles, e.exporterDigest = exp, files, digest
	}
	switch {
	case first || poolReplaced:
		e.setFactsTimer(0)
	case old.PlanInterval != s.PlanInterval:
		e.setFactsTimer(s.PlanInterval)
	}
	return nil
}

func pgConfig(s settings.Settings) pg.Config {
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

// setRunner makes the runner match s, building a new pool when the connection settings or the idle
// timeout changed. The replaced runner's pool closes once its runs end.
func (e *engine) setRunner(ctx context.Context, s settings.Settings, idle time.Duration) (poolReplaced bool) {
	spec := poolSpec{postgres: s.Postgres, queryTimeout: s.Limits.QueryTimeout, lockTimeout: s.Limits.LockTimeout, idleTimeout: idle}
	cur := e.runner
	if cur != nil && cur.spec == spec && cur.limits == s.Limits {
		return false
	}
	var pool *pg.Pool
	if cur != nil && cur.spec == spec {
		pool = cur.pool
	} else {
		pool = pg.NewPool(pgConfig(s), s.Postgres.MaxConnections, idle, e.clk, pg.WithLogger(log.FromContext(ctx).GetLogger()))
	}
	next := &runner{spec: spec, limits: s.Limits, pool: pool, exec: executor.New(pool, s.Limits, e.serverVersion, e.clk)}

	e.mu.Lock()
	e.runner = next
	e.mu.Unlock()
	if cur == nil {
		return true
	}
	if cur.pool == pool {
		return false
	}
	e.retired.Add(1)
	go func() {
		defer e.retired.Done()
		cur.inflight.Wait()
		cur.pool.Close()
	}()
	return true
}

func (e *engine) serverVersion() *semver.Version { return e.server.Load() }

// startLoop replaces the export loop with one that exports with res through exp.
func (e *engine) startLoop(ctx context.Context, res *resource.Resource, exp *export.Exporter, s settings.Exporter) {
	e.stopLoop()
	l := export.NewLoop(e.reader, res, e.clk, e.newRand(), e.stats)
	l.SetExporter(exp, s.Interval, s.MaxPointsPerRequest)
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
	e.mu.Lock()
	r := e.runner
	if r != nil {
		r.inflight.Add(1)
	}
	e.mu.Unlock()
	if r == nil {
		return
	}
	res := r.exec.Run(ctx, job)
	r.inflight.Done()
	e.record(ctx, res)
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
		case selfmetrics.OutcomeError, selfmetrics.OutcomeTimeout, selfmetrics.OutcomeSkipped:
			e.store.Delete(key)
			if res.Err != nil {
				e.errs.Failed(logger, job.Query, job.Database, res.Err)
				if !serverError(res.Err) {
					e.requestFacts()
				}
			}
		}
		e.stats.QueryRun(job.Query, job.Database, outcome, res.Duration, res.Started)
		e.stats.Series(e.store.Series())
	})
}

// serverError reports whether PostgreSQL itself returned err, as opposed to a lost connection,
// a backoff or a context error.
func serverError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr)
}

// skipped is the scheduler's SkipFunc.
func (e *engine) skipped(job planner.Job, missed int) {
	e.plan.whilePlanned(job, func() {
		e.store.Delete(snapshot.Key{Query: job.Query, Database: job.Database})
		now := e.clk.Now()
		for range missed {
			e.stats.QueryRun(job.Query, job.Database, selfmetrics.OutcomeSkipped, 0, now)
		}
		e.stats.Series(e.store.Series())
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
		e.factsRound(ctx)
		return
	}
	e.earlyTimer = e.clk.NewTimer(wait)
}

// factsRound gathers the facts, sets upMetric, closes idle connections and replans when the facts changed.
func (e *engine) factsRound(ctx context.Context) {
	stopTimer(&e.earlyTimer)
	cur := e.plan.current
	if cur == nil || e.runner == nil {
		return
	}
	s := cur.settings
	e.setFactsTimer(s.PlanInterval)
	e.lastFacts = e.clk.Now()

	facts, err := e.gatherFacts(ctx, s)
	e.runner.pool.CloseIdle()
	logger := log.FromContext(ctx)
	key := snapshot.Key{Database: s.Postgres.Database}
	if err != nil {
		e.up.Store(false)
		if ctx.Err() == nil && e.factsErrs.note(key, err.Error()) {
			logger.Error(err, "gather facts", "database", s.Postgres.Database)
		}
		return
	}
	if e.factsErrs.forget(key) {
		logger.Info("gathered facts again", "database", s.Postgres.Database)
	}
	e.up.Store(true)
	v := facts.Version
	e.server.Store(&v)
	e.plan.setFacts(ctx, facts)
}

func (e *engine) gatherFacts(ctx context.Context, s settings.Settings) (planner.Facts, error) {
	// Long enough to wait for one run to free a connection, connect and run the facts queries.
	ctx, cancel := context.WithTimeout(ctx, connectTimeout+2*(s.Limits.QueryTimeout+runGrace))
	defer cancel()
	conn, release, err := e.runner.pool.Acquire(ctx, s.Postgres.Database)
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

// watch makes the watcher cover files. The current watcher stays until the new one is ready;
// if the new one fails, it is retried after watchRetry.
func (e *engine) watch(ctx context.Context, files []string) {
	files = uniqueFiles(files)
	if e.watcher != nil && slices.Equal(e.watcher.files, files) {
		e.pendingWatch = nil
		stopTimer(&e.retryTimer)
		return
	}
	w, err := e.startWatch(ctx, files)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if !slices.Equal(e.pendingWatch, files) {
			log.FromContext(ctx).Error(err, "watch the configuration files", "files", files, "retryEvery", watchRetry)
		}
		e.pendingWatch = files
		if e.retryTimer == nil {
			e.retryTimer = e.clk.NewTimer(watchRetry)
		}
		return
	}
	e.stopWatch()
	e.watcher, e.pendingWatch = w, nil
	stopTimer(&e.retryTimer)
}

// retryWatch retries a failed watch and reloads once it works, since changes may have been missed.
func (e *engine) retryWatch(ctx context.Context) {
	e.retryTimer = nil
	if e.pendingWatch == nil {
		return
	}
	e.watch(ctx, e.pendingWatch)
	if e.pendingWatch == nil {
		e.reload(ctx)
	}
}

// startWatch starts a watcher on files and returns once it is ready.
func (e *engine) startWatch(ctx context.Context, files []string) (*watchRun, error) {
	logger := log.FromContext(ctx)
	wctx, cancel := context.WithCancel(ctx)
	ready := make(chan struct{})
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- sources.Watch(wctx, files, watchDebounce, e.notify,
			sources.WithReady(func() { close(ready) }),
			sources.WithErrorHandler(func(err error) {
				// A watcher error can mean lost events, so look at the files again.
				logger.Error(err, "watch the configuration files")
				e.notify()
			}))
	}()
	select {
	case <-ready:
		return &watchRun{files: files, cancel: cancel, done: done}, nil
	case err := <-result:
		cancel()
		if err == nil {
			err = errors.New("the watcher stopped before it was ready")
		}
		return nil, fmt.Errorf("start the watcher: %w", err)
	}
}

func (e *engine) stopWatch() {
	if e.watcher == nil {
		return
	}
	e.watcher.cancel()
	<-e.watcher.done
	e.watcher = nil
}

// notify tells the control goroutine that a watched file changed.
func (e *engine) notify() {
	select {
	case e.changed <- struct{}{}:
	default:
	}
}
