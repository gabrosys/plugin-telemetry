package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// idleGrace is added to the longest query interval to get the pool's idle timeout.
const idleGrace = 10 * time.Second

// configuration is a loaded configuration with the definitions of its queries.
type configuration struct {
	settings       settings.Settings
	result         sources.Result
	defs           []queries.Definition // in the order of result.Entries
	idleTimeout    time.Duration        // of the connection pool
	exporterDigest string               // of the files the exporter reads
}

// replanner holds the configuration in force and rebuilds the plan when it or the facts change.
// whilePlanned may be called from any goroutine; every other method runs on the engine's control goroutine.
type replanner struct {
	cfg    Config
	stats  *selfmetrics.Stats
	store  *snapshot.Store
	errs   *errorLog
	update func(planner.Plan)

	loader     *sources.Loader
	current    *configuration // nil until the first valid configuration
	facts      *planner.Facts // nil until the first facts round
	skipped    []string       // the databases skipped for their names in the last facts round
	generation uint64
	digest     string // of the settings bytes, the sources and the exporter files the last reload read

	mu      sync.RWMutex            // guards planned; whilePlanned holds it while a result is written
	planned map[snapshot.Key]string // the fingerprint of each planned job
}

func newReplanner(cfg Config, stats *selfmetrics.Stats, store *snapshot.Store, errs *errorLog, update func(planner.Plan)) *replanner {
	return &replanner{cfg: cfg, stats: stats, store: store, errs: errs, update: update}
}

// reload reads the settings and digests them with the sources and the exporter files. When the
// digest changed it loads the sources, defines the queries, applies, commits and replans; a change of
// the exporter files alone rebuilds the exporter with no new generation. On error nothing in force changes;
// a not-ready input (ErrNotReady) also leaves the digest and the load counts alone.
func (e *engine) reload(ctx context.Context) error {
	r, read := e.plan, e.cfg.ReadFile
	logger := log.FromContext(ctx)

	// The settings and each file enter the digest before they are read, so a change during the load triggers another one.
	h := sha256.New()
	s, data, err := e.cfg.Settings()
	addSettings(h, data, err)
	if errors.Is(err, ErrNoConfiguration) {
		r.digest = hex.EncodeToString(h.Sum(nil))
		e.files.Store(nil)
		if r.current != nil {
			e.teardown()
			r.idle(ctx)
		}
		return fmt.Errorf("load settings: %w", err)
	}
	if errors.Is(err, ErrNotReady) {
		return fmt.Errorf("load settings: %w", err)
	}
	if err != nil {
		d := hex.EncodeToString(h.Sum(nil))
		if d == r.digest {
			return nil
		}
		r.digest = d
		r.stats.ConfigLoad(r.cfg.Name(), selfmetrics.OutcomeError, 0)
		return fmt.Errorf("load settings: %w", err)
	}

	cur := r.current
	files := watchedFiles(s)
	if cur != nil {
		// Until s is in force, a change to the files of either can fix or replace it.
		files = uniqueFiles(append(files, watchedFiles(cur.settings)...))
	}
	e.files.Store(&files)
	addFiles(h, read, s.Sources)
	exporterDigest := digestOf(read, exporterFiles(s.Exporter))
	h.Write([]byte(exporterDigest))
	d := hex.EncodeToString(h.Sum(nil))
	if d == r.digest {
		return nil
	}
	// A failed apply keeps this digest: safe only while every apply failure depends on digested inputs.
	r.digest = d
	r.stats.ConfigLoad(r.cfg.Name(), selfmetrics.OutcomeOK, 0)

	if r.loader == nil {
		r.loader = sources.NewLoader(read, s.Sources, LoaderOptions(r.cfg)...)
	} else {
		r.loader.SetFiles(s.Sources)
	}
	res := r.loader.Load()
	r.recordLoad(logger, res)

	same := cur != nil && res.Hash == cur.result.Hash && reflect.DeepEqual(s, cur.settings)
	if same && exporterDigest == cur.exporterDigest {
		return nil
	}
	var next *configuration
	if same {
		c := *cur
		next = &c
	} else {
		next = r.define(s, res)
	}
	next.exporterDigest = exporterDigest
	if err := e.apply(ctx, cur, next); err != nil {
		if same {
			logger.Error(err, "reload the exporter files")
			return nil
		}
		return fmt.Errorf("apply configuration: %w", err)
	}
	if same {
		r.current = next
		logger.Info("reloaded the exporter files")
		return nil
	}
	r.commit(logger, next)
	inForce := watchedFiles(next.settings)
	e.files.Store(&inForce)
	r.replan(ctx)
	return nil
}

// recordLoad counts and logs the outcome of each source.
func (r *replanner) recordLoad(logger log.Logger, res sources.Result) {
	for _, st := range res.Status {
		r.stats.ConfigLoad(st.Source, loadOutcome(st.Outcome), len(st.Duplicates))
		switch st.Outcome {
		case sources.OutcomeMissing:
			logger.Warning("query source missing", "source", st.Source, "error", fmt.Sprint(st.Err))
		case sources.OutcomeInvalid:
			logger.Warning("query source invalid", "source", st.Source, "usingPrevious", st.UsingPrevious,
				"error", fmt.Sprint(st.Err))
		case sources.OutcomeDenied:
			logger.Warning("query source denied", "source", st.Source, "usingPrevious", st.UsingPrevious,
				"error", fmt.Sprint(st.Err))
		}
		if len(st.Duplicates) > 0 {
			logger.Warning("query source overrides earlier queries", "source", st.Source, "queries", st.Duplicates)
		}
	}
}

// loadOutcome maps the outcome of a source to the self-metric outcome: ok, denied, or error.
func loadOutcome(o string) selfmetrics.Outcome {
	switch o {
	case sources.OutcomeOK:
		return selfmetrics.OutcomeOK
	case sources.OutcomeDenied:
		return selfmetrics.OutcomeDenied
	default:
		return selfmetrics.OutcomeError
	}
}

// Definitions defines the queries of res; a source cfg marks sensitive gets a definition that quotes no value.
func Definitions(cfg Config, s settings.Settings, res sources.Result) []queries.Definition {
	sensitive := func(string) bool { return false }
	if c, ok := cfg.(sensitiveConfig); ok {
		sensitive = c.Sensitive
	}
	defs := make([]queries.Definition, 0, len(res.Entries))
	for _, e := range res.Entries {
		opts := queries.Options{MaxLabelsPerSeries: s.Limits.MaxLabelsPerSeries, Sensitive: sensitive(e.Source)}
		defs = append(defs, queries.Define(e.Key, e.Query, opts))
	}

	return defs
}

// define builds the configuration of s and res.
func (r *replanner) define(s settings.Settings, res sources.Result) *configuration {
	defs := Definitions(r.cfg, s, res)
	return &configuration{settings: s, result: res, defs: defs, idleTimeout: idleTimeout(defs, s)}
}

// commit logs the warnings of next and makes it the configuration in force.
func (r *replanner) commit(logger log.Logger, next *configuration) {
	keys := make([]string, 0, len(next.defs))
	for i := range next.defs {
		d, source := &next.defs[i], next.result.Entries[i].Source
		keys = append(keys, d.Key)
		for _, w := range d.Warnings {
			logger.Warning("query definition", "query", d.Key, "source", source, "warning", w)
		}
		if d.Disabled != "" {
			logger.Warning("query disabled", "query", d.Key, "source", source, "reason", d.Disabled)
		}
	}
	for _, w := range next.settings.Warnings(keys) {
		logger.Warning("settings", "warning", w)
	}

	r.current = next
	r.generation++
	r.stats.ConfigGeneration(r.generation, next.result.Hash)
	logger.Info("configuration applied", "generation", r.generation, "hash", next.result.Hash, "queries", len(next.defs))
}

// setFacts records the facts of a round and replans when they changed.
func (r *replanner) setFacts(ctx context.Context, f planner.Facts) {
	if !slices.Equal(r.skipped, f.SkippedDatabases) {
		logger := log.FromContext(ctx)
		if len(f.SkippedDatabases) > 0 {
			logger.Warning("databases not monitored, their names are not valid UTF-8", "databases", f.SkippedDatabases)
		} else {
			logger.Info("all databases are monitored again")
		}
	}
	r.skipped = f.SkippedDatabases
	if r.facts != nil && sameFacts(*r.facts, f) {
		return
	}
	r.facts = &f
	r.replan(ctx)
}

func sameFacts(a, b planner.Facts) bool {
	return a.Version.Equal(&b.Version) && a.InRecovery == b.InRecovery && slices.Equal(a.Databases, b.Databases)
}

// replan builds the plan and publishes it. It does nothing before the first configuration and the first facts.
func (r *replanner) replan(ctx context.Context) {
	if r.current == nil || r.facts == nil {
		return
	}
	r.publish(ctx, planner.Build(r.current.defs, r.current.settings, *r.facts))
}

// idle drops the configuration in force and hands the scheduler an empty plan.
func (r *replanner) idle(ctx context.Context) {
	r.current, r.facts, r.skipped, r.loader = nil, nil, nil, nil
	r.publish(ctx, planner.Plan{})
}

// publish makes p the plan: it drops the snapshot entries, self-metrics and failures p no longer has
// and hands p to the scheduler.
func (r *replanner) publish(ctx context.Context, p planner.Plan) {
	planned := make(map[snapshot.Key]string, len(p.Jobs))
	keep := make(map[selfmetrics.QueryDatabase]bool, len(p.Jobs))
	for _, j := range p.Jobs {
		planned[snapshot.Key{Query: j.Query, Database: j.Database}] = j.Fingerprint
		keep[selfmetrics.QueryDatabase{Query: j.Query, Database: j.Database}] = true
	}

	r.setPlanned(p, planned, keep)

	logger := log.FromContext(ctx)
	for _, s := range p.Skips {
		logger.Debug("query not planned", "query", s.Query, "database", s.Database, "reason", s.Reason)
	}
	logger.Info("plan updated", "jobs", len(p.Jobs), "skipped", len(p.Skips))
	r.update(p)
}

// setPlanned makes planned the planned jobs and drops what p no longer has, while no result is written.
func (r *replanner) setPlanned(p planner.Plan, planned map[snapshot.Key]string, keep map[selfmetrics.QueryDatabase]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.planned = planned
	for _, j := range p.Jobs {
		r.stats.QueryInterval(j.Query, j.Interval)
	}
	r.store.Retain(func(k snapshot.Key, fingerprint string) bool {
		fp, ok := planned[k]
		return ok && fp == fingerprint
	})
	r.stats.Retain(keep)
	r.stats.Series(r.store.Series())
	r.errs.retain(func(k snapshot.Key) bool {
		_, ok := planned[k]
		return ok
	})
}

// whilePlanned calls fn if job is in the plan with its fingerprint, and holds off replans until fn returns.
func (r *replanner) whilePlanned(job planner.Job, fn func()) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if fp, ok := r.planned[snapshot.Key{Query: job.Query, Database: job.Database}]; ok && fp == job.Fingerprint {
		fn()
	}
}

// idleTimeout is the longest interval of the queries that can be planned, plus idleGrace. Facts only
// remove queries from that set, so it is never shorter than the longest planned interval.
func idleTimeout(defs []queries.Definition, s settings.Settings) time.Duration {
	var longest time.Duration
	for i := range defs {
		d := &defs[i]
		if d.Disabled != "" || s.ExcludedQuery(d.Key) {
			continue
		}
		_, interval := planner.EffectiveInterval(d, s)
		longest = max(longest, interval)
	}
	if longest > math.MaxInt64-idleGrace {
		return time.Duration(math.MaxInt64)
	}
	return longest + idleGrace
}

// watchedFiles lists every file the settings name that the engine reads.
func watchedFiles(s settings.Settings) []string {
	files := append([]string{}, s.Sources...)
	files = append(files, s.Postgres.PasswordFile)
	return uniqueFiles(append(files, exporterFiles(s.Exporter)...))
}

// exporterFiles lists the files the exporter reads.
func exporterFiles(e settings.Exporter) []string {
	return uniqueFiles([]string{e.HeadersFile, e.TLS.CAFile, e.TLS.CertFile, e.TLS.KeyFile})
}

// uniqueFiles drops empty names and repeats, keeping the order.
func uniqueFiles(files []string) []string {
	seen := make(map[string]bool, len(files))
	out := make([]string, 0, len(files))
	for _, f := range files {
		if f == "" || seen[f] {
			continue
		}
		seen[f] = true
		out = append(out, f)
	}
	return out
}

// digestOf hashes the contents of files in order, read with read; an unreadable file contributes its error.
func digestOf(read sources.ReadFile, files []string) string {
	h := sha256.New()
	addFiles(h, read, files)
	return hex.EncodeToString(h.Sum(nil))
}

func addFiles(h hash.Hash, read sources.ReadFile, files []string) {
	for _, f := range files {
		addFile(h, read, f)
	}
}

// addSettings adds the bytes Config.Settings returned; with none, the error stands for them.
func addSettings(h hash.Hash, data []byte, err error) {
	state := []byte{1}
	if data == nil && err != nil {
		state, data = []byte{0}, []byte(err.Error())
	}
	sum := sha256.Sum256(data)
	h.Write(state)
	h.Write(sum[:])
}

func addFile(h hash.Hash, read sources.ReadFile, file string) {
	state := []byte{1}
	data, err := read(file)
	if err != nil {
		state, data = []byte{0}, []byte(err.Error())
	}
	sum := sha256.Sum256(data)
	h.Write([]byte(file))
	h.Write(state)
	h.Write(sum[:])
}
