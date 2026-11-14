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
	settings    settings.Settings
	result      sources.Result
	defs        []queries.Definition // in the order of result.Entries
	idleTimeout time.Duration        // of the connection pool
}

// components are what a configuration sets up besides the plan.
type components interface {
	// watch makes the watcher cover files besides the settings; it is called before the sources are read.
	watch(ctx context.Context, files []string)
	// apply sets up next; prev is nil the first time. A nil next tears everything down. On error nothing has changed.
	apply(ctx context.Context, prev, next *configuration) error
}

// replanner loads the configuration and rebuilds the plan when the configuration or the facts change.
// whilePlanned may be called from any goroutine; every other method runs on the engine's control goroutine.
type replanner struct {
	cfg    Config
	stats  *selfmetrics.Stats
	store  *snapshot.Store
	errs   *errorLog
	update func(planner.Plan)
	comps  components

	loader      *sources.Loader
	loaderFiles []string
	current     *configuration // nil until the first valid configuration
	facts       *planner.Facts // nil until the first facts round
	generation  uint64
	digest      string   // of the settings bytes and digestFiles as the last load read them; empty forces the next load
	digestFiles []string // the sources the settings name

	mu      sync.RWMutex            // guards planned; whilePlanned holds it while a result is written
	planned map[snapshot.Key]string // the fingerprint of each planned job
}

func newReplanner(
	cfg Config, stats *selfmetrics.Stats, store *snapshot.Store, errs *errorLog, update func(planner.Plan), comps components,
) *replanner {
	return &replanner{cfg: cfg, stats: stats, store: store, errs: errs, update: update, comps: comps}
}

// changed reports whether the settings or the files the last load read have changed since.
func (r *replanner) changed() bool {
	if r.digest == "" {
		return true
	}
	_, data, err := r.cfg.Settings()
	h := sha256.New()
	addSettings(h, data, err)
	addFiles(h, r.cfg.ReadFile, r.digestFiles)
	return hex.EncodeToString(h.Sum(nil)) != r.digest
}

// reload loads the settings and the sources and, when they differ from the configuration in force,
// applies them and replans. On error the configuration in force stays.
func (r *replanner) reload(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// The settings and each file enter the digest before they are read, so a change during the load triggers another one.
	h := sha256.New()
	s, data, err := r.cfg.Settings()
	addSettings(h, data, err)
	if errors.Is(err, ErrNoConfiguration) {
		r.digest, r.digestFiles = hex.EncodeToString(h.Sum(nil)), nil
		r.comps.watch(ctx, nil)
		if r.current != nil {
			r.idle(ctx)
		}
		return fmt.Errorf("load settings: %w", err)
	}
	r.stats.ConfigLoad(r.cfg.Name(), err == nil, 0)
	if err != nil {
		r.digest, r.digestFiles = hex.EncodeToString(h.Sum(nil)), nil
		return fmt.Errorf("load settings: %w", err)
	}

	r.comps.watch(ctx, watchedFiles(s))
	addFiles(h, r.cfg.ReadFile, s.Sources)
	r.digest, r.digestFiles = hex.EncodeToString(h.Sum(nil)), slices.Clone(s.Sources)

	if r.loader == nil || !slices.Equal(r.loaderFiles, s.Sources) {
		r.loader, r.loaderFiles = sources.NewLoader(r.cfg.ReadFile, s.Sources, LoaderOptions(r.cfg)...), slices.Clone(s.Sources)
	}
	res := r.loader.Load()
	r.recordLoad(logger, res)

	if r.current != nil && res.Hash == r.current.result.Hash && reflect.DeepEqual(s, r.current.settings) {
		return nil
	}
	next := define(s, res)
	if err := r.comps.apply(ctx, r.current, next); err != nil {
		r.digest = ""
		return fmt.Errorf("apply configuration: %w", err)
	}
	r.commit(logger, next)
	r.replan(ctx)
	return nil
}

// recordLoad counts and logs the outcome of each source.
func (r *replanner) recordLoad(logger log.Logger, res sources.Result) {
	for _, st := range res.Status {
		r.stats.ConfigLoad(st.Source, st.Outcome == sources.OutcomeOK, len(st.Duplicates))
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

// define builds the configuration of s and res.
func define(s settings.Settings, res sources.Result) *configuration {
	opts := queries.Options{MaxLabelsPerSeries: s.Limits.MaxLabelsPerSeries}
	defs := make([]queries.Definition, 0, len(res.Entries))
	for _, e := range res.Entries {
		defs = append(defs, queries.Define(e.Key, e.Query, opts))
	}
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

// idle drops the configuration in force and everything it set up, and hands the scheduler an empty plan.
func (r *replanner) idle(ctx context.Context) {
	// Tearing down cannot fail.
	if err := r.comps.apply(ctx, r.current, nil); err != nil {
		log.FromContext(ctx).Error(err, "tear down the configuration")
	}
	r.current, r.facts, r.loader, r.loaderFiles = nil, nil, nil, nil
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

	r.mu.Lock()
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
	r.mu.Unlock()

	logger := log.FromContext(ctx)
	for _, s := range p.Skips {
		logger.Debug("query not planned", "query", s.Query, "database", s.Database, "reason", s.Reason)
	}
	logger.Info("plan updated", "jobs", len(p.Jobs), "skipped", len(p.Skips))
	r.update(p)
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
