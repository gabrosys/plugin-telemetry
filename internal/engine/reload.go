package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"os"
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
	// watch makes the file watcher cover files; it is called before the sources are read.
	watch(ctx context.Context, files []string)
	// apply sets up next; prev is nil the first time. On error nothing has changed.
	apply(ctx context.Context, prev, next *configuration) error
}

// replanner loads the configuration and rebuilds the plan when the configuration or the facts change.
// whilePlanned may be called from any goroutine; every other method runs on the engine's control goroutine.
type replanner struct {
	path   string
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
	digest      string   // of digestFiles as the last load read them; empty forces the next load
	digestFiles []string // the settings file, then the sources it names

	mu      sync.RWMutex            // guards planned; whilePlanned holds it while a result is written
	planned map[snapshot.Key]string // the fingerprint of each planned job
}

func newReplanner(
	path string, stats *selfmetrics.Stats, store *snapshot.Store, errs *errorLog, update func(planner.Plan), comps components,
) *replanner {
	return &replanner{path: path, stats: stats, store: store, errs: errs, update: update, comps: comps}
}

// changed reports whether the files the last load read have changed since.
func (r *replanner) changed() bool {
	return r.digest == "" || digestOf(r.digestFiles) != r.digest
}

// reload loads the settings and the sources and, when they differ from the configuration in force,
// applies them and replans. On error the configuration in force stays.
func (r *replanner) reload(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// Each file enters the digest before it is read, so a change during the load triggers another one.
	h := sha256.New()
	addFile(h, r.path)
	s, err := settings.Load(r.path)
	r.stats.ConfigLoad(r.path, err == nil, 0)
	if err != nil {
		r.digest, r.digestFiles = hex.EncodeToString(h.Sum(nil)), []string{r.path}
		return fmt.Errorf("load settings: %w", err)
	}

	r.comps.watch(ctx, watchedFiles(r.path, s))
	for _, f := range s.Sources {
		addFile(h, f)
	}
	r.digest, r.digestFiles = hex.EncodeToString(h.Sum(nil)), append([]string{r.path}, s.Sources...)

	if r.loader == nil || !slices.Equal(r.loaderFiles, s.Sources) {
		r.loader, r.loaderFiles = sources.NewLoader(s.Sources), slices.Clone(s.Sources)
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

// replan builds the plan, drops the snapshot entries, self-metrics and failures it no longer has,
// and hands it on. It does nothing before the first configuration and the first facts.
func (r *replanner) replan(ctx context.Context) {
	if r.current == nil || r.facts == nil {
		return
	}
	p := planner.Build(r.current.defs, r.current.settings, *r.facts)

	planned := make(map[snapshot.Key]string, len(p.Jobs))
	keep := make(map[string]bool, len(p.Jobs))
	for _, j := range p.Jobs {
		planned[snapshot.Key{Query: j.Query, Database: j.Database}] = j.Fingerprint
		keep[j.Query] = true
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
		_, interval := s.GroupFor(d.Key)
		longest = max(longest, interval, cacheInterval(d.CacheSeconds))
	}
	if longest > math.MaxInt64-idleGrace {
		return time.Duration(math.MaxInt64)
	}
	return longest + idleGrace
}

// cacheInterval is the planner's conversion of cache_seconds.
func cacheInterval(seconds uint64) time.Duration {
	if seconds > math.MaxInt64/uint64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds) * time.Second
}

// watchedFiles lists every file the configuration at path reads.
func watchedFiles(path string, s settings.Settings) []string {
	files := append([]string{path}, s.Sources...)
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

// digestOf hashes the contents of files in order; an unreadable file contributes its error.
func digestOf(files []string) string {
	h := sha256.New()
	for _, f := range files {
		addFile(h, f)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func addFile(h hash.Hash, file string) {
	state := []byte{1}
	data, err := os.ReadFile(file) //nolint:gosec // paths come from the operator's settings
	if err != nil {
		state, data = []byte{0}, []byte(err.Error())
	}
	sum := sha256.Sum256(data)
	h.Write([]byte(file))
	h.Write(state)
	h.Write(sum[:])
}
