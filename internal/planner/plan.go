package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

// Reasons a query or one of its databases is not planned.
const (
	ReasonExcludedQuery    = "excluded query"
	ReasonDisabled         = "disabled: "
	ReasonAllMetrics       = "all metrics excluded"
	ReasonPrimaryOnly      = "primary only"
	ReasonRunOnServer      = "runonserver"
	ReasonExcludedDatabase = "excluded database"
	ReasonMaxDatabases     = "maxDatabases"
	ReasonNoDatabase       = "no matching database"
)

// isGlob is CloudNativePG's isPathPattern (pkg/management/postgres/metrics/collector.go:78 at v1.30.1).
var isGlob = regexp.MustCompile(`[][*?]`)

// Job is one query to run on one database.
type Job struct {
	Query       string // Definition.Key
	Database    string
	Group       string
	Interval    time.Duration
	Fingerprint string
	Def         *queries.Definition
}

// Skip is a query, or one database of it, that is not planned.
type Skip struct{ Query, Database, Reason string }

// Plan is the result of Build.
type Plan struct {
	Jobs  []Job  // sorted by Database, then Query
	Skips []Skip // sorted the same way; Database empty when the whole query is skipped
}

// Build plans the jobs and skips for defs.
func Build(defs []queries.Definition, s settings.Settings, f Facts) Plan {
	var p Plan
	for i := range defs {
		planQuery(&p, &defs[i], s, f)
	}
	slices.SortFunc(p.Jobs, func(a, b Job) int {
		return cmpPair(a.Database, a.Query, b.Database, b.Query)
	})
	slices.SortFunc(p.Skips, func(a, b Skip) int {
		return cmpPair(a.Database, a.Query, b.Database, b.Query)
	})
	return p
}

func cmpPair(a1, a2, b1, b2 string) int {
	if c := strings.Compare(a1, b1); c != 0 {
		return c
	}
	return strings.Compare(a2, b2)
}

func planQuery(p *Plan, d *queries.Definition, s settings.Settings, f Facts) {
	skip := func(db, reason string) { p.Skips = append(p.Skips, Skip{Query: d.Key, Database: db, Reason: reason}) }

	switch {
	case s.ExcludedQuery(d.Key):
		skip("", ReasonExcludedQuery)
		return
	case d.Disabled != "":
		skip("", ReasonDisabled+d.Disabled)
		return
	case d.PrimaryOnly && f.InRecovery:
		skip("", ReasonPrimaryOnly)
		return
	case d.RunOnServer != nil && !d.RunOnServer.Check(&f.Version):
		skip("", ReasonRunOnServer)
		return
	}

	def, excluded, allExcluded := applyMetricExclusions(d, s)
	if allExcluded {
		skip("", ReasonAllMetrics)
		return
	}

	targets := expandTargets(d.TargetDatabases, s.Postgres.Database, f.Databases)
	if len(targets) == 0 {
		skip("", ReasonNoDatabase)
		return
	}

	group, interval := s.GroupFor(d.Key)
	interval = max(interval, cacheInterval(d.CacheSeconds))
	fingerprint := fingerprintOf(d.Fingerprint, excluded)

	planned := 0
	for _, db := range targets {
		switch {
		case s.ExcludedDatabase(db):
			skip(db, ReasonExcludedDatabase)
		case planned >= s.Limits.MaxDatabases:
			skip(db, ReasonMaxDatabases)
		default:
			planned++
			p.Jobs = append(p.Jobs, Job{
				Query: d.Key, Database: db, Group: group, Interval: interval,
				Fingerprint: fingerprint, Def: def,
			})
		}
	}
}

// applyMetricExclusions returns a copy of d whose excluded value columns are discarded, the sorted
// names of those metrics, and whether the query had value columns and all of them are excluded.
func applyMetricExclusions(d *queries.Definition, s settings.Settings) (def *queries.Definition, excluded []string, all bool) {
	cp := *d
	cp.Columns = make(map[string]queries.Column, len(d.Columns))
	values := 0
	for name, c := range d.Columns {
		if c.Role == queries.RoleValue && c.Metric != "" {
			values++
			if s.ExcludedMetric(c.Metric) {
				c.Role = queries.RoleDiscard
				excluded = append(excluded, c.Metric)
			}
		}
		cp.Columns[name] = c
	}
	slices.Sort(excluded)
	return &cp, excluded, values > 0 && len(excluded) == values
}

func fingerprintOf(base string, excluded []string) string {
	h := sha256.New()
	h.Write([]byte(base))
	for _, m := range excluded {
		h.Write([]byte{0})
		h.Write([]byte(m))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func cacheInterval(seconds uint64) time.Duration {
	if seconds > math.MaxInt64/uint64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds) * time.Second
}

// expandTargets follows CloudNativePG's expandTargetDatabases (collector.go:262-281 at v1.30.1):
// literal names are kept as given, globs match against the connectable databases.
func expandTargets(targets []string, defaultDB string, databases []string) []string {
	if len(targets) == 0 {
		return []string{defaultDB}
	}
	set := map[string]struct{}{}
	for _, t := range targets {
		if !isGlob.MatchString(t) {
			set[t] = struct{}{}
			continue
		}
		for _, db := range databases {
			if ok, err := path.Match(t, db); err == nil && ok {
				set[db] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for db := range set {
		out = append(out, db)
	}
	slices.Sort(out)
	return out
}
