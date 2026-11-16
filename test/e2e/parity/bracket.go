// Package parity compares an instance's CNPG metrics endpoint with what the sidecar exports for it.
package parity

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Margin is how far CNPG's samples must lie before and after ours.
const Margin = time.Second

// Kinds of a Series.
const (
	kindCounter   = "counter"
	kindGauge     = "gauge"
	kindHistogram = "histogram"
)

// Verdicts of a Check.
const (
	verdictPass        = "pass"
	verdictFail        = "fail"
	verdictNotCompared = "not compared"
	verdictExpected    = "expected"
	verdictSkipped     = "skipped"
)

// Rules of a Check; a skipped check carries the exception name from Options.Skip instead.
const (
	ruleBracket       = "bracket"
	ruleSelfMeasuring = "self-measuring"
	ruleQueryCache    = "cnpg-query-cache"
	// ruleWALRegex: v1.30.1 counts WAL files with ^[0-9A-F]{24}, without $, so .partial and .backup files
	// count too (pkg/management/postgres/webserver/metricserver/wal.go:147); main anchors it.
	ruleWALRegex = "v1.30.1-wal-regex"
	ruleCNPGOnly = "cnpg-only"
	ruleOursOnly = "ours-only"
)

// selfMeasuring are CNPG's metrics that measure its collector (pg_collector.go:112-141 at v1.30.1).
var selfMeasuring = []string{
	"cnpg_collector_collections_total", "cnpg_collector_collection_errors_total",
	"cnpg_collector_last_collection_error", "cnpg_collector_collection_duration_seconds",
}

// queryCache maps CNPG's query-cache metrics (metrics/collector.go:352-376 at v1.30.1) to our nearest equivalent.
var queryCache = map[string]string{
	"cnpg_cache_hits":            "none",
	"cnpg_cache_miss":            "none",
	"cnpg_last_error":            "cnpg_otel_query_runs_total",
	"cnpg_errors_total":          "cnpg_otel_query_runs_total",
	"cnpg_last_update_timestamp": "cnpg_otel_query_last_success_timestamp_seconds",
}

// Series is one sample of one side.
type Series struct {
	Name, Metric, Kind string // Name metric{k="v",...} sorted; Kind counter, gauge or histogram
	Labels             map[string]string
	Value, Sum         float64 // Value is a histogram's count
	At                 time.Time
}

// Check is the verdict on one series, or on one metric for the rules that judge a metric as a whole.
type Check struct {
	Series, Metric, Kind, Rule, Verdict, Detail string // Verdict pass, fail, not compared, expected, skipped
	Gated                                       bool   // Options.Gate holds for Metric
	Before, Ours, After                         *Series
}

// Options tune Bracket.
type Options struct {
	Gate          func(metric string) bool // nil gates nothing
	Skip          map[string]string        // series or metric name to exception name
	LooseWALCount bool                     // v1.30.1-wal-regex
}

// Bracket checks ours against CNPG's samples before and after it. A series passes when CNPG's before sample is
// at or before ours.At − Margin, the after sample at or after ours.At + Margin, and ours lies in [min, max] of
// them (NaN equals NaN; histograms by count and sum). The metrics measuring the collector itself pass by
// presence and label keys; the query-cache, go_* and process_* metrics are expected on CNPG's side only, and
// cnpg_otel_*, otel.* and go.* on ours only. The checks are sorted by metric, then series.
func Bracket(before, after, ours []Series, o Options) []Check {
	s := sides{byName(before), byName(after), byName(ours)}
	names := map[string][]string{} // metric to its series names on any side
	for _, side := range []map[string]Series{s.before, s.after, s.ours} {
		for name, x := range side {
			if !slices.Contains(names[x.Metric], name) {
				names[x.Metric] = append(names[x.Metric], name)
			}
		}
	}

	var out []Check
	for _, metric := range slices.Sorted(maps.Keys(names)) {
		var checks []Check
		var series []string // the metric's series no skip names
		for _, name := range slices.Sorted(slices.Values(names[metric])) {
			if exception := cmp.Or(o.Skip[name], o.Skip[metric]); exception != "" {
				checks = append(checks, s.skipped(name, exception))
				continue
			}
			series = append(series, name)
		}
		switch {
		case len(series) == 0:
		case slices.Contains(selfMeasuring, metric):
			checks = append(checks, s.selfMeasuring(metric, series))
		case expectedRule(metric) != "":
			checks = append(checks, s.expected(metric, series))
		default:
			for _, name := range series {
				checks = append(checks, s.bracket(name, o.LooseWALCount))
			}
		}
		for i := range checks {
			checks[i].Gated = o.Gate != nil && o.Gate(metric)
		}
		out = append(out, checks...)
	}

	return out
}

// Failures returns the gated checks that do not pass: failed, not compared, or on one side only.
func Failures(checks []Check) []Check {
	var out []Check
	for _, c := range checks {
		if failing(c) {
			out = append(out, c)
		}
	}

	return out
}

// Retry returns first's failures but the gauges, plus second's failing checks of those gauges. A failing
// gauge that second does not hold stays failing.
func Retry(first, second []Check) []Check {
	again := map[string]Check{}
	for _, c := range second {
		again[c.Series] = c
	}
	var out []Check
	for _, c := range Failures(first) {
		if next, ok := again[c.Series]; ok && c.Kind == kindGauge {
			if failing(next) {
				out = append(out, next)
			}
			continue
		}
		out = append(out, c)
	}

	return out
}

func failing(c Check) bool {
	return c.Gated && c.Verdict != verdictPass && c.Verdict != verdictExpected && c.Verdict != verdictSkipped
}

type sides struct{ before, after, ours map[string]Series }

func byName(series []Series) map[string]Series {
	out := make(map[string]Series, len(series))
	for _, s := range series {
		out[s.Name] = s
	}

	return out
}

// samples returns name's sample on each side, nil where absent.
func (s sides) samples(name string) (before, ours, after *Series) {
	pick := func(side map[string]Series) *Series {
		if x, ok := side[name]; ok {
			return &x
		}
		return nil
	}

	return pick(s.before), pick(s.ours), pick(s.after)
}

func (s sides) check(name, rule string) Check {
	c := Check{Series: name, Rule: rule}
	c.Before, c.Ours, c.After = s.samples(name)
	for _, x := range []*Series{c.Ours, c.Before, c.After} {
		if x != nil {
			c.Metric, c.Kind = x.Metric, x.Kind
			break
		}
	}

	return c
}

func (s sides) skipped(name, exception string) Check {
	c := s.check(name, exception)
	c.Verdict, c.Detail = verdictSkipped, "named exception "+exception

	return c
}

func (s sides) bracket(name string, looseWAL bool) Check {
	c := s.check(name, ruleBracket)
	b, o, a := c.Before, c.Ours, c.After
	c.Verdict = verdictNotCompared
	switch {
	case o == nil:
		c.Verdict, c.Detail = verdictFail, "only in CNPG"
		return c
	case b == nil && a == nil:
		c.Verdict, c.Detail = verdictFail, "only in ours"
		return c
	case b == nil || a == nil:
		c.Detail = "CNPG has no sample on one side of ours"
		return c
	case b.Kind != o.Kind:
		c.Verdict, c.Detail = verdictFail, fmt.Sprintf("kind: CNPG before %s, ours %s", b.Kind, o.Kind)
		return c
	case a.Kind != o.Kind:
		c.Verdict, c.Detail = verdictFail, fmt.Sprintf("kind: CNPG after %s, ours %s", a.Kind, o.Kind)
		return c
	case b.At.After(o.At.Add(-Margin)):
		c.Detail = fmt.Sprintf("CNPG's before sample is less than %s before ours", Margin)
		return c
	case a.At.Before(o.At.Add(Margin)):
		c.Detail = fmt.Sprintf("CNPG's after sample is less than %s after ours", Margin)
		return c
	}

	ok := between(b.Value, o.Value, a.Value)
	c.Detail = "value " + rangeDetail(b.Value, o.Value, a.Value)
	if o.Kind == kindHistogram {
		ok = ok && between(b.Sum, o.Sum, a.Sum)
		c.Detail = "count " + rangeDetail(b.Value, o.Value, a.Value) + "; sum " + rangeDetail(b.Sum, o.Sum, a.Sum)
	}
	if looseWAL && o.Metric == "cnpg_collector_pg_wal" && (o.Labels["value"] == "count" || o.Labels["value"] == "size") {
		c.Rule = ruleWALRegex
		ok = o.Value <= max(b.Value, a.Value) // false for any NaN
		c.Detail = fmt.Sprintf("ours %s, at most CNPG's larger %s", number(o.Value), number(max(b.Value, a.Value)))
	}
	c.Verdict = verdictFail
	if ok {
		c.Verdict = verdictPass
	}

	return c
}

// between reports whether v lies in [min, max] of b and a, NaN equal to NaN.
func between(b, v, a float64) bool {
	if math.IsNaN(v) || math.IsNaN(b) || math.IsNaN(a) {
		return math.IsNaN(v) && math.IsNaN(b) && math.IsNaN(a)
	}

	return min(b, a) <= v && v <= max(b, a)
}

func rangeDetail(b, v, a float64) string {
	return fmt.Sprintf("ours %s, CNPG [%s, %s]", number(v), number(min(b, a)), number(max(b, a)))
}

func number(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// selfMeasuring passes when ours has metric whenever CNPG has it, with the same label keys.
func (s sides) selfMeasuring(metric string, series []string) Check {
	c := Check{Series: metric, Metric: metric, Rule: ruleSelfMeasuring}
	cnpgKeys, ourKeys := map[string]bool{}, map[string]bool{}
	for _, name := range series {
		b, o, a := s.samples(name)
		for _, x := range []*Series{b, a} {
			if x == nil {
				continue
			}
			c.Kind = x.Kind
			for k := range x.Labels {
				cnpgKeys[k] = true
			}
		}
		if o != nil {
			c.Kind = o.Kind
			for k := range o.Labels {
				ourKeys[k] = true
			}
		}
		c.Before, c.Ours, c.After = firstOf(c.Before, b), firstOf(c.Ours, o), firstOf(c.After, a)
	}
	cnpgList, ourList := slices.Sorted(maps.Keys(cnpgKeys)), slices.Sorted(maps.Keys(ourKeys))
	switch {
	case c.Before == nil && c.After == nil:
		c.Verdict, c.Detail = verdictExpected, "only in ours"
	case c.Ours == nil:
		c.Verdict, c.Detail = verdictFail, "only in CNPG"
	case !slices.Equal(cnpgList, ourList):
		c.Verdict, c.Detail = verdictFail, fmt.Sprintf("label keys: CNPG %v, ours %v", cnpgList, ourList)
	default:
		c.Verdict, c.Detail = verdictPass, fmt.Sprintf("present with label keys %v", ourList)
	}

	return c
}

func firstOf(kept, next *Series) *Series {
	if kept != nil {
		return kept
	}

	return next
}

// expectedRule returns the rule under which metric is expected on one side only, or "".
func expectedRule(metric string) string {
	switch {
	case queryCache[metric] != "":
		return ruleQueryCache
	case strings.HasPrefix(metric, "go_"), strings.HasPrefix(metric, "process_"):
		return ruleCNPGOnly
	case strings.HasPrefix(metric, "cnpg_otel_"), strings.HasPrefix(metric, "otel."), strings.HasPrefix(metric, "go."):
		return ruleOursOnly
	}

	return ""
}

// expected passes metric when it is on its rule's side only.
func (s sides) expected(metric string, series []string) Check {
	rule := expectedRule(metric)
	c := Check{Series: metric, Metric: metric, Rule: rule, Verdict: verdictExpected}
	var inCNPG, inOurs bool
	for _, name := range series {
		b, o, a := s.samples(name)
		for _, x := range []*Series{b, a, o} {
			if x != nil {
				c.Kind = x.Kind
			}
		}
		inCNPG = inCNPG || b != nil || a != nil
		inOurs = inOurs || o != nil
	}
	if rule == ruleOursOnly {
		c.Detail = "on our side only"
		if inCNPG {
			c.Verdict, c.Detail = verdictFail, "expected on our side only, found in CNPG"
		}

		return c
	}
	c.Detail = "on CNPG's side only"
	if rule == ruleQueryCache {
		c.Detail += "; ours: " + queryCache[metric]
	}
	if inOurs {
		c.Verdict, c.Detail = verdictFail, "expected on CNPG's side only, found in ours"
	}

	return c
}
