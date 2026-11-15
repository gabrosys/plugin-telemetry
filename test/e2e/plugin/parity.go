// Package plugin compares an instance's CNPG metrics endpoint with what the sidecar exports for it.
package plugin

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/contrib/instrumentation/runtime"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	podAttribute = "k8s.pod.name"
	// counterTolerance is how far below CNPG's value ours may be, as a fraction of CNPG's.
	counterTolerance = 0.1
	absent           = "absent"
	// cnpgUpdated is when CNPG last computed its query metrics, in whole Unix seconds.
	cnpgUpdated = "cnpg_last_update_timestamp"
	// ourSuccess is when a query of ours last ran with outcome ok, by query and database.
	ourSuccess = "cnpg_otel_query_last_success_timestamp_seconds"
)

var (
	// ErrNoCNPGMetrics is returned when CNPG's text holds no sample.
	ErrNoCNPGMetrics = errors.New("the CNPG text holds no metric")
	// ErrNoPodLines is returned when no line holds a resource of the pod.
	ErrNoPodLines = errors.New("no exported line holds the pod")
)

// cnpg_collector_* metrics the engine already exports.
var comparedCollector = []string{"cnpg_collector_up", "cnpg_collector_lo_pages"}

// Parity compares one instance's CNPG endpoint (Prometheus text) with our export for the same
// instance (OTLP lines), by metric name and label keys, and by value where both have a sample.
// OnlyCNPG, OnlyOurs, LabelDiffs and ValueDiffs hold unexpected differences only. Every slice is sorted.
type Parity struct {
	OnlyCNPG, OnlyOurs     []string
	LabelDiffs, ValueDiffs []Diff
	// ExpectedCNPG and ExpectedOurs are the metrics that only one side is expected to have.
	ExpectedCNPG, ExpectedOurs []string
	// Gauges are the gauge series that are not constants, with both values. They are never differences.
	Gauges []Diff
	// NotCompared are the counter and histogram series whose values weren't checked, with the reason.
	NotCompared []Diff
	// Checked counts the counter and histogram series whose values were checked.
	Checked int
}

// CNPGSampleTime returns CNPG's cnpg_last_update_timestamp: when it last computed its query metrics, in
// whole seconds. ok is false when the text doesn't hold it.
func CNPGSampleTime(promText []byte) (time.Time, bool, error) {
	families, err := cnpgFamilies(promText)
	if err != nil {
		return time.Time{}, false, err
	}
	at, ok := cnpgUpdatedAt(families)

	return unixTime(at), ok, nil
}

// OurSampleTime returns when query last ran with outcome ok on pod, from pod's newest export in lines;
// for a query that runs on several databases, the oldest of their times.
func OurSampleTime(lines []otlp.Line, pod, query string) (time.Time, bool) {
	databases := querySuccesses(otlp.ForResource(lines, podAttribute, pod))[query]
	if len(databases) == 0 {
		return time.Time{}, false
	}

	return unixTime(slices.Min(slices.Collect(maps.Values(databases)))), true
}

// Diff is a difference on a metric, or on a series written name{key="value",...}.
type Diff struct{ Metric, Detail string }

type kind string

const (
	kindCounter     kind = "counter"
	kindGauge       kind = "gauge"
	kindHistogram   kind = "histogram"
	kindSummary     kind = "summary"
	kindExponential kind = "exponential histogram"
)

// family is one metric of one side. Its series are keyed by labelString.
type family struct {
	kind   kind
	scope  string // ours only
	keys   []string
	series map[string]sample
}

// sample is a series' newest value, or for a histogram its count and sum.
type sample struct {
	labels     map[string]string
	value, sum float64
	constant   bool // ours only: at least 2 points, all with one value
}

// sampleTimes says when each side computed its values, in Unix seconds.
type sampleTimes struct {
	cnpg     float64
	hasCNPG  bool
	ours     map[string]map[string]float64 // by query, then database, from the newest export
	interval float64                       // median gap between the pod's exports; 0 with one export
}

// Compare compares CNPG's text for pod with the lines that hold pod's resource, found by its
// k8s.pod.name attribute. Counters and histograms are value-checked only when CNPG's
// cnpg_last_update_timestamp and our query's last success are at most one export interval apart, the
// interval being the median gap between pod's exports in lines. Compare fails when the text doesn't
// parse or holds no metric, and when no line holds pod.
func Compare(promText []byte, lines []otlp.Line, pod string) (Parity, error) {
	cnpg, err := cnpgFamilies(promText)
	if err != nil {
		return Parity{}, err
	}
	podLines := otlp.ForResource(lines, podAttribute, pod)
	if len(podLines) == 0 {
		return Parity{}, fmt.Errorf("%w: %s", ErrNoPodLines, pod)
	}
	ours, err := ourFamilies(podLines)
	if err != nil {
		return Parity{}, err
	}
	times := newSampleTimes(cnpg, podLines)

	selfMetrics := map[string]bool{}
	for _, m := range selfmetrics.ByComponent(selfmetrics.ComponentEngine) {
		selfMetrics[m.Name] = true
	}

	var p Parity
	for _, name := range unionKeys(cnpg, ours) {
		c, inCNPG := cnpg[name]
		o, inOurs := ours[name]
		switch {
		case !inOurs && expectedCNPG(name):
			p.ExpectedCNPG = append(p.ExpectedCNPG, name)
		case !inOurs:
			p.OnlyCNPG = append(p.OnlyCNPG, name)
		case !inCNPG && (o.scope == runtime.ScopeName || strings.HasPrefix(name, "cnpg_otel_") || selfMetrics[name]):
			p.ExpectedOurs = append(p.ExpectedOurs, name)
		case !inCNPG:
			p.OnlyOurs = append(p.OnlyOurs, name)
		default:
			p.compare(name, c, o, times)
		}
	}
	p.sort()

	return p, nil
}

func expectedCNPG(name string) bool {
	if slices.Contains(comparedCollector, name) {
		return false
	}

	return strings.HasPrefix(name, "cnpg_collector_") || strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_")
}

func (p *Parity) compare(name string, c, o family, times sampleTimes) {
	if c.kind != o.kind {
		p.ValueDiffs = append(p.ValueDiffs, Diff{name, fmt.Sprintf("type: CNPG %s, ours %s", c.kind, o.kind)})
		if !scalar(c.kind) || !scalar(o.kind) {
			return
		}
	}
	if !slices.Equal(c.keys, o.keys) {
		p.LabelDiffs = append(p.LabelDiffs, Diff{name, labelDetail(c.keys, o.keys)})
		return
	}

	for _, labels := range unionKeys(c.series, o.series) {
		cv, inCNPG := c.series[labels]
		ov, inOurs := o.series[labels]
		series := seriesName(name, labels)
		attrs := ov.labels
		if !inOurs {
			attrs = cv.labels
		}
		switch o.kind {
		case kindCounter:
			if reason, ok := times.close(name, attrs); !ok {
				p.NotCompared = append(p.NotCompared, Diff{series, "counter: " + reason + "; " + values(cv.value, inCNPG, ov.value, inOurs)})
				continue
			}
			p.Checked++
			if d, ok := counterDiff(cv.value, inCNPG, ov.value, inOurs); !ok {
				p.ValueDiffs = append(p.ValueDiffs, Diff{series, "counter: " + d})
			}
		case kindHistogram:
			if reason, ok := times.close(name, attrs); !ok {
				p.NotCompared = append(p.NotCompared, Diff{series, fmt.Sprintf("histogram: %s; count %s; sum %s",
					reason, values(cv.value, inCNPG, ov.value, inOurs), values(cv.sum, inCNPG, ov.sum, inOurs))})
				continue
			}
			p.Checked++
			for _, part := range []struct {
				name       string
				cnpg, ours float64
			}{{"count", cv.value, ov.value}, {"sum", cv.sum, ov.sum}} {
				if d, ok := counterDiff(part.cnpg, inCNPG, part.ours, inOurs); !ok {
					p.ValueDiffs = append(p.ValueDiffs, Diff{series, part.name + ": " + d})
				}
			}
		case kindGauge:
			switch {
			case inCNPG && inOurs && ov.constant:
				if !same(cv.value, ov.value) {
					p.ValueDiffs = append(p.ValueDiffs, Diff{
						series,
						"constant: " + values(cv.value, inCNPG, ov.value, inOurs) + "; " + times.gapDetail(name, attrs),
					})
				}
			default:
				p.Gauges = append(p.Gauges, Diff{series, values(cv.value, inCNPG, ov.value, inOurs)})
			}
		case kindSummary, kindExponential:
			// Names and label keys only.
		}
	}
}

func scalar(k kind) bool { return k == kindCounter || k == kindGauge }

func newSampleTimes(cnpg map[string]family, podLines []otlp.Line) sampleTimes {
	var t sampleTimes
	t.cnpg, t.hasCNPG = cnpgUpdatedAt(cnpg)

	exports := make([]float64, 0, len(podLines))
	for _, l := range podLines {
		exports = append(exports, float64(l.Time.UnixNano())/1e9)
	}
	slices.Sort(exports)
	exports = slices.Compact(exports)
	if len(exports) > 1 {
		gaps := make([]float64, 0, len(exports)-1)
		for i := 1; i < len(exports); i++ {
			gaps = append(gaps, exports[i]-exports[i-1])
		}
		t.interval = median(gaps)
	}
	t.ours = querySuccesses(podLines)

	return t
}

func cnpgUpdatedAt(cnpg map[string]family) (float64, bool) {
	s, ok := cnpg[cnpgUpdated].series[""]
	return s.value, ok
}

// querySuccesses returns our last success times by query and database, from the newest export in podLines.
func querySuccesses(podLines []otlp.Line) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	if len(podLines) == 0 {
		return out
	}
	newest := slices.MaxFunc(podLines, func(a, b otlp.Line) int { return a.Time.Compare(b.Time) }).Time
	var last []otlp.Line
	for _, l := range podLines {
		if l.Time.Equal(newest) {
			last = append(last, l)
		}
	}
	for _, pt := range otlp.Points(last, ourSuccess) {
		query, database := pt.Attrs["query"], pt.Attrs["database"]
		if out[query] == nil {
			out[query] = map[string]float64{}
		}
		out[query][database] = pt.Value
	}

	return out
}

func unixTime(seconds float64) time.Time {
	whole := math.Floor(seconds)
	return time.Unix(int64(whole), int64((seconds-whole)*1e9))
}

func median(xs []float64) float64 {
	sorted := slices.Sorted(slices.Values(xs))
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}

	return (sorted[mid-1] + sorted[mid]) / 2
}

// close reports whether CNPG's and our sample times of the series of metric with labels are at most one
// export interval apart, or why not.
func (t sampleTimes) close(metric string, labels map[string]string) (string, bool) {
	gap, unknown := t.gap(metric, labels)
	switch {
	case unknown != "":
		return unknown, false
	case t.interval == 0:
		return "one export, no export interval", false
	case gap > t.interval:
		return "sample times " + strconv.FormatFloat(gap, 'f', 1, 64) + " s apart", false
	}

	return "", true
}

// gapDetail describes the gap between the sample times of the series of metric with labels.
func (t sampleTimes) gapDetail(metric string, labels map[string]string) string {
	gap, unknown := t.gap(metric, labels)
	if unknown != "" {
		return "sample time unknown: " + unknown
	}

	return "sample times " + strconv.FormatFloat(gap, 'f', 1, 64) + " s apart"
}

// gap returns how far apart CNPG's and our sample times of the series of metric with labels are, or why
// that is unknown. Our time is that of the query with the longest cnpg_<query>_ prefix of metric, on the
// series' database or datname when the query ran there, else on the query's oldest database.
func (t sampleTimes) gap(metric string, labels map[string]string) (float64, string) {
	if !t.hasCNPG {
		return 0, "no CNPG time"
	}
	var query string
	for q := range t.ours {
		if strings.HasPrefix(metric, "cnpg_"+q+"_") && len(q) > len(query) {
			query = q
		}
	}
	if query == "" {
		return 0, "no query match"
	}

	databases := t.ours[query]
	ours, ok := databases[labels["database"]]
	if !ok {
		ours, ok = databases[labels["datname"]]
	}
	if !ok {
		ours = slices.Min(slices.Collect(maps.Values(databases)))
	}

	return math.Abs(t.cnpg - ours), ""
}

// counterDiff applies the counter rule: ours ≤ CNPG and CNPG − ours ≤ 10 % of CNPG, NaN equal to NaN.
func counterDiff(cnpg float64, inCNPG bool, ours float64, inOurs bool) (string, bool) {
	switch {
	case !inCNPG || !inOurs:
		return values(cnpg, inCNPG, ours, inOurs), false
	case math.IsNaN(cnpg) && math.IsNaN(ours):
		return "", true
	case math.IsNaN(cnpg) || math.IsNaN(ours):
		return values(cnpg, inCNPG, ours, inOurs), false
	case ours > cnpg:
		return fmt.Sprintf("ours %s above CNPG %s", number(ours), number(cnpg)), false
	case cnpg-ours > counterTolerance*cnpg:
		return fmt.Sprintf("ours %s more than 10 %% below CNPG %s", number(ours), number(cnpg)), false
	}

	return "", true
}

func same(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }

func values(cnpg float64, inCNPG bool, ours float64, inOurs bool) string {
	show := func(v float64, ok bool) string {
		if !ok {
			return absent
		}
		return number(v)
	}

	return fmt.Sprintf("CNPG %s, ours %s", show(cnpg, inCNPG), show(ours, inOurs))
}

func number(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

func labelDetail(cnpg, ours []string) string {
	var parts []string
	if only := without(cnpg, ours); len(only) > 0 {
		parts = append(parts, "keys only in CNPG: "+strings.Join(only, ", "))
	}
	if only := without(ours, cnpg); len(only) > 0 {
		parts = append(parts, "keys only in ours: "+strings.Join(only, ", "))
	}

	return strings.Join(append(parts, "values not compared"), "; ")
}

func without(a, b []string) []string {
	var out []string
	for _, k := range a {
		if !slices.Contains(b, k) {
			out = append(out, k)
		}
	}

	return out
}

func (p *Parity) sort() {
	for _, names := range []*[]string{&p.OnlyCNPG, &p.OnlyOurs, &p.ExpectedCNPG, &p.ExpectedOurs} {
		slices.Sort(*names)
	}
	for _, diffs := range []*[]Diff{&p.LabelDiffs, &p.ValueDiffs, &p.NotCompared, &p.Gauges} {
		slices.SortFunc(*diffs, func(a, b Diff) int {
			return cmp.Or(strings.Compare(a.Metric, b.Metric), strings.Compare(a.Detail, b.Detail))
		})
	}
}

func cnpgFamilies(text []byte) (map[string]family, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	mfs, err := parser.TextToMetricFamilies(bytes.NewReader(text))
	if err != nil {
		return nil, fmt.Errorf("parse the CNPG text: %w", err)
	}
	if len(mfs) == 0 {
		return nil, ErrNoCNPGMetrics
	}

	out := make(map[string]family, len(mfs))
	for name, mf := range mfs {
		f := family{kind: cnpgKind(mf.GetType()), series: map[string]sample{}}
		keys := map[string]bool{}
		for _, m := range mf.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
				keys[lp.GetName()] = true
			}
			s := cnpgSample(m)
			s.labels = labels
			f.series[labelString(labels)] = s
		}
		f.keys = slices.Sorted(maps.Keys(keys))
		out[name] = f
	}

	return out, nil
}

func cnpgKind(t dto.MetricType) kind {
	switch t {
	case dto.MetricType_COUNTER:
		return kindCounter
	case dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
		return kindGauge
	case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
		return kindHistogram
	case dto.MetricType_SUMMARY:
		return kindSummary
	}

	return kind(t.String())
}

func cnpgSample(m *dto.Metric) sample {
	switch {
	case m.GetCounter() != nil:
		return sample{value: m.GetCounter().GetValue()}
	case m.GetGauge() != nil:
		return sample{value: m.GetGauge().GetValue()}
	case m.GetUntyped() != nil:
		return sample{value: m.GetUntyped().GetValue()}
	case m.GetHistogram() != nil:
		h := m.GetHistogram()
		count := float64(h.GetSampleCount())
		if h.SampleCountFloat != nil {
			count = h.GetSampleCountFloat()
		}
		return sample{value: count, sum: h.GetSampleSum()}
	case m.GetSummary() != nil:
		return sample{value: float64(m.GetSummary().GetSampleCount()), sum: m.GetSummary().GetSampleSum()}
	}

	return sample{value: math.NaN()}
}

func ourFamilies(lines []otlp.Line) (map[string]family, error) {
	out := map[string]family{}
	for _, scope := range otlp.Scopes(lines) {
		shapes, err := otlp.Shapes(lines, scope.Name)
		if err != nil {
			return nil, fmt.Errorf("read our %s metrics: %w", scope.Name, err)
		}
		for _, s := range shapes {
			if _, seen := out[s.Name]; seen {
				continue
			}
			out[s.Name] = family{kind: ourKind(s), scope: scope.Name, keys: s.Attributes, series: ourSeries(lines, s)}
		}
	}

	return out, nil
}

func ourKind(s otlp.Shape) kind {
	switch s.Type {
	case otlp.TypeSum:
		if s.Monotonic != nil && *s.Monotonic {
			return kindCounter
		}
		return kindGauge
	case otlp.TypeHistogram:
		return kindHistogram
	case otlp.TypeExponentialHistogram:
		return kindExponential
	case otlp.TypeSummary:
		return kindSummary
	default:
		return kindGauge
	}
}

func ourSeries(lines []otlp.Line, s otlp.Shape) map[string]sample {
	out := map[string]sample{}
	switch s.Type {
	case otlp.TypeGauge, otlp.TypeSum:
		byLabels := map[string][]otlp.Point{}
		for _, pt := range otlp.Points(lines, s.Name) {
			labels := labelString(pt.Attrs)
			byLabels[labels] = append(byLabels[labels], pt)
		}
		for labels, pts := range byLabels {
			s := pointsSample(pts)
			s.labels = pts[0].Attrs
			out[labels] = s
		}
	case otlp.TypeHistogram:
		newest := map[string]otlp.HistogramPoint{}
		for _, pt := range otlp.HistogramPoints(lines, s.Name) {
			labels := labelString(pt.Attrs)
			if last, ok := newest[labels]; !ok || !pt.Time.Before(last.Time) {
				newest[labels] = pt
			}
		}
		for labels, pt := range newest {
			out[labels] = sample{labels: pt.Attrs, value: float64(pt.Count), sum: pt.Sum}
		}
	}

	return out
}

// pointsSample returns the newest value of pts, in file order, and whether they are at least 2 with one value.
func pointsSample(pts []otlp.Point) sample {
	newest := pts[0]
	constant := len(pts) >= 2
	for _, pt := range pts[1:] {
		if !pt.Time.Before(newest.Time) {
			newest = pt
		}
		if !same(pt.Value, pts[0].Value) {
			constant = false
		}
	}

	return sample{value: newest.Value, constant: constant}
}

// labelString writes labels as key="value" pairs sorted by key and joined by commas.
func labelString(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Quote(labels[k]))
	}

	return strings.Join(parts, ",")
}

func seriesName(name, labels string) string {
	if labels == "" {
		return name
	}

	return name + "{" + labels + "}"
}

func unionKeys[V any](a, b map[string]V) []string {
	keys := slices.Collect(maps.Keys(a))
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	return keys
}

// Markdown renders the report under title, with the tolerance rules and one section per field.
func (p Parity) Markdown(title string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", title)
	b.WriteString("Counters, and histogram counts and sums, pass when ours ≤ CNPG and CNPG − ours ≤ 10 % of CNPG. " +
		"They are checked only when CNPG's cnpg_last_update_timestamp and our query's last success are at most " +
		"one export interval apart, and are listed as not compared otherwise. " +
		"A series whose points of ours, at least 2, all hold one value must match exactly whatever their sample times, " +
		"and its difference gives their gap; other gauges are listed and never fail. " +
		"NaN equals NaN.\n\n")
	b.WriteString("CNPG can't see MetricQuerySets, so their metrics are only in ours.\n\n")

	b.WriteString("## Summary\n\n| Section | Count |\n|---|---:|\n")
	for _, row := range []struct {
		name  string
		count int
	}{
		{"Only in CNPG", len(p.OnlyCNPG)},
		{"Only in ours", len(p.OnlyOurs)},
		{"Expected only in CNPG", len(p.ExpectedCNPG)},
		{"Expected only in ours", len(p.ExpectedOurs)},
		{"Label differences", len(p.LabelDiffs)},
		{"Value differences", len(p.ValueDiffs)},
		{"Checked by the counter rule", p.Checked},
		{"Not compared", len(p.NotCompared)},
		{"Gauges", len(p.Gauges)},
	} {
		fmt.Fprintf(&b, "| %s | %d |\n", row.name, row.count)
	}
	b.WriteString("\n")

	writeNames(&b, "## Only in CNPG", p.OnlyCNPG)
	writeNames(&b, "## Only in ours", p.OnlyOurs)
	b.WriteString("## Expected on one side\n\n")
	writeNames(&b, "### CNPG", p.ExpectedCNPG)
	writeNames(&b, "### Ours", p.ExpectedOurs)
	writeDiffs(&b, "## Label differences", p.LabelDiffs)
	writeDiffs(&b, "## Value differences", p.ValueDiffs)
	writeDiffs(&b, "## Not compared", p.NotCompared)
	writeDiffs(&b, "## Gauges", p.Gauges)

	return strings.TrimRight(b.String(), "\n") + "\n"
}

func writeNames(b *strings.Builder, heading string, names []string) {
	b.WriteString(heading + "\n\n")
	if len(names) == 0 {
		b.WriteString("none\n\n")
		return
	}
	for _, n := range names {
		b.WriteString("- " + code(n) + "\n")
	}
	b.WriteString("\n")
}

func writeDiffs(b *strings.Builder, heading string, diffs []Diff) {
	b.WriteString(heading + "\n\n")
	if len(diffs) == 0 {
		b.WriteString("none\n\n")
		return
	}
	for _, d := range diffs {
		b.WriteString("- " + code(d.Metric) + ": " + d.Detail + "\n")
	}
	b.WriteString("\n")
}

// code writes s as a code span, with a fence longer than any run of backticks in s.
func code(s string) string {
	longest, run := 0, 0
	for _, r := range s {
		if r != '`' {
			run = 0
			continue
		}
		run++
		longest = max(longest, run)
	}
	if longest == 0 {
		return "`" + s + "`"
	}
	fence := strings.Repeat("`", longest+1)

	return fence + " " + s + " " + fence
}
