package parity

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	podAttribute = "k8s.pod.name"
	// lastSuccess is when a query last ran with outcome ok, by query and database, in Unix seconds.
	lastSuccess = "cnpg_otel_query_last_success_timestamp_seconds"
)

// Ours reads pod's newest export in lines, found by its k8s.pod.name resource attribute. At is the last
// success, in that export, of the last of queryOf[metric] that has one there, on the database the series'
// database or datname label names, else on the oldest; without such a query it is the point time. The series
// are sorted by Name.
func Ours(lines []otlp.Line, pod string, queryOf map[string][]string) []Series {
	podLines := otlp.ForResource(lines, podAttribute, pod)
	times := otlp.ExportTimes(podLines)
	if len(times) == 0 {
		return nil
	}
	export := otlp.ExportAt(podLines, times[len(times)-1])

	successes := map[string]map[string]float64{} // query, then database
	for _, p := range otlp.Points(export, lastSuccess) {
		query := p.Attrs["query"]
		if successes[query] == nil {
			successes[query] = map[string]float64{}
		}
		successes[query][p.Attrs["database"]] = p.Value
	}

	names := map[string]bool{}
	for _, scope := range otlp.Scopes(export) {
		for _, name := range otlp.Names(export, scope.Name) {
			names[name] = true
		}
	}
	var out []Series
	for name := range names {
		add := func(s Series) {
			s.Name, s.Metric = seriesName(name, s.Labels), name
			for _, query := range slices.Backward(queryOf[name]) {
				if byDatabase := successes[query]; len(byDatabase) > 0 {
					s.At = successAt(byDatabase, s.Labels)
					break
				}
			}
			out = append(out, s)
		}
		for _, p := range otlp.Points(export, name) {
			kind := kindGauge
			if p.Monotonic {
				kind = kindCounter
			}
			add(Series{Kind: kind, Labels: p.Attrs, Value: p.Value, At: p.Time})
		}
		for _, p := range otlp.HistogramPoints(export, name) {
			add(Series{Kind: kindHistogram, Labels: p.Attrs, Value: float64(p.Count), Sum: p.Sum, At: p.Time})
		}
	}
	slices.SortStableFunc(out, func(a, b Series) int { return strings.Compare(a.Name, b.Name) })

	return out
}

func successAt(byDatabase map[string]float64, labels map[string]string) time.Time {
	for _, key := range []string{"database", "datname"} {
		if at, ok := byDatabase[labels[key]]; ok {
			return UnixSeconds(at)
		}
	}

	return UnixSeconds(slices.Min(slices.Collect(maps.Values(byDatabase))))
}

// QueryIndex maps each metric of the queries in sets to the keys of the queries that define it, sorted,
// name overrides included.
func QueryIndex(sets ...queries.UserQueries) map[string][]string {
	out := map[string][]string{}
	for _, set := range sets {
		for key, q := range set {
			for _, column := range queries.Define(key, q, queries.Options{}).Columns {
				if column.Role == queries.RoleValue && !slices.Contains(out[column.Metric], key) {
					out[column.Metric] = append(out[column.Metric], key)
				}
			}
		}
	}
	for metric := range out {
		slices.Sort(out[metric])
	}

	return out
}
