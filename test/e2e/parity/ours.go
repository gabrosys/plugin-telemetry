package parity

import (
	"maps"
	"slices"
	"strings"
	"time"

	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	podAttribute = "k8s.pod.name"
	// lastSuccess is when a query last ran with outcome ok, by query and database, in Unix seconds.
	lastSuccess = "cnpg_otel_query_last_success_timestamp_seconds"
)

// Ours reads pod's newest export in lines, found by its k8s.pod.name resource attribute. At is the last
// success of queryOf[metric] in that export, on the database the series' database or datname label names,
// else on the oldest; without a query or a success it is the point time. The series are sorted by Name.
func Ours(lines []otlp.Line, pod string, queryOf map[string]string) []Series {
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

	var out []Series
	for _, l := range export {
		for _, rm := range l.Request.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					for _, s := range points(m) {
						if byDatabase := successes[queryOf[s.Metric]]; len(byDatabase) > 0 {
							s.At = successAt(byDatabase, s.Labels)
						}
						out = append(out, s)
					}
				}
			}
		}
	}
	slices.SortFunc(out, func(a, b Series) int { return strings.Compare(a.Name, b.Name) })

	return out
}

func successAt(byDatabase map[string]float64, labels map[string]string) time.Time {
	for _, key := range []string{"database", "datname"} {
		if at, ok := byDatabase[labels[key]]; ok {
			return unixSeconds(at)
		}
	}

	return unixSeconds(slices.Min(slices.Collect(maps.Values(byDatabase))))
}

// points returns m's data points as series timed by the point.
func points(m *metricpb.Metric) []Series {
	var out []Series
	add := func(kind string, attrs map[string]string, ns uint64, value, sum float64) {
		out = append(out, Series{
			Name: seriesName(m.GetName(), attrs), Metric: m.GetName(), Kind: kind, Labels: attrs,
			Value: value, Sum: sum, At: unixNano(ns),
		})
	}
	switch d := m.GetData().(type) {
	case *metricpb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			add(KindGauge, otlp.Attributes(dp.GetAttributes()), dp.GetTimeUnixNano(), pointValue(dp), 0)
		}
	case *metricpb.Metric_Sum:
		kind := KindGauge
		if d.Sum.GetIsMonotonic() {
			kind = KindCounter
		}
		for _, dp := range d.Sum.GetDataPoints() {
			add(kind, otlp.Attributes(dp.GetAttributes()), dp.GetTimeUnixNano(), pointValue(dp), 0)
		}
	case *metricpb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			add(KindHistogram, otlp.Attributes(dp.GetAttributes()), dp.GetTimeUnixNano(), float64(dp.GetCount()), dp.GetSum())
		}
	case *metricpb.Metric_ExponentialHistogram:
		for _, dp := range d.ExponentialHistogram.GetDataPoints() {
			add(KindHistogram, otlp.Attributes(dp.GetAttributes()), dp.GetTimeUnixNano(), float64(dp.GetCount()), dp.GetSum())
		}
	case *metricpb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			add(KindSummary, otlp.Attributes(dp.GetAttributes()), dp.GetTimeUnixNano(), float64(dp.GetCount()), dp.GetSum())
		}
	}

	return out
}

func pointValue(dp *metricpb.NumberDataPoint) float64 {
	if v, ok := dp.GetValue().(*metricpb.NumberDataPoint_AsInt); ok {
		return float64(v.AsInt)
	}

	return dp.GetAsDouble()
}

func unixNano(ns uint64) time.Time {
	return time.Unix(0, int64(ns)) //nolint:gosec // OTLP times are nanoseconds since the epoch, far below MaxInt64
}

// QueryIndex maps each metric of the queries in sets to its query key, name overrides included. When two
// queries define one metric, the later set wins, and within a set the key sorting last
// (pg_stat_bgwriter_17 over pg_stat_bgwriter).
func QueryIndex(sets ...queries.UserQueries) map[string]string {
	out := map[string]string{}
	for _, set := range sets {
		for _, key := range slices.Sorted(maps.Keys(set)) {
			for _, column := range queries.Define(key, set[key], queries.Options{}).Columns {
				if column.Role == queries.RoleValue {
					out[column.Metric] = key
				}
			}
		}
	}

	return out
}
