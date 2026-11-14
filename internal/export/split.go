package export

import "go.opentelemetry.io/otel/sdk/metric/metricdata"

// Split cuts rm into requests of at most maxPoints data points each, keeping resource and scopes.
func Split(rm *metricdata.ResourceMetrics, maxPoints int) []*metricdata.ResourceMetrics {
	if maxPoints < 1 {
		maxPoints = 1
	}

	var (
		out   []*metricdata.ResourceMetrics
		cur   *metricdata.ResourceMetrics
		used  int
		scope int // index in cur.ScopeMetrics being filled, -1 when none
	)
	start := func() {
		cur = &metricdata.ResourceMetrics{Resource: rm.Resource}
		out = append(out, cur)
		used, scope = 0, -1
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			n := pointCount(m.Data)
			for from := 0; from < n; {
				if cur == nil || used == maxPoints {
					start()
				}
				take := min(n-from, maxPoints-used)
				if scope < 0 {
					cur.ScopeMetrics = append(cur.ScopeMetrics, metricdata.ScopeMetrics{Scope: sm.Scope})
					scope = len(cur.ScopeMetrics) - 1
				}
				part := m
				part.Data = slicePoints(m.Data, from, from+take)
				cur.ScopeMetrics[scope].Metrics = append(cur.ScopeMetrics[scope].Metrics, part)
				used += take
				from += take
			}
		}
		scope = -1
	}

	return out
}

func pointCount(a metricdata.Aggregation) int {
	switch d := a.(type) {
	case metricdata.Gauge[float64]:
		return len(d.DataPoints)
	case metricdata.Sum[float64]:
		return len(d.DataPoints)
	case metricdata.Histogram[float64]:
		return len(d.DataPoints)
	default:
		return 0
	}
}

func slicePoints(a metricdata.Aggregation, from, to int) metricdata.Aggregation {
	switch d := a.(type) {
	case metricdata.Gauge[float64]:
		d.DataPoints = d.DataPoints[from:to:to]

		return d
	case metricdata.Sum[float64]:
		d.DataPoints = d.DataPoints[from:to:to]

		return d
	case metricdata.Histogram[float64]:
		d.DataPoints = d.DataPoints[from:to:to]

		return d
	default:
		return a
	}
}
