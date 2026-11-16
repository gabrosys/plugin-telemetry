package parity_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/e2e/parity"
)

// query is one query's self-metrics in an export: its interval and, when ok, its last success.
type query struct {
	name     string
	interval time.Duration
	success  time.Time
	ok       bool
}

func gaugePoint(at time.Time, value float64, attrs map[string]string) *metricpb.NumberDataPoint {
	dp := &metricpb.NumberDataPoint{
		TimeUnixNano: uint64(at.UnixNano()),
		Value:        &metricpb.NumberDataPoint_AsDouble{AsDouble: value},
	}
	for k, v := range attrs {
		dp.Attributes = append(dp.Attributes, &commonpb.KeyValue{
			Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
		})
	}

	return dp
}

func gaugeOf(name string, points ...*metricpb.NumberDataPoint) *metricpb.Metric {
	return &metricpb.Metric{Name: name, Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: points}}}
}

// exportLine is one export at at holding config_info and the queries' self-metrics on database postgres.
func exportLine(at time.Time, qs ...query) otlp.Line {
	metrics := []*metricpb.Metric{gaugeOf(otlp.ConfigInfo, gaugePoint(at, 1, nil))}
	for _, q := range qs {
		metrics = append(metrics, gaugeOf("cnpg_otel_query_interval_seconds",
			gaugePoint(at, q.interval.Seconds(), map[string]string{"query": q.name})))
		if q.ok {
			metrics = append(metrics, gaugeOf("cnpg_otel_query_last_success_timestamp_seconds",
				gaugePoint(at, float64(q.success.UnixNano())/1e9, map[string]string{"query": q.name, "database": "postgres"})))
		}
	}
	req := &colmetricpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricpb.ResourceMetrics{{
		ScopeMetrics: []*metricpb.ScopeMetrics{{Metrics: metrics}},
	}}}

	return otlp.Line{Time: at, Request: req}
}

var _ = Describe("FreshExport", func() {
	t0 := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	at := func(seconds float64) time.Time { return t0.Add(time.Duration(seconds * float64(time.Second))) }
	wait := parity.Wait{After: at(1), ExportInterval: 5 * time.Second, Deadline: at(60)}

	// exports returns one export at each time in seconds, every one holding qs.
	exports := func(seconds []float64, qs ...query) []otlp.Line {
		var out []otlp.Line
		for _, s := range seconds {
			out = append(out, exportLine(at(s), qs...))
		}
		return out
	}

	DescribeTable("picks the first export no query holds back",
		func(lines []otlp.Line, w parity.Wait, export, newest float64, stale []string) {
			cut, gotNewest, gotStale := parity.FreshExport(lines, w)
			Expect(gotStale).To(Equal(stale))
			if export < 0 {
				Expect(cut).To(BeEmpty())
				Expect(gotNewest).To(BeZero())
				return
			}
			// The cut ends at the chosen export; Ours reads the newest export in it.
			times := otlp.ExportTimes(cut)
			Expect(times).NotTo(BeEmpty())
			Expect(times[len(times)-1]).To(BeTemporally("==", at(export)))
			Expect(gotNewest).To(BeTemporally("==", at(newest)))
		},
		Entry("fresh: every last success after After",
			exports([]float64{6, 11}, query{"q", 5 * time.Second, at(5), true}), wait, 6.0, 6.0, nil),
		Entry("a last success newer than the export sets newest",
			exports([]float64{6}, query{"q", 5 * time.Second, at(6.5), true}), wait, 6.0, 6.5, nil),
		Entry("an export at or before After holds nothing",
			exports([]float64{1, 6}, query{"q", 5 * time.Second, at(5), true}), wait, 6.0, 6.0, nil),
		Entry("a failing query on a short interval holds the exports for one interval plus one export interval",
			exports([]float64{6, 11, 16}, query{"q", 5 * time.Second, at(-10), true}), wait, 11.0, 11.0, nil),
		Entry("a failing query on a short interval, before its release",
			exports([]float64{6}, query{"q", 5 * time.Second, at(-10), true}), wait, -1.0, 0.0,
			[]string{"q on postgres at 2026-11-16T11:59:50Z"}),
		Entry("a slow query holds nothing",
			exports([]float64{6}, query{"q", 2 * time.Minute, at(-10), true}), wait, 6.0, 6.0, nil),
		Entry("a query whose release leaves no export before the deadline holds nothing",
			exports([]float64{6}, query{"q", 30 * time.Second, at(-10), true}),
			parity.Wait{After: at(1), ExportInterval: 5 * time.Second, Deadline: at(40)}, 6.0, 6.0, nil),
		Entry("the same query with room before the deadline holds the export",
			exports([]float64{6}, query{"q", 30 * time.Second, at(-10), true}), wait, -1.0, 0.0,
			[]string{"q on postgres at 2026-11-16T11:59:50Z"}),
		Entry("a required query holds the exports however long it fails",
			exports([]float64{6, 11, 16, 59}, query{"q", 5 * time.Second, at(-10), true}),
			parity.Wait{After: at(1), ExportInterval: 5 * time.Second, Deadline: at(60), Required: []string{"q"}},
			-1.0, 0.0, []string{"q on postgres at 2026-11-16T11:59:50Z"}),
		Entry("a required query without a last success holds the exports",
			exports([]float64{6, 11, 16}, query{"q", 5 * time.Second, time.Time{}, false}),
			parity.Wait{After: at(1), ExportInterval: 5 * time.Second, Deadline: at(60), Required: []string{"q"}},
			-1.0, 0.0, []string{"q has no last success"}),
		Entry("no export after After",
			exports([]float64{0, 1}, query{"q", 5 * time.Second, at(5), true}), wait, -1.0, 0.0,
			[]string{"the export at 2026-11-16T12:00:01Z"}),
	)

	It("cuts the lines at the next export", func() {
		lines := exports([]float64{6, 11}, query{"q", 5 * time.Second, at(5), true})
		cut, _, _ := parity.FreshExport(lines, wait)
		Expect(cut).To(HaveLen(1))
		Expect(cut[0].Time).To(BeTemporally("==", at(6)))
	})
})

var _ = Describe("Latest", func() {
	It("returns the later time", func() {
		a, b := time.Unix(1, 0), time.Unix(2, 0)
		Expect(parity.Latest(a, b)).To(Equal(b))
		Expect(parity.Latest(b, a)).To(Equal(b))
	})
})
