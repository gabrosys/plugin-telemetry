// Package export turns the snapshot into OTLP data and sends it.
package export

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/safe"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

// ScopeName is the instrumentation scope of the exported metrics.
const ScopeName = "github.com/gabrosys/plugin-telemetry"

// Producer is an sdkmetric.Producer over the snapshot. It never returns an error: an
// error from a Producer drops the whole collection (sdk/metric v1.47.0 periodic_reader.go:265-277).
type Producer struct {
	store        *snapshot.Store
	clk          clock.PassiveClock
	onDuplicates func(map[snapshot.Key]int)
	extra        func() []queries.Sample
}

// NewProducer returns a Producer reading store. onDuplicates may be nil; it receives the
// dropped series per Key once per Produce when there are any.
// extra, when not nil, is called once per Produce; its samples join the snapshot's points of the same name.
// A panic in extra drops only its samples (safe.Call): a Producer error drops the whole collection.
// An extra sample wins over a snapshot series of its name and labels, or of its name and another kind;
// the snapshot's series counts as a duplicate. Of repeated extra samples the first is kept.
func NewProducer(
	store *snapshot.Store, clk clock.PassiveClock, onDuplicates func(map[snapshot.Key]int), extra func() []queries.Sample,
) *Producer {
	return &Producer{store: store, clk: clk, onDuplicates: onDuplicates, extra: extra}
}

// metricBuilder collects the data points of one metric name.
type metricBuilder struct {
	name, description string
	kind              queries.Kind
	scalars           []metricdata.DataPoint[float64]
	histograms        []metricdata.HistogramDataPoint[float64]
}

// Produce converts the snapshot into one ScopeMetrics.
func (p *Producer) Produce(ctx context.Context) ([]metricdata.ScopeMetrics, error) {
	var extra []queries.Sample
	if p.extra != nil {
		safe.Call(log.FromContext(ctx).GetLogger(), "extra samples", func() { extra = p.extra() })
	}
	points, duplicates := p.store.Points()
	if duplicates == nil {
		duplicates = make(map[snapshot.Key]int)
	}
	now := p.clk.Now()

	all := make([]snapshot.Point, 0, len(extra)+len(points))
	extraIDs := make(map[string]struct{}, len(extra))
	for _, s := range extra {
		id := queries.SeriesID(s.Metric, s.Labels)
		if _, dup := extraIDs[id]; dup {
			continue
		}
		extraIDs[id] = struct{}{}
		all = append(all, snapshot.Point{
			Metric: s.Metric, Description: s.Description, Kind: s.Kind, Labels: s.Labels, Value: s.Value, Histogram: s.Histogram,
		})
	}
	for _, pt := range points {
		if _, dup := extraIDs[queries.SeriesID(pt.Metric, pt.Labels)]; dup {
			duplicates[pt.Key]++

			continue
		}
		all = append(all, pt)
	}

	var order []*metricBuilder
	byName := make(map[string]*metricBuilder)
	for _, pt := range all {
		b, ok := byName[pt.Metric]
		switch {
		case !ok:
			b = &metricBuilder{name: pt.Metric, description: pt.Description, kind: pt.Kind}
			byName[pt.Metric] = b
			order = append(order, b)
		case b.kind != pt.Kind:
			duplicates[pt.Key]++

			continue
		}

		attrs := labelSet(pt.Labels)
		if pt.Kind == queries.KindHistogram {
			if pt.Histogram == nil {
				continue
			}
			b.histograms = append(b.histograms, histogramPoint(pt, attrs, now))

			continue
		}
		b.scalars = append(b.scalars, metricdata.DataPoint[float64]{
			Attributes: attrs,
			StartTime:  pt.Start,
			Time:       now,
			Value:      pt.Value,
		})
	}

	if len(duplicates) > 0 && p.onDuplicates != nil {
		p.onDuplicates(duplicates)
	}

	metrics := make([]metricdata.Metrics, 0, len(order))
	for _, b := range order {
		if m, ok := b.build(); ok {
			metrics = append(metrics, m)
		}
	}
	if len(metrics) == 0 {
		return nil, nil
	}

	return []metricdata.ScopeMetrics{{
		Scope:   instrumentation.Scope{Name: ScopeName, Version: version.Version},
		Metrics: metrics,
	}}, nil
}

func (b *metricBuilder) build() (metricdata.Metrics, bool) {
	m := metricdata.Metrics{Name: b.name, Description: b.description}
	switch b.kind {
	case queries.KindCounter:
		m.Data = metricdata.Sum[float64]{
			DataPoints:  b.scalars,
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
		}
	case queries.KindGauge:
		m.Data = metricdata.Gauge[float64]{DataPoints: b.scalars}
	case queries.KindHistogram:
		m.Data = metricdata.Histogram[float64]{
			DataPoints:  b.histograms,
			Temporality: metricdata.CumulativeTemporality,
		}
	default:
		return m, false
	}

	return m, len(b.scalars)+len(b.histograms) > 0
}

func labelSet(labels []queries.Label) attribute.Set {
	kvs := make([]attribute.KeyValue, len(labels))
	for i, l := range labels {
		kvs[i] = attribute.String(l.Key, l.Value)
	}

	return attribute.NewSet(kvs...)
}

// histogramPoint converts CNPG's cumulative bucket counts to OTLP per-bucket counts. OTLP needs
// len(BucketCounts) == len(Bounds)+1, so an explicit +Inf bound is dropped and the overflow
// bucket is what remains of Count.
func histogramPoint(pt snapshot.Point, attrs attribute.Set, now time.Time) metricdata.HistogramDataPoint[float64] {
	h := pt.Histogram
	bounds := slices.Clone(h.Bounds)
	cumulative := slices.Clone(h.Cumulative)
	if n := len(bounds); n > 0 && math.IsInf(bounds[n-1], 1) {
		bounds = bounds[:n-1]
		if len(cumulative) >= n {
			cumulative = cumulative[:n-1]
		}
	}

	counts := make([]uint64, len(bounds)+1)
	var prev uint64
	for i := range bounds {
		if i >= len(cumulative) {
			break
		}
		counts[i] = satSub(cumulative[i], prev)
		prev = cumulative[i]
	}
	counts[len(bounds)] = satSub(h.Count, prev)

	return metricdata.HistogramDataPoint[float64]{
		Attributes:   attrs,
		StartTime:    pt.Start,
		Time:         now,
		Count:        h.Count,
		Bounds:       bounds,
		BucketCounts: counts,
		Sum:          h.Sum,
	}
}

func satSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}

	return a - b
}
