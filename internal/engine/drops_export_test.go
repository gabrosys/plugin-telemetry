package engine

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

// DroppedSeries stores two entries with a series in common and a histogram without data, collects the
// engine's metrics, and returns cnpg_otel_query_dropped_series_total by reason.
func DroppedSeries(ctx context.Context) (map[string]int64, error) {
	e, err := newEngine(ctx, Options{Config: FileConfig("engine.yaml")}, nil)
	if err != nil {
		return nil, err
	}
	at := time.Now()
	gauge := queries.Sample{Metric: "shared", Kind: queries.KindGauge, Value: 1}
	if err := e.store.Put(snapshot.Key{Query: "a", Database: "app"}, "f", []queries.Sample{gauge}, at); err != nil {
		return nil, fmt.Errorf("put a: %w", err)
	}
	empty := queries.Sample{Metric: "h", Kind: queries.KindHistogram}
	if err := e.store.Put(snapshot.Key{Query: "b", Database: "app"}, "f", []queries.Sample{gauge, empty}, at); err != nil {
		return nil, fmt.Errorf("put b: %w", err)
	}

	// The producer counts the drops while a collection runs, so the second collection reports them.
	var rm metricdata.ResourceMetrics
	for range 2 {
		if err := e.reader.Collect(ctx, &rm); err != nil {
			return nil, fmt.Errorf("collect: %w", err)
		}
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != "cnpg_otel_query_dropped_series_total" || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				reason, _ := dp.Attributes.Value("reason")
				got[reason.AsString()] += dp.Value
			}
		}
	}

	return got, e.mp.Shutdown(ctx)
}
