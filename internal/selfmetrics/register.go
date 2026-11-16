package selfmetrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	operationDuration = "otel.sdk.exporter.operation.duration"
	// HookCalls is the plugin's hook call counter.
	HookCalls = "cnpg_otel_plugin_hook_calls_total"
	// HookDuration is the plugin's hook duration histogram.
	HookDuration = "cnpg_otel_plugin_hook_duration_seconds"
	// IdentityReconciles is the plugin's identity reconcile counter.
	IdentityReconciles = "cnpg_otel_plugin_identity_reconciles_total"
)

// contextForRecord is the context for histogram recordings; Export has no caller context.
var contextForRecord = context.Background()

// Views returns the views a MeterProvider needs for the catalogue: the duration
// histograms have no bucket boundaries, which an instrument option cannot express.
func Views() []sdkmetric.View {
	single := sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: []float64{}}}
	return []sdkmetric.View{
		sdkmetric.NewView(sdkmetric.Instrument{Name: operationDuration}, single),
		sdkmetric.NewView(sdkmetric.Instrument{Name: HookDuration}, single),
	}
}

type observed struct {
	name    string
	counter metric.Int64Observable
	gauge   metric.Float64Observable
}

// Register creates one observable instrument per catalogue entry that reads s,
// and the synchronous operation duration histogram that Export records into.
func Register(m metric.Meter, s *Stats) error {
	return register(m, s, ByComponent(ComponentEngine))
}

// register is Register over the given metrics. It fails for a counter or gauge that s
// has no reader for, so the callback never returns an error.
func register(m metric.Meter, s *Stats, ms []Metric) error {
	var obs []observed
	var instruments []metric.Observable
	for _, def := range ms {
		switch Instrument(def.Instrument) {
		case InstrumentCounter:
			if _, ok := s.points(def.Name); !ok {
				return fmt.Errorf("metric %s: no reader", def.Name)
			}
			c, err := m.Int64ObservableCounter(def.Name, metric.WithUnit(def.Unit), metric.WithDescription(def.Description))
			if err != nil {
				return fmt.Errorf("create counter %s: %w", def.Name, err)
			}
			obs = append(obs, observed{name: def.Name, counter: c})
			instruments = append(instruments, c)
		case InstrumentGauge:
			if _, ok := s.points(def.Name); !ok {
				return fmt.Errorf("metric %s: no reader", def.Name)
			}
			g, err := m.Float64ObservableGauge(def.Name, metric.WithUnit(def.Unit), metric.WithDescription(def.Description))
			if err != nil {
				return fmt.Errorf("create gauge %s: %w", def.Name, err)
			}
			obs = append(obs, observed{name: def.Name, gauge: g})
			instruments = append(instruments, g)
		case InstrumentHistogram:
			if def.Name != operationDuration {
				return fmt.Errorf("histogram %s: no recorder", def.Name)
			}
			h, err := m.Float64Histogram(def.Name,
				metric.WithUnit(def.Unit), metric.WithDescription(def.Description),
				metric.WithExplicitBucketBoundaries())
			if err != nil {
				return fmt.Errorf("create histogram %s: %w", def.Name, err)
			}
			s.mu.Lock()
			s.duration = h
			s.mu.Unlock()
		default:
			return fmt.Errorf("metric %s: unknown instrument %q", def.Name, def.Instrument)
		}
	}

	_, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, ob := range obs {
			pts, _ := s.points(ob.name)
			for _, p := range pts {
				opt := metric.WithAttributes(p.attrs...)
				if ob.counter != nil {
					o.ObserveInt64(ob.counter, int64(p.value), opt)
				} else {
					o.ObserveFloat64(ob.gauge, p.value, opt)
				}
			}
		}
		return nil
	}, instruments...)
	if err != nil {
		return fmt.Errorf("register callback: %w", err)
	}
	return nil
}
