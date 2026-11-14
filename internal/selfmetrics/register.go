package selfmetrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const operationDuration = "otel.sdk.exporter.operation.duration"

// contextForRecord is the context for histogram recordings; Export has no caller context.
var contextForRecord = context.Background()

// Views returns the views a MeterProvider needs for the catalogue: the operation
// duration histogram has no bucket boundaries, which an instrument option cannot express.
func Views() []sdkmetric.View {
	return []sdkmetric.View{
		sdkmetric.NewView(
			sdkmetric.Instrument{Name: operationDuration},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: []float64{}}},
		),
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
	var obs []observed
	var instruments []metric.Observable
	for _, def := range Catalogue() {
		switch Instrument(def.Instrument) {
		case InstrumentCounter:
			c, err := m.Int64ObservableCounter(def.Name, metric.WithUnit(def.Unit), metric.WithDescription(def.Description))
			if err != nil {
				return fmt.Errorf("create counter %s: %w", def.Name, err)
			}
			obs = append(obs, observed{name: def.Name, counter: c})
			instruments = append(instruments, c)
		case InstrumentGauge:
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
			pts, ok := s.points(ob.name)
			if !ok {
				return fmt.Errorf("observe %s: no reader", ob.name)
			}
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
