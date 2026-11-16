package operator

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ImageEventHandler exposes imageEventHandler to the external test package.
var ImageEventHandler = imageEventHandler

// Exposed for manager_test.go.
var (
	NewScheme      = newScheme
	ManagerOptions = managerOptions
)

// Exposed for controller_test.go.
var (
	SpecChanged         = specChanged
	OwnedObjects        = ownedObjects
	ExportersToClusters = exportersToClusters
)

// Exposed for metrics_test.go.
func CollectMetrics(ctx context.Context, m *PluginMetrics) (*metricdata.ResourceMetrics, error) {
	rm := &metricdata.ResourceMetrics{}
	if err := m.reader.Collect(ctx, rm); err != nil {
		return nil, fmt.Errorf("collect plugin metrics: %w", err)
	}

	return rm, nil
}

// SetDrainTimeout sets drainTimeout for run_test.go and returns a func that restores it.
func SetDrainTimeout(d time.Duration) (restore func()) {
	previous := drainTimeout
	drainTimeout = d

	return func() { drainTimeout = previous }
}
