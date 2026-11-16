//go:build e2e

package measure_test

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"

	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

// counterDelta sums, over the series of counter with the given outcome, the last value minus the first.
func counterDelta(lines []otlp.Line, counter, outcome string) float64 {
	first, last := map[string]float64{}, map[string]float64{}
	for _, p := range otlp.Matching(otlp.Points(lines, counter), map[string]string{"outcome": outcome}) {
		key := fmt.Sprint(p.Attrs)
		if _, ok := first[key]; !ok {
			first[key] = p.Value
		}
		last[key] = p.Value
	}
	var d float64
	for key, v := range last {
		d += v - first[key]
	}

	return d
}

// exportInterval is the export interval of every measurement.
const exportInterval = 30 * time.Second

// measuredExporter is the framework's exporter with a 30 s interval.
func measuredExporter(namespace, name string) *v1alpha1.TelemetryExporter {
	e := framework.ExporterFor(namespace, name)
	e.Spec.Metrics.Interval = &metav1.Duration{Duration: exportInterval}

	return e
}
