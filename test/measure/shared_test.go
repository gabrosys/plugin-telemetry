//go:build e2e

package measure_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
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

// measuredExporter is the default TelemetryExporter of namespace with a 30 s interval.
func measuredExporter(namespace string) *v1alpha1.TelemetryExporter {
	e := framework.ExporterFor(namespace, podpatch.DefaultExporter)
	e.Spec.Metrics.Interval = &metav1.Duration{Duration: exportInterval}

	return e
}

// measureEnv reads the environment, waits for the plugin and the collector, and returns the clients and the output directory.
func measureEnv(ctx SpecContext) (env framework.Env, out string, c client.Client, cfg *rest.Config) {
	GinkgoHelper()

	env, err := framework.LoadEnv(os.Getenv)
	Expect(err).NotTo(HaveOccurred())
	out = os.Getenv("MEASURE_OUT")
	Expect(out).NotTo(BeEmpty(), "MEASURE_OUT")
	c, cfg, err = framework.NewClients(env.KubeContext)
	Expect(err).NotTo(HaveOccurred())

	Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
		WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())
	Expect(framework.CollectorReady(ctx, c)).To(Succeed())

	return env, out, c, cfg
}

// writeMeasurement writes m to $out/<name>-<cnpg>.json and adds it to the report.
func writeMeasurement(out, name, cnpg string, m any) {
	GinkgoHelper()

	data, err := json.MarshalIndent(m, "", "  ")
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(filepath.Join(out, name+"-"+cnpg+".json"), append(data, '\n'), 0o644)).To(Succeed()) //nolint:gosec // a report file
	AddReportEntry(name, string(data))
}
