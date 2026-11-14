package selfmetrics_test

import (
	"bytes"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

const goldenPath = "../../docs/self-metrics.md"

var _ = Describe("Catalogue", func() {
	It("has the designed rows", func() {
		type row struct {
			name, instrument, unit string
			attributes             []string
		}
		want := []row{
			{"cnpg_otel_query_runs_total", "counter", "{run}", []string{"query", "database", "outcome"}},
			{"cnpg_otel_query_last_duration_seconds", "gauge", "s", []string{"query", "database"}},
			{"cnpg_otel_query_last_success_timestamp_seconds", "gauge", "s", []string{"query", "database"}},
			{"cnpg_otel_query_interval_seconds", "gauge", "s", []string{"query"}},
			{"cnpg_otel_query_capped_series", "gauge", "{series}", []string{"query", "database"}},
			{"cnpg_otel_query_dropped_series_total", "counter", "{series}", []string{"query", "database", "reason"}},
			{"cnpg_otel_series", "gauge", "{series}", nil},
			{"cnpg_otel_config_loads_total", "counter", "{load}", []string{"source", "outcome"}},
			{"cnpg_otel_config_duplicate_queries_total", "counter", "{query}", []string{"source"}},
			{"cnpg_otel_config_info", "gauge", "", []string{"generation", "hash"}},
			{"cnpg_otel_check", "gauge", "", []string{"check"}},
			{"cnpg_otel_exports_total", "counter", "{export}", []string{"outcome"}},
			{"cnpg_otel_last_export_success_timestamp_seconds", "gauge", "s", nil},
			{
				"otel.sdk.exporter.metric_data_point.exported", "counter", "{data_point}",
				[]string{"error.type", "otel.component.type", "otel.component.name", "server.address", "server.port"},
			},
			{
				"otel.sdk.exporter.operation.duration", "histogram", "s",
				[]string{
					"error.type", "http.response.status_code", "otel.component.type",
					"otel.component.name", "server.address", "server.port",
				},
			},
		}

		got := make([]row, 0, len(want))
		seen := map[string]bool{}
		for _, m := range selfmetrics.Catalogue() {
			got = append(got, row{m.Name, m.Instrument, m.Unit, m.Attributes})
			Expect(seen).NotTo(HaveKey(m.Name))
			seen[m.Name] = true
			Expect(m.Description).NotTo(BeEmpty(), m.Name)
		}
		Expect(got).To(Equal(want))
	})

	It("leaves the unit cell empty for a metric without a unit", func() {
		var buf bytes.Buffer
		Expect(selfmetrics.Render(&buf, []selfmetrics.Metric{{Name: "m", Instrument: "gauge", Description: "d"}})).To(Succeed())
		Expect(buf.String()).To(HaveSuffix("| `m` | gauge |  |  | d |\n"))
	})

	It("renders the committed markdown", func() {
		var buf bytes.Buffer
		Expect(selfmetrics.Render(&buf, selfmetrics.Catalogue())).To(Succeed())

		if os.Getenv("UPDATE_GOLDEN") != "" {
			Expect(os.WriteFile(goldenPath, buf.Bytes(), 0o600)).To(Succeed())
		}

		want, err := os.ReadFile(goldenPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal(string(want)))
	})
})
