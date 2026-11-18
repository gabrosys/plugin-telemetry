package selfmetrics_test

import (
	"bytes"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

const (
	goldenDir  = "../../docs"
	goldenName = "self-metrics.md"
)

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
			{"cnpg_collector_collections_total", "counter", "", nil},
			{"cnpg_collector_collection_errors_total", "counter", "", []string{"collector"}},
			{"cnpg_collector_last_collection_error", "gauge", "", nil},
			{"cnpg_collector_collection_duration_seconds", "gauge", "", []string{"collector"}},
		}

		got := make([]row, 0, len(want))
		seen := map[string]bool{}
		for _, m := range selfmetrics.ByComponent(selfmetrics.ComponentEngine) {
			got = append(got, row{m.Name, string(m.Instrument), m.Unit, m.Attributes})
			Expect(seen).NotTo(HaveKey(m.Name))
			seen[m.Name] = true
			Expect(m.Description).NotTo(BeEmpty(), m.Name)
		}
		Expect(got).To(Equal(want))
	})

	It("has the plugin rows", func() {
		type row struct {
			name, instrument, unit string
			attributes             []string
		}
		var got []row
		for _, m := range selfmetrics.ByComponent(selfmetrics.ComponentPlugin) {
			got = append(got, row{m.Name, string(m.Instrument), m.Unit, m.Attributes})
			Expect(m.Description).NotTo(BeEmpty(), m.Name)
		}
		Expect(got).To(Equal([]row{
			{"cnpg_otel_plugin_hook_calls_total", "counter", "{call}", []string{"service", "operation", "outcome"}},
			{"cnpg_otel_plugin_hook_duration_seconds", "histogram", "s", []string{"service", "operation"}},
			{"cnpg_otel_plugin_identity_reconciles_total", "counter", "{reconcile}", []string{"outcome", "reason"}},
		}))
	})

	It("gives every metric a known instrument", func() {
		for _, m := range selfmetrics.Catalogue() {
			Expect(m.Instrument).To(BeElementOf(
				selfmetrics.InstrumentCounter, selfmetrics.InstrumentGauge, selfmetrics.InstrumentHistogram), m.Name)
		}
	})

	It("gives every metric a known component", func() {
		for _, m := range selfmetrics.Catalogue() {
			Expect(m.Component).To(BeElementOf(selfmetrics.ComponentEngine, selfmetrics.ComponentPlugin), m.Name)
		}
	})

	It("generates a plugin section in docs/self-metrics.md", func() {
		text := string(fixture.Read(GinkgoTB(), goldenDir, goldenName))
		Expect(text).To(ContainSubstring("## Engine"))
		Expect(text).To(ContainSubstring("## Plugin"))
		Expect(text).To(ContainSubstring("| `cnpg_otel_plugin_hook_calls_total` | counter | `{call}` | `service`, `operation`, `outcome` |"))
		Expect(strings.Index(text, "## Plugin")).To(BeNumerically("<", strings.Index(text, "cnpg_otel_plugin_hook_calls_total")))
		Expect(strings.Index(text, "## Engine")).To(BeNumerically("<", strings.Index(text, "cnpg_otel_query_runs_total")))
	})

	It("gives the self-measuring rows CloudNativePG's descriptions", func() {
		help := map[string]string{}
		for _, m := range selfmetrics.ByComponent(selfmetrics.ComponentEngine) {
			help[m.Name] = m.Description
		}
		// pg_collector.go:112-129,136-141 at v1.30.1.
		Expect(help).To(HaveKeyWithValue("cnpg_collector_collections_total", "Total number of times PostgreSQL was accessed for metrics."))
		Expect(help).To(HaveKeyWithValue("cnpg_collector_collection_errors_total", "Total errors occurred accessing PostgreSQL for metrics."))
		Expect(help).To(HaveKeyWithValue("cnpg_collector_last_collection_error", "1 if the last collection ended with error, 0 otherwise."))
		Expect(help).To(HaveKeyWithValue("cnpg_collector_collection_duration_seconds", "Collection time duration in seconds"))
	})

	It("leaves the unit cell empty for a metric without a unit", func() {
		var buf bytes.Buffer
		Expect(selfmetrics.Render(&buf, []selfmetrics.Metric{{Name: "m", Component: selfmetrics.ComponentEngine, Instrument: selfmetrics.InstrumentGauge, Description: "d"}})).To(Succeed())
		Expect(buf.String()).To(HaveSuffix("| `m` | gauge |  |  | d |\n"))
	})

	It("documents denied and cnpg_otel_check in docs/self-metrics.md", func() {
		text := string(fixture.Read(GinkgoTB(), goldenDir, goldenName))
		Expect(text).To(ContainSubstring("Query runs by outcome (ok, error, timeout, skipped, capped, denied)."))
		Expect(text).To(ContainSubstring("Configuration loads by source and outcome (ok, error, denied)."))
		Expect(text).To(ContainSubstring("| `cnpg_otel_check` | gauge |  | `check` |"))
		Expect(text).To(ContainSubstring("pg_monitor, read_only, track_io_timing, track_wal_io_timing, track_functions"))
		Expect(text).To(ContainSubstring("read_only checks the sidecar's transactions"))
		Expect(text).To(ContainSubstring("superuser is 1 while the session role is a superuser; the sidecar then runs no query"))
	})

	It("renders the committed markdown", func() {
		var buf bytes.Buffer
		Expect(selfmetrics.Render(&buf, selfmetrics.Catalogue())).To(Succeed())

		Expect(buf.String()).To(Equal(string(fixture.Golden(GinkgoTB(), goldenDir, goldenName, buf.Bytes()))))
	})
})

var _ = Describe("Render", func() {
	It("fails for a component without a section and writes nothing", func() {
		var out bytes.Buffer
		err := selfmetrics.Render(&out, append(selfmetrics.Catalogue(), selfmetrics.Metric{Name: "m", Component: "other"}))

		Expect(err).To(MatchError(And(ContainSubstring(`"m"`), ContainSubstring(`"other"`))))
		Expect(out.Len()).To(BeZero())
	})
})
