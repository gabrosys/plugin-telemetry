//go:build integration

package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/test/pgtest"
)

var _ = Describe("instance test and print-config against PostgreSQL", Ordered, func() {
	var config string

	BeforeAll(func(ctx SpecContext) {
		image := os.Getenv("POSTGRES_IMAGE")
		Expect(image).NotTo(BeEmpty(), "POSTGRES_IMAGE must name the PostgreSQL image (make test-integration sets it)")
		pg.ClearEnv()
		export.ClearEnv()

		server, err := pgtest.Start(ctx, image)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) { Expect(server.Terminate(ctx)).To(Succeed()) })

		dir := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(dir, "extra.yaml"), []byte(`extra_probe:
  query: SELECT 42 AS value
  metrics:
    - value:
        usage: GAUGE
        description: A probe.
`), 0o600)).To(Succeed())
		config = filepath.Join(dir, "engine.yaml")
		Expect(os.WriteFile(config, fmt.Appendf(nil, `cluster: it
postgres:
  host: %s
  port: %d
  user: %s
  passwordFile: %s
sources:
  - extra.yaml
exporter:
  endpoint: http://127.0.0.1:4318/v1/metrics
`, server.Host, server.Port, pgtest.MonitorUser, server.MonitorConfig(dir).PasswordFile), 0o600)).To(Succeed())
	}, NodeTimeout(5*time.Minute))

	It("runs every planned job once and prints the series", func(ctx SpecContext) {
		out, err := execute(ctx, "instance", "test", "--config", config)
		Expect(err).NotTo(HaveOccurred())

		var series []string
		for _, l := range strings.Split(out, "\n") {
			if l == "" || strings.HasPrefix(l, "#") {
				break
			}
			series = append(series, l)
		}
		Expect(series).To(ContainElement("cnpg_extra_probe_value 42"))
		Expect(series).To(ContainElement(HavePrefix(`cnpg_collector_lo_pages{datname="postgres"} `)))
		Expect(out).To(ContainSubstring("# skipped"))
		Expect(out).To(ContainSubstring("# errors"))
	}, SpecTimeout(time.Minute))

	It("prints the plan from live facts", func(ctx SpecContext) {
		out, err := execute(ctx, "instance", "print-config", "--config", config)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("job extra_probe postgres group= interval=30s"))
		Expect(out).NotTo(ContainSubstring("unplanned"))
	}, SpecTimeout(time.Minute))
})
