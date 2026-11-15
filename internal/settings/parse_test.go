package settings_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

var _ = Describe("Parse", func() {
	It("applies the defaults and resolves paths against dir", func() {
		s, err := settings.Parse([]byte("cluster: c\nexporter:\n  endpoint: http://c:4318\n  headersFile: h\n"), "/etc/x")
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Exporter.HeadersFile).To(Equal("/etc/x/h"))
		Expect(s.Exporter.Timeout).To(Equal(10 * time.Second))
		Expect(s.Postgres.User).To(Equal("cnpg_metrics_exporter"))
	})

	It("rejects unknown keys and invalid settings", func() {
		_, err := settings.Parse([]byte("cluster: c\nretries: 1\n"), "/")
		Expect(err).To(MatchError(ContainSubstring("retries")))
		_, err = settings.Parse([]byte("exporter:\n  endpoint: http://c:4318\n"), "/")
		Expect(err).To(MatchError(ContainSubstring("cluster")))
	})
})

var _ = Describe("Defaults", func() {
	It("is what Load starts from", func() {
		d := settings.Defaults()
		Expect(d.Exporter.Interval).To(Equal(30 * time.Second))
		Expect(d.Postgres.Host).To(Equal("/controller/run"))
		Expect(d.PlanInterval).To(Equal(30 * time.Second))
	})
})

var _ = DescribeTable("DefaultExporterTimeout",
	func(interval, want time.Duration) {
		Expect(settings.DefaultExporterTimeout(interval)).To(Equal(want))
	},
	Entry("30s", 30*time.Second, 10*time.Second),
	Entry("20s", 20*time.Second, 10*time.Second),
	Entry("10s", 10*time.Second, 5*time.Second),
	Entry("1s", time.Second, 500*time.Millisecond),
)
