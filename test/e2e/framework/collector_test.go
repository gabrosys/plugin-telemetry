package framework_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	tailLine1 = `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"a"},"metrics":[{"name":"m1","gauge":` +
		`{"dataPoints":[{"timeUnixNano":"1000000000","asInt":"1"}]}}]}]}]}`
	tailLine2 = `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"a"},"metrics":[{"name":"m2","gauge":` +
		`{"dataPoints":[{"timeUnixNano":"2000000000","asInt":"2"}]}}]}]}]}`
)

var _ = Describe("ParseLines", func() {
	It("parses tail output into lines, skipping a partial last line", func() {
		lines, err := framework.ParseLines(tailLine1 + "\n" + tailLine2 + "\n" + tailLine1[:20])
		Expect(err).NotTo(HaveOccurred())
		Expect(lines).To(HaveLen(2))
		Expect(lines[1].Time.Unix()).To(Equal(int64(2)))
	})

	It("returns no lines for empty output", func() {
		lines, err := framework.ParseLines("")
		Expect(err).NotTo(HaveOccurred())
		Expect(lines).To(BeEmpty())
	})

	It("fails on a complete line that is not OTLP/JSON", func() {
		_, err := framework.ParseLines("garbage\n")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ParseLinesSince", func() {
	It("excludes the lines before since", func() {
		lines, err := framework.ParseLinesSince(tailLine1+"\n"+tailLine2+"\n", time.Unix(2, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(lines).To(HaveLen(1))
		Expect(lines[0].Time.Unix()).To(Equal(int64(2)))
	})

	It("returns no lines when all are older", func() {
		lines, err := framework.ParseLinesSince(tailLine1+"\n", time.Unix(5, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(lines).To(BeEmpty())
	})
})
