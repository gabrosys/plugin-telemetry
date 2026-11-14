package otlp_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	counterLine = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"plugin-telemetry"}}]},` +
		`"scopeMetrics":[{"scope":{"name":"scope-a","version":"1"},"metrics":[{"name":"calls_total","sum":{"isMonotonic":true,` +
		`"dataPoints":[{"timeUnixNano":"2000000000","asInt":"3","attributes":[{"key":"hook","value":{"stringValue":"EVALUATE"}}]}]}}]}]}]}` + "\n"
	gaugeLine = `{"resourceMetrics":[{"resource":{"attributes":[]},"scopeMetrics":[{"scope":{"name":"scope-b"},` +
		`"metrics":[{"name":"up","gauge":{"dataPoints":[{"timeUnixNano":"3000000000","asDouble":1.5}]}}]}]}]}` + "\n"
)

var _ = Describe("Lines", func() {
	It("decodes complete lines and leaves a partial last line for the next update", func() {
		var l otlp.Lines
		partial := gaugeLine[:len(gaugeLine)/2]
		Expect(l.Update([]byte(counterLine + partial))).To(Succeed())
		Expect(l.All()).To(HaveLen(1))

		Expect(l.Update([]byte(counterLine + gaugeLine))).To(Succeed())
		Expect(l.All()).To(HaveLen(2))
	})

	It("fails when the file shrank", func() {
		var l otlp.Lines
		Expect(l.Update([]byte(counterLine))).To(Succeed())
		Expect(l.Update([]byte(counterLine[:10]))).To(MatchError(otlp.ErrShrank))
	})

	It("fails on a line that is not OTLP/JSON", func() {
		var l otlp.Lines
		Expect(l.Update([]byte("not json\n"))).To(MatchError(ContainSubstring("decode the line at byte 0")))
	})
})

var _ = Describe("readers", func() {
	var lines []otlp.Line

	BeforeEach(func() {
		var l otlp.Lines
		Expect(l.Update([]byte(counterLine + gaugeLine))).To(Succeed())
		lines = l.All()
	})

	It("stamps a line with its latest point time", func() {
		Expect(lines[0].Time).To(Equal(time.Unix(2, 0)))
	})

	It("returns the points, the last matching point and the lines since a time", func() {
		Expect(otlp.Points(lines, "calls_total")).To(HaveLen(1))
		p, ok := otlp.Last(lines, "calls_total", map[string]string{"hook": "EVALUATE"})
		Expect(ok).To(BeTrue())
		Expect(p.Value).To(Equal(3.0))
		_, ok = otlp.Last(lines, "calls_total", map[string]string{"hook": "other"})
		Expect(ok).To(BeFalse())
		Expect(otlp.Since(lines, time.Unix(3, 0))).To(HaveLen(1))
	})

	It("returns the points of a metric with the resource they came in", func() {
		Expect(otlp.ResourcePoints(lines, "calls_total")).To(Equal([]otlp.ResourcePoint{{
			Resource: map[string]string{"service.name": "plugin-telemetry"},
			Point: otlp.Point{
				Time: time.Unix(2, 0), Attrs: map[string]string{"hook": "EVALUATE"}, Value: 3,
			},
		}}))
		Expect(otlp.ResourcePoints(lines, "missing")).To(BeEmpty())
	})

	It("returns one export per resource with its series, named by metric and attributes", func() {
		Expect(otlp.Exports(lines)).To(Equal([]otlp.Export{
			{
				Resource: map[string]string{"service.name": "plugin-telemetry"},
				Time:     time.Unix(2, 0),
				Series:   []string{`calls_total{hook="EVALUATE"}`},
			},
			{Resource: map[string]string{}, Time: time.Unix(3, 0), Series: []string{"up{}"}},
		}))
	})

	It("returns resources, scopes and shapes", func() {
		Expect(otlp.Resources(lines)).To(Equal([]map[string]string{{"service.name": "plugin-telemetry"}, {}}))
		Expect(otlp.Scopes(lines)).To(Equal([]otlp.Scope{{Name: "scope-a", Version: "1"}, {Name: "scope-b"}}))
		shapes, err := otlp.Shapes(lines, "scope-a")
		Expect(err).NotTo(HaveOccurred())
		monotonic := true
		Expect(shapes).To(Equal([]otlp.Shape{{
			Name: "calls_total", Type: otlp.TypeSum, Monotonic: &monotonic, Attributes: []string{"hook"},
		}}))
	})
})
