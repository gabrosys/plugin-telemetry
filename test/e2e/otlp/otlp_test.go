package otlp_test

import (
	"strconv"
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
	// Two resources in one request: pod-a's counter at 2 s and pod-b's histogram at 4 s.
	twoPodsLine = `{"resourceMetrics":[` +
		`{"resource":{"attributes":[{"key":"k8s.pod.name","value":{"stringValue":"pod-a"}}]},` +
		`"scopeMetrics":[{"scope":{"name":"scope-a"},"metrics":[{"name":"calls_total","sum":{"isMonotonic":true,` +
		`"dataPoints":[{"timeUnixNano":"2000000000","asInt":"7"}]}}]}]},` +
		`{"resource":{"attributes":[{"key":"k8s.pod.name","value":{"stringValue":"pod-b"}}]},` +
		`"scopeMetrics":[{"scope":{"name":"scope-a"},"metrics":[{"name":"latency","histogram":{` +
		`"dataPoints":[{"timeUnixNano":"4000000000","count":"5","sum":2.5,"bucketCounts":["2","3"],"explicitBounds":[1],` +
		`"attributes":[{"key":"op","value":{"stringValue":"read"}}]}]}}]}]}]}` + "\n"
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

var _ = Describe("Decode", func() {
	It("decodes the complete lines and reports the bytes up to the last newline", func() {
		lines, consumed, err := otlp.Decode([]byte(counterLine + gaugeLine + counterLine[:10]))
		Expect(err).NotTo(HaveOccurred())
		Expect(lines).To(HaveLen(2))
		Expect(lines[0].Time).To(Equal(time.Unix(2, 0)))
		Expect(lines[1].Time).To(Equal(time.Unix(3, 0)))
		Expect(consumed).To(Equal(len(counterLine + gaugeLine)))
	})

	It("decodes nothing from a chunk without a newline", func() {
		lines, consumed, err := otlp.Decode([]byte(counterLine[:10]))
		Expect(err).NotTo(HaveOccurred())
		Expect(lines).To(BeEmpty())
		Expect(consumed).To(BeZero())
	})

	It("names the byte offset of a line that is not OTLP JSON", func() {
		_, _, err := otlp.Decode([]byte("not json\n"))
		Expect(err).To(MatchError(ContainSubstring("decode the line at byte 0")))
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
		Expect(otlp.Points(lines, "calls_total")).To(HaveExactElements(HaveField("Monotonic", true)))
		Expect(otlp.Points(lines, "up")).To(HaveExactElements(HaveField("Monotonic", false)))
		p, ok := otlp.Last(lines, "calls_total", map[string]string{"hook": "EVALUATE"})
		Expect(ok).To(BeTrue())
		Expect(p.Value).To(Equal(3.0))
		_, ok = otlp.Last(lines, "calls_total", map[string]string{"hook": "other"})
		Expect(ok).To(BeFalse())
		Expect(otlp.Since(lines, time.Unix(3, 0))).To(HaveLen(1))
	})

	It("returns one export per resource with its series, named by metric and attributes", func() {
		Expect(otlp.Exports(lines)).To(Equal([]otlp.Export{
			{
				Resource: map[string]string{"service.name": "plugin-telemetry"},
				Time:     time.Unix(2, 0),
				Series:   []string{`calls_total{hook="EVALUATE"}`},
				Scopes:   []otlp.Scope{{Name: "scope-a", Version: "1"}},
			},
			{
				Resource: map[string]string{}, Time: time.Unix(3, 0), Series: []string{"up{}"},
				Scopes: []otlp.Scope{{Name: "scope-b"}},
			},
		}))
	})

	It("keeps the resources with an attribute value, stamped with the latest point time of what is kept", func() {
		var l otlp.Lines
		Expect(l.Update([]byte(counterLine + gaugeLine + twoPodsLine))).To(Succeed())
		all := l.All()

		podA := otlp.ForResource(all, "k8s.pod.name", "pod-a")
		Expect(podA).To(HaveLen(1))
		Expect(podA[0].Time).To(Equal(time.Unix(2, 0)))
		Expect(otlp.Resources(podA)).To(Equal([]map[string]string{{"k8s.pod.name": "pod-a"}}))
		Expect(otlp.Points(podA, "calls_total")).To(HaveExactElements(HaveField("Value", 7.0)))
		Expect(otlp.HistogramPoints(podA, "latency")).To(BeEmpty())

		Expect(otlp.ForResource(all, "service.name", "plugin-telemetry")).To(HaveLen(1))
		Expect(otlp.ForResource(all, "k8s.pod.name", "absent")).To(BeEmpty())
		Expect(all[2].Request.GetResourceMetrics()).To(HaveLen(2), "the input lines are left as they were")
	})

	It("returns the histogram points of a metric", func() {
		var l otlp.Lines
		Expect(l.Update([]byte(twoPodsLine))).To(Succeed())

		Expect(otlp.HistogramPoints(l.All(), "latency")).To(Equal([]otlp.HistogramPoint{{
			Time: time.Unix(4, 0), Attrs: map[string]string{"op": "read"}, Count: 5, Sum: 2.5,
		}}))
		Expect(otlp.HistogramPoints(l.All(), "calls_total")).To(BeEmpty())
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

	It("fails on a metric with no data", func() {
		var l otlp.Lines
		Expect(l.Update([]byte(`{"resourceMetrics":[{"resource":{"attributes":[]},"scopeMetrics":[{"scope":{"name":"scope-a"},` +
			`"metrics":[{"name":"up"}]}]}]}` + "\n"))).To(Succeed())

		_, err := otlp.Shapes(l.All(), "scope-a")
		Expect(err).To(MatchError(ContainSubstring("metric up")))
	})
})

var _ = Describe("ExportAt", func() {
	// exportLines returns, for each time in seconds, a line with the config info point and a line with an up point.
	exportLines := func(seconds ...int) []otlp.Line {
		var raw string
		for _, s := range seconds {
			ns := strconv.Itoa(s) + "000000000"
			raw += `{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"` + otlp.ConfigInfo + `","gauge":{"dataPoints":[{"timeUnixNano":"` + ns + `","asInt":"1"}]}}]}]}]}` + "\n"
			raw += `{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"up","gauge":{"dataPoints":[{"timeUnixNano":"` + ns + `","asInt":"1"}]}}]}]}]}` + "\n"
		}
		lines, _, err := otlp.Decode([]byte(raw))
		Expect(err).NotTo(HaveOccurred())
		return lines
	}
	at := func(s int) time.Time { return time.Unix(int64(s), 0) }

	It("lists the export times sorted and without duplicates", func() {
		lines := exportLines(20, 10, 30)
		Expect(otlp.ExportTimes(append(lines, lines...))).To(Equal([]time.Time{at(10), at(20), at(30)}))
	})

	It("returns exactly the lines of one export", func() {
		lines := exportLines(10, 20, 30)
		Expect(otlp.ExportAt(lines, at(10))).To(Equal(lines[0:2]))
		Expect(otlp.ExportAt(lines, at(20))).To(Equal(lines[2:4]))
	})

	It("runs the last export to the end of the slice", func() {
		lines := exportLines(10, 20, 30)
		Expect(otlp.ExportAt(lines, at(30))).To(Equal(lines[4:]))
	})

	It("returns no lines for a time past every export", func() {
		Expect(otlp.ExportAt(exportLines(10, 20), at(21))).To(BeEmpty())
	})
})
