package framework_test

import (
	"errors"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
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

var _ = Describe("RotatedFiles", func() {
	It("lists the backups the file exporter names, oldest first, and ignores other files", func() {
		listing := "metrics-2026-11-14T18-07-14.274-size.jsonl\n" +
			"metrics.jsonl\n" +
			"metrics-2026-11-14T17-59-01.000-size.jsonl\n" +
			"metrics-2026-11-14T18-07-14.274.jsonl\n" +
			"other-2026-11-14T17-00-00.000-size.jsonl\n" +
			"metrics-2026-11-13T23-59-59.999-size.jsonl\n"
		Expect(framework.RotatedFiles(listing)).To(Equal([]string{
			"/data/metrics-2026-11-13T23-59-59.999-size.jsonl",
			"/data/metrics-2026-11-14T17-59-01.000-size.jsonl",
			"/data/metrics-2026-11-14T18-07-14.274-size.jsonl",
		}))
	})

	It("lists nothing when the collector has not rotated", func() {
		Expect(framework.RotatedFiles("metrics.jsonl\n")).To(BeEmpty())
	})
})

var _ = Describe("ReadFiles", func() {
	line := func(name string, sec int) string {
		return `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"a"},"metrics":[{"name":"` + name +
			`","gauge":{"dataPoints":[{"timeUnixNano":"` + strconv.Itoa(sec) + `000000000","asInt":"1"}]}}]}]}]}` + "\n"
	}
	names := func(lines []otlp.Line) []string {
		var out []string
		for _, l := range lines {
			out = append(out, l.Request.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()[0].GetName())
		}

		return out
	}
	files := []string{"old", "new", "current"}
	content := map[string]string{
		"old":     line("o1", 1) + line("o2", 2),
		"new":     line("n1", 3) + line("n2", 4),
		"current": line("c1", 5),
	}
	reader := func(requested map[string]int) func(string, int) (string, error) {
		return func(file string, n int) (string, error) {
			requested[file] = n

			return content[file], nil
		}
	}

	It("returns the lines of the backups and the current file in order", func() {
		got, err := framework.ReadFiles(files, reader(map[string]int{}), 100, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(names(got)).To(Equal([]string{"o1", "o2", "n1", "n2", "c1"}))
	})

	It("bounds the lines across the files, asking older files only for what is left", func() {
		requested := map[string]int{}
		got, err := framework.ReadFiles(files, reader(requested), 3, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(requested).To(Equal(map[string]int{"current": 3, "new": 2}))
		Expect(names(got)).To(Equal([]string{"n1", "n2", "c1"}))
	})

	It("keeps the lines at or after since", func() {
		got, err := framework.ReadFiles(files, reader(map[string]int{}), 100, time.Unix(4, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(names(got)).To(Equal([]string{"n2", "c1"}))
	})

	It("skips a last line without a newline in each file", func() {
		partial := map[string]string{"old": line("o1", 1) + line("o2", 2)[:30], "current": line("c1", 5)}
		got, err := framework.ReadFiles([]string{"old", "current"}, func(file string, _ int) (string, error) {
			return partial[file], nil
		}, 100, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(names(got)).To(Equal([]string{"o1", "c1"}))
	})

	It("fails when a file cannot be read", func() {
		_, err := framework.ReadFiles(files, func(string, int) (string, error) { return "", errors.New("gone") }, 5, time.Time{})
		Expect(err).To(MatchError(ContainSubstring("gone")))
	})
})
