package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/cli"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// execute runs the root command with args and returns its standard output.
func execute(ctx context.Context, args ...string) (string, error) {
	var out bytes.Buffer
	cmd := cli.NewRootCommand() //nolint:contextcheck // the context reaches the command through ExecuteContext
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(ctx)
	return out.String(), err
}

func testdata(name string) string {
	GinkgoHelper()
	abs, err := filepath.Abs(filepath.Join("testdata", name))
	Expect(err).NotTo(HaveOccurred())
	return abs
}

var _ = Describe("instance --feature-gates", func() {
	It("fails on an unknown gate before doing anything else", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := execute(ctx, "instance", "--feature-gates", "+unknown")
		Expect(err).To(MatchError(ContainSubstring("unknown")))
		Expect(ctx.Err()).NotTo(HaveOccurred())
	})

	It("is accepted by every subcommand", func() {
		for _, sub := range []string{"validate", "test", "print-config"} {
			_, err := execute(context.Background(), "instance", sub, "--config", testdata("engine-valid.yaml"), "--feature-gates", "+unknown")
			Expect(err).To(MatchError(ContainSubstring("unknown feature gate")), sub)
		}
	})
})

var _ = Describe("instance --config", func() {
	It("applies the memory limit and runs the engine until its context ends", func() {
		calls := 0
		DeferCleanup(cli.SetMemlimit(func(func(string) ([]byte, error), func(string) string) (int64, error) {
			calls++
			return 1 << 20, nil
		}))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := execute(ctx, "instance", "--config", testdata("engine-valid.yaml"))
			done <- err
		}()
		time.Sleep(200 * time.Millisecond)
		cancel()
		Eventually(done, 20*time.Second).Should(Receive(BeNil()))
		Expect(calls).To(Equal(1))
	})
})

var _ = Describe("instance --config with an unreadable memory limit", func() {
	It("logs the error at info and runs the engine", func() {
		DeferCleanup(cli.SetMemlimit(func(func(string) ([]byte, error), func(string) string) (int64, error) {
			return 0, fmt.Errorf("read /sys/fs/cgroup/memory.max: %w", fs.ErrPermission)
		}))
		logFile := filepath.Join(GinkgoT().TempDir(), "log")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := execute(ctx, "--log-destination", logFile, "instance", "--config", testdata("engine-valid.yaml"))
			done <- err
		}()
		logged := func(g Gomega) string {
			raw, err := os.ReadFile(logFile) //nolint:gosec // a file the spec created
			g.Expect(err).NotTo(HaveOccurred())
			return string(raw)
		}
		Eventually(logged).WithTimeout(10 * time.Second).Should(ContainSubstring("starting the metrics engine"))
		cancel()
		Eventually(done, 20*time.Second).Should(Receive(BeNil()))

		var line string
		for l := range strings.Lines(logged(Default)) {
			if strings.Contains(l, fs.ErrPermission.Error()) {
				line = l
			}
		}
		Expect(line).To(And(ContainSubstring(`"level":"info"`), ContainSubstring("memory limit")))
	})
})

var _ = Describe("instance validate", func() {
	It("requires --config", func() {
		_, err := execute(context.Background(), "instance", "validate")
		Expect(err).To(MatchError(ContainSubstring("config")))
	})

	It("prints warnings and succeeds", func() {
		out, err := execute(context.Background(), "instance", "validate", "--config", testdata("engine-valid.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("warning " + testdata("queries-warn.yaml") + " probe: column \"other\": unknown usage \"BOGUS\""))
		Expect(out).To(ContainSubstring(`warning - -: group "unused": pattern "no_such_*" matches no query`))
		Expect(out).NotTo(ContainSubstring("error "))
	})

	It("fails on a source that does not parse and names it", func() {
		broken := testdata("queries-broken.yaml")
		out, err := execute(context.Background(), "instance", "validate", "--config", testdata("engine-broken.yaml"))
		Expect(err).To(HaveOccurred())
		Expect(out).To(ContainSubstring("error " + broken + " -: "))
	})

	It("fails on a source that does not exist", func() {
		out, err := execute(context.Background(), "instance", "validate", "--config", testdata("engine-missing.yaml"))
		Expect(err).To(HaveOccurred())
		Expect(out).To(ContainSubstring("error " + testdata("queries-absent.yaml") + " -: "))
	})

	It("fails on invalid settings", func() {
		out, err := execute(context.Background(), "instance", "validate", "--config", testdata("engine-invalid.yaml"))
		Expect(err).To(HaveOccurred())
		Expect(out).To(ContainSubstring("error - -: "))
	})
})

var _ = Describe("instance print-config", func() {
	It("never prints a header value", func() {
		out, err := execute(context.Background(), "instance", "print-config", "--offline", "--config", testdata("engine-headers.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("authorization: <redacted>"))
		Expect(out).To(ContainSubstring("x-tenant: <redacted>"))
		Expect(out).NotTo(ContainSubstring("s3cr3t-token"))
		Expect(out).NotTo(ContainSubstring("acme-secret"))
	})

	It("prints the settings, the sources and the unplanned queries offline", func() {
		out, err := execute(context.Background(), "instance", "print-config", "--offline", "--config", testdata("engine-valid.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("cluster: demo"))
		Expect(out).To(ContainSubstring("ok builtin"))
		Expect(out).To(ContainSubstring("unplanned collector group= interval=30s"))
		Expect(out).To(ContainSubstring("unplanned probe group= interval=30s"))
	})

	It("lists the whole-query skips offline", func() {
		out, err := execute(context.Background(), "instance", "print-config", "--offline", "--config", testdata("engine-excluded.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("skipped collector: excluded query"))
		Expect(out).To(ContainSubstring("skipped probe: all metrics excluded"))
	})

	It("fails when it cannot connect and --offline is not set", func() {
		_, err := execute(context.Background(), "instance", "print-config", "--config", testdata("engine-unreachable.yaml"))
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("instance test", func() {
	It("fails when it cannot connect", func() {
		_, err := execute(context.Background(), "instance", "test", "--config", testdata("engine-unreachable.yaml"))
		Expect(err).To(HaveOccurred())
	})

	It("fails on invalid settings", func() {
		_, err := execute(context.Background(), "instance", "test", "--config", testdata("engine-invalid.yaml"))
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("formatSamples", func() {
	It("prints sorted series and expands histograms", func() {
		lines := cli.FormatSamples([]queries.Sample{
			{Metric: "b_total", Kind: queries.KindCounter, Value: 3},
			{
				Metric: "a_hist", Kind: queries.KindHistogram, Labels: []queries.Label{{Key: "k", Value: "v"}},
				Histogram: &queries.Histogram{Bounds: []float64{1, 5}, Cumulative: []uint64{2, 3}, Count: 4, Sum: 7.5},
			},
			{Metric: "a_gauge", Kind: queries.KindGauge, Labels: []queries.Label{{Key: "k", Value: `x"y`}}, Value: 1.5},
		})
		Expect(lines).To(Equal([]string{
			`a_gauge{k="x\"y"} 1.5`,
			`a_hist_bucket{k="v",le="1"} 2`,
			`a_hist_bucket{k="v",le="5"} 3`,
			`a_hist_bucket{k="v",le="+Inf"} 4`,
			`a_hist_sum{k="v"} 7.5`,
			`a_hist_count{k="v"} 4`,
			`b_total 3`,
		}))
	})
})

var _ = DescribeTable("the paths that connect clear PG* and OTEL_* before connecting",
	func(subcommand []string, config string, runsEngine bool) {
		const (
			pgValue   = "secret-pg-host"
			otelValue = "secret-otel-token"
		)
		GinkgoT().Setenv("PGHOST", pgValue)
		GinkgoT().Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization="+otelValue)
		logFile := filepath.Join(GinkgoT().TempDir(), "log.json")
		DeferCleanup(cli.SetMemlimit(func(func(string) ([]byte, error), func(string) string) (int64, error) { return 0, nil }))

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		args := append(slices.Clone(subcommand), "--config", testdata(config), "--log-destination", logFile)
		_, err := execute(ctx, args...)
		if runsEngine {
			Expect(err).NotTo(HaveOccurred())
		} else {
			Expect(err).To(HaveOccurred())
		}

		_, pgSet := os.LookupEnv("PGHOST")
		_, otelSet := os.LookupEnv("OTEL_EXPORTER_OTLP_HEADERS")
		Expect(pgSet).To(BeFalse())
		Expect(otelSet).To(BeFalse())

		logged, err := os.ReadFile(logFile) //nolint:gosec // a file the spec created
		Expect(err).NotTo(HaveOccurred())
		Expect(string(logged)).To(ContainSubstring("removed environment variables"))
		Expect(string(logged)).To(ContainSubstring("PGHOST"))
		Expect(string(logged)).To(ContainSubstring("OTEL_EXPORTER_OTLP_HEADERS"))
		Expect(string(logged)).NotTo(ContainSubstring(pgValue))
		Expect(string(logged)).NotTo(ContainSubstring(otelValue))
	},
	Entry("instance --config", []string{"instance"}, "engine-valid.yaml", true),
	Entry("instance test", []string{"instance", "test"}, "engine-unreachable.yaml", false),
	Entry("instance print-config", []string{"instance", "print-config"}, "engine-unreachable.yaml", false),
)
