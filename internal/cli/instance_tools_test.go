package cli_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
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
