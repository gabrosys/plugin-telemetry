//go:build e2e

package plugin_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/e2e/plugin"
)

const (
	// parityWindow and parityExports give our series several query runs, so constants stand out from gauges.
	parityWindow  = 70 * time.Second
	parityExports = 7
	// parityTail covers parityWindow, and probeTail one export, for every resource that exports to the collector.
	parityTail = 120
	probeTail  = 20
	// alignQuery is the query of CNPG's default monitoring whose sample times AC7 aligns.
	alignQuery = "pg_stat_database"
	// Our sample may be this old at the scrape: 1 s off the interval for CNPG's whole seconds, 1 s for the scrape.
	parityFreshness = exportInterval - 2*time.Second
	// Our query runs drift against the export ticks by their 0-1 s jitter alone, so a bad phase takes up to 12 runs.
	parityAlignTimeout = 6 * time.Minute
	// cnpgQueriesTTL is CNPG's default metricsQueriesTTL: a scrape within it gets the cached query metrics.
	cnpgQueriesTTL = 30 * time.Second
	metricsPort    = "9187"
	metricsPath    = "/metrics"
	reportDir      = "bin/e2e"
)

var errNoRepoRoot = errors.New("no go.mod above the working directory")

// parityTarget is what the parity spec reads from its mode's container once BeforeAll has run.
type parityTarget struct {
	env     framework.Env
	c       client.Client
	cfg     *rest.Config
	ns      string
	cluster string
	mode    string
}

// itParity adds AC7 to the mode's Ordered container; target is called when the spec runs.
func itParity(target func() parityTarget) {
	It("AC7: the parity report is produced", func(ctx SpecContext) {
		t := target()

		var cl apiv1.Cluster
		Expect(t.c.Get(ctx, client.ObjectKey{Namespace: t.ns, Name: t.cluster}, &cl)).To(Succeed())
		primary := cl.Status.CurrentPrimary
		Expect(primary).NotTo(BeEmpty())

		cs, err := kubernetes.NewForConfig(t.cfg)
		Expect(err).NotTo(HaveOccurred())

		// CNPG is scraped once our newest export holds a fresh alignQuery sample, so that CNPG's recomputed
		// sample lands within one export interval of ours. A scrape that misses waits out CNPG's cache.
		start := time.Now()
		var (
			text                 []byte
			lines                []otlp.Line
			scraped              time.Time
			scrapes              int
			sampled, cnpgSampled time.Time
		)
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(time.Since(scraped)).To(BeNumerically(">", cnpgQueriesTTL), "CNPG still serves the cache of the scrape at %s", scraped)

			probe, err := framework.CollectorLinesSince(ctx, t.cfg, probeTail, start)
			g.Expect(err).NotTo(HaveOccurred())
			probe = otlp.ForResource(probe, "service.namespace", t.ns)
			times := exportTimes(otlp.ForResource(probe, "k8s.pod.name", primary))
			g.Expect(times).NotTo(BeEmpty(), "no export of %s since %s", primary, start)
			newest := times[len(times)-1]
			var ok bool
			sampled, ok = plugin.OurSampleTime(probe, primary, alignQuery)
			g.Expect(ok).To(BeTrue(), "the newest export of %s holds no %s sample", primary, alignQuery)
			g.Expect(time.Since(sampled)).To(BeNumerically("<=", parityFreshness), "our %s sample is from %s", alignQuery, sampled)

			text, err = cs.CoreV1().Pods(t.ns).ProxyGet("http", primary, metricsPort, metricsPath, nil).DoRaw(ctx)
			scraped = time.Now()
			scrapes++
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(scraped.Sub(newest)).To(BeNumerically("<=", exportInterval), "CNPG scraped at %s, our newest point at %s", scraped, newest)
			cnpgSampled, ok, err = plugin.CNPGSampleTime(text)
			g.Expect(err).NotTo(HaveOccurred())
			if ok {
				// 1 s under the interval, which Compare measures from the exports.
				g.Expect(cnpgSampled.Sub(sampled).Abs()).To(BeNumerically("<=", exportInterval-time.Second),
					"CNPG sampled at %s, our %s at %s", cnpgSampled, alignQuery, sampled)
			}

			all, err := framework.CollectorLinesSince(ctx, t.cfg, parityTail, start.Add(-parityWindow))
			g.Expect(err).NotTo(HaveOccurred())
			lines = upTo(otlp.ForResource(all, "service.namespace", t.ns), newest)
			g.Expect(len(exportTimes(otlp.ForResource(lines, "k8s.pod.name", primary)))).To(BeNumerically(">=", parityExports),
				"exports of %s since %s", primary, start.Add(-parityWindow))
		}).WithContext(ctx).WithTimeout(parityAlignTimeout).WithPolling(500 * time.Millisecond).Should(Succeed())
		AddReportEntry("parity alignment", fmt.Sprintf("%d scrapes in %s; our %s sample at %s, CNPG's at %s",
			scrapes, time.Since(start).Round(time.Second), alignQuery, sampled.Format(time.RFC3339Nano), cnpgSampled.Format(time.RFC3339)))

		p, err := plugin.Compare(text, lines, primary)
		Expect(err).NotTo(HaveOccurred())

		report := p.Markdown(fmt.Sprintf("Parity: CNPG %s, %s mode, primary %s", t.env.CNPGVersion, t.mode, primary))
		path, err := writeReport(fmt.Sprintf("parity-%s-%s.md", t.env.CNPGVersion, t.mode), report)
		Expect(err).NotTo(HaveOccurred())
		Expect(appendStepSummary(os.Getenv("GITHUB_STEP_SUMMARY"), report)).To(Succeed())
		AddReportEntry("parity report", path)
	}, SpecTimeout(parityAlignTimeout+2*time.Minute))
}

// upTo returns the lines whose Time is at or before t.
func upTo(lines []otlp.Line, t time.Time) []otlp.Line {
	var out []otlp.Line
	for _, l := range lines {
		if !l.Time.After(t) {
			out = append(out, l)
		}
	}

	return out
}

// exportTimes returns the distinct times of lines, sorted.
func exportTimes(lines []otlp.Line) []time.Time {
	out := make([]time.Time, 0, len(lines))
	for _, l := range lines {
		out = append(out, l.Time)
	}
	slices.SortFunc(out, time.Time.Compare)

	return slices.CompactFunc(out, time.Time.Equal)
}

// writeReport writes report to bin/e2e/name under the repository root and returns its path.
func writeReport(name, report string) (string, error) {
	dir, err := repoRoot()
	if err != nil {
		return "", err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", dir, err)
	}

	rel := filepath.Join(reportDir, name)
	err = root.MkdirAll(reportDir, 0o750)
	if err == nil {
		err = root.WriteFile(rel, []byte(report), 0o644)
	}
	if err := errors.Join(err, root.Close()); err != nil {
		return "", fmt.Errorf("write %s in %s: %w", rel, dir, err)
	}

	return filepath.Join(dir, rel), nil
}

// appendStepSummary appends report to the file at path, creating it; an empty path does nothing.
func appendStepSummary(path, report string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // the runner names this file
	if err != nil {
		return fmt.Errorf("open the step summary: %w", err)
	}
	if _, err := f.WriteString(report + "\n"); err != nil {
		return errors.Join(fmt.Errorf("append to the step summary: %w", err), f.Close())
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close the step summary: %w", err)
	}

	return nil
}

// repoRoot returns the closest directory above the working directory that holds go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get the working directory: %w", err)
	}
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("look for go.mod in %s: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errNoRepoRoot
		}
		dir = parent
	}
}
