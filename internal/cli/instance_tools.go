package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v4"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/featuregate"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

const (
	connectTimeout = 5 * time.Second
	poolIdle       = 30 * time.Second
	redacted       = "<redacted>"
	absent         = "-"
)

// config is a loaded configuration: the settings, the merged sources and their definitions.
type config struct {
	settings settings.Settings
	result   sources.Result
	defs     []queries.Definition // in the order of result.Entries
}

func loadConfig(path string) (*config, error) {
	s, err := settings.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load settings: %w", err)
	}
	res := sources.NewLoader(s.Sources).Load()
	opts := queries.Options{MaxLabelsPerSeries: s.Limits.MaxLabelsPerSeries}
	defs := make([]queries.Definition, 0, len(res.Entries))
	for _, e := range res.Entries {
		defs = append(defs, queries.Define(e.Key, e.Query, opts))
	}

	return &config{settings: s, result: res, defs: defs}, nil
}

func (c *config) keys() []string {
	keys := make([]string, 0, len(c.defs))
	for i := range c.defs {
		keys = append(keys, c.defs[i].Key)
	}

	return keys
}

func newValidateCommand(flags *instanceFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Check the settings and the query sources without connecting",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := flags.registry(); err != nil {
				return err
			}
			if err := flags.requireConfig(); err != nil {
				return err
			}

			return validate(cmd.OutOrStdout(), flags.config)
		},
	}
}

// finding writes "<level> <source> <query>: <message>", one line per line of message.
func finding(w io.Writer, level, source, query, message string) error {
	for _, line := range strings.Split(strings.TrimSpace(message), "\n") {
		if _, err := fmt.Fprintf(w, "%s %s %s: %s\n", level, source, query, line); err != nil {
			return fmt.Errorf("write finding: %w", err)
		}
	}

	return nil
}

func validate(w io.Writer, path string) error {
	errCount := 0
	report := func(level, source, query, message string) error {
		if level == "error" {
			errCount++
		}

		return finding(w, level, source, query, message)
	}

	c, err := loadConfig(path)
	if err != nil {
		if rerr := report("error", absent, absent, err.Error()); rerr != nil {
			return rerr
		}

		return fmt.Errorf("the configuration has %d error(s)", errCount)
	}

	for _, st := range c.result.Status {
		if st.Outcome == sources.OutcomeOK {
			continue
		}
		if err := report("error", st.Source, absent, fmt.Sprint(st.Err)); err != nil {
			return err
		}
	}
	for i := range c.defs {
		d, source := &c.defs[i], c.result.Entries[i].Source
		for _, warning := range d.Warnings {
			if err := report("warning", source, d.Key, warning); err != nil {
				return err
			}
		}
		if d.Disabled != "" {
			if err := report("warning", source, d.Key, "disabled: "+d.Disabled); err != nil {
				return err
			}
		}
	}
	for _, warning := range c.settings.Warnings(c.keys()) {
		if err := report("warning", absent, absent, warning); err != nil {
			return err
		}
	}
	if errCount > 0 {
		return fmt.Errorf("the configuration has %d error(s)", errCount)
	}

	return nil
}

// connect opens a pool and gathers the facts from the default database. The caller closes the pool.
func connect(ctx context.Context, s settings.Settings) (*pg.Pool, planner.Facts, error) {
	pool := pg.NewPool(engine.PostgresConfig(s), s.Postgres.MaxConnections, poolIdle, clock.RealClock{},
		pg.WithLogger(log.FromContext(ctx).GetLogger()))
	ctx, cancel := context.WithTimeout(ctx, connectTimeout+2*s.Limits.QueryTimeout)
	defer cancel()
	conn, release, err := pool.Acquire(ctx, s.Postgres.Database)
	if err != nil {
		pool.Close()
		return nil, planner.Facts{}, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	facts, err := planner.GatherFacts(ctx, conn)
	release(err != nil)
	if err != nil {
		pool.Close()
		return nil, planner.Facts{}, fmt.Errorf("gather facts: %w", err)
	}

	return pool, facts, nil
}

func newTestCommand(flags *instanceFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "test",
		Short: "Run every planned query once and print the series",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := flags.registry(); err != nil {
				return err
			}
			if err := flags.requireConfig(); err != nil {
				return err
			}

			return runTest(cmd.Context(), cmd.OutOrStdout(), flags.config)
		},
	}
}

func runTest(ctx context.Context, w io.Writer, path string) error {
	c, err := loadConfig(path)
	if err != nil {
		return err
	}
	clearEnv(ctx)
	pool, facts, err := connect(ctx, c.settings)
	if err != nil {
		return err
	}
	defer pool.Close()

	plan := planner.Build(c.defs, c.settings, facts)
	server := facts.Version
	exec := executor.New(pool, c.settings.Limits, func() *semver.Version { return &server }, clock.RealClock{})

	var (
		samples []queries.Sample
		failed  []string
	)
	for _, st := range c.result.Status {
		if st.Outcome != sources.OutcomeOK {
			failed = append(failed, fmt.Sprintf("%s: %v", st.Source, st.Err))
		}
	}
	for _, job := range plan.Jobs {
		res := exec.Run(ctx, job)
		if res.Outcome != selfmetrics.OutcomeOK {
			msg := string(res.Outcome)
			if res.Err != nil {
				msg = fmt.Sprintf("%s: %v", res.Outcome, res.Err)
			}
			failed = append(failed, fmt.Sprintf("%s %s: %s", job.Query, job.Database, msg))
			continue
		}
		samples = append(samples, res.Samples...)
	}

	var b strings.Builder
	for _, l := range formatSamples(samples) {
		b.WriteString(l + "\n")
	}
	b.WriteString("# skipped\n")
	for _, s := range plan.Skips {
		b.WriteString(skipLine(s) + "\n")
	}
	b.WriteString("# errors\n")
	for _, f := range failed {
		b.WriteString(strings.ReplaceAll(f, "\n", " ") + "\n")
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("write result: %w", err)
	}

	return nil
}

func skipLine(s planner.Skip) string {
	if s.Database == "" {
		return fmt.Sprintf("%s: %s", s.Query, s.Reason)
	}

	return fmt.Sprintf("%s %s: %s", s.Query, s.Database, s.Reason)
}

// formatSamples renders samples as `name{k="v"} value` lines sorted by name and labels; a histogram
// becomes its _bucket, _sum and _count lines.
func formatSamples(samples []queries.Sample) []string {
	type rendered struct {
		metric, labels string
		lines          []string
	}
	items := make([]rendered, 0, len(samples))
	for i := range samples {
		s := &samples[i]
		labels := labelPairs(s.Labels)
		r := rendered{metric: s.Metric, labels: strings.Join(labels, ",")}
		if s.Histogram == nil {
			r.lines = []string{series(s.Metric, labels, s.Value)}
		} else {
			h := s.Histogram
			for j, bound := range h.Bounds {
				le := `le="` + strconv.FormatFloat(bound, 'g', -1, 64) + `"`
				r.lines = append(r.lines, series(s.Metric+"_bucket", append(slices.Clone(labels), le), float64(h.Cumulative[j])))
			}
			r.lines = append(r.lines,
				series(s.Metric+"_bucket", append(slices.Clone(labels), `le="+Inf"`), float64(h.Count)),
				series(s.Metric+"_sum", labels, h.Sum),
				series(s.Metric+"_count", labels, float64(h.Count)))
		}
		items = append(items, r)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].metric != items[j].metric {
			return items[i].metric < items[j].metric
		}
		return items[i].labels < items[j].labels
	})

	var out []string
	for _, it := range items {
		out = append(out, it.lines...)
	}

	return out
}

func labelPairs(labels []queries.Label) []string {
	pairs := make([]string, 0, len(labels))
	for _, l := range labels {
		pairs = append(pairs, l.Key+"="+strconv.Quote(l.Value))
	}

	return pairs
}

func series(name string, labels []string, value float64) string {
	v := strconv.FormatFloat(value, 'g', -1, 64)
	if len(labels) == 0 {
		return name + " " + v
	}

	return name + "{" + strings.Join(labels, ",") + "} " + v
}

func newPrintConfigCommand(flags *instanceFlags) *cobra.Command {
	var offline bool
	cmd := &cobra.Command{
		Use:   "print-config",
		Short: "Print the effective settings, the sources, the feature gates and the plan",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			gates, err := flags.registry()
			if err != nil {
				return err
			}
			if err := flags.requireConfig(); err != nil {
				return err
			}

			return printConfig(cmd.Context(), cmd.OutOrStdout(), flags.config, gates, offline)
		},
	}
	cmd.Flags().BoolVar(&offline, "offline", false, "do not connect: print the queries that need no facts to be skipped")

	return cmd
}

func printConfig(ctx context.Context, w io.Writer, path string, gates *featuregate.Registry, offline bool) error {
	c, err := loadConfig(path)
	if err != nil {
		return err
	}
	out, err := describe(c, gates)
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString(out)

	var connectErr error
	if offline {
		b.WriteString("# plan (offline: not planned from live facts)\n")
		b.WriteString(offlinePlan(c))
	} else {
		clearEnv(ctx)
		var pool *pg.Pool
		var facts planner.Facts
		if pool, facts, connectErr = connect(ctx, c.settings); connectErr == nil {
			pool.Close()
			b.WriteString("# plan\n")
			b.WriteString(livePlan(planner.Build(c.defs, c.settings, facts)))
		}
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("write configuration: %w", err)
	}

	return connectErr
}

// describe renders the settings, the headers, the sources and the feature gates.
func describe(c *config, gates *featuregate.Registry) (string, error) {
	var b strings.Builder
	raw, err := yaml.Marshal(c.settings)
	if err != nil {
		return "", fmt.Errorf("render settings: %w", err)
	}
	b.WriteString("# settings\n")
	b.Write(raw)

	if f := c.settings.Exporter.HeadersFile; f != "" {
		b.WriteString("# headers\n")
		keys, err := headerKeys(f)
		if err != nil {
			fmt.Fprintf(&b, "unreadable: %v\n", err)
		}
		for _, k := range keys {
			fmt.Fprintf(&b, "%s: %s\n", k, redacted)
		}
	}

	b.WriteString("# sources\n")
	for _, st := range c.result.Status {
		if st.Outcome == sources.OutcomeOK {
			fmt.Fprintf(&b, "ok %s\n", st.Source)
			continue
		}
		fmt.Fprintf(&b, "%s %s: %v\n", st.Outcome, st.Source, strings.ReplaceAll(fmt.Sprint(st.Err), "\n", " "))
	}

	b.WriteString("# feature gates\n")
	for _, g := range gates.List() {
		fmt.Fprintf(&b, "%s %s enabled=%t\n", g.ID, g.Stage, g.Enabled)
	}

	return b.String(), nil
}

// headerKeys returns the sorted keys of the OTEL_EXPORTER_OTLP_HEADERS-format file; values are dropped.
func headerKeys(file string) ([]string, error) {
	data, err := os.ReadFile(file) //nolint:gosec // the path comes from the operator's settings
	if err != nil {
		return nil, fmt.Errorf("read headers file: %w", err)
	}
	var keys []string
	for _, pair := range strings.Split(strings.TrimSpace(string(data)), ",") {
		if k, _, ok := strings.Cut(pair, "="); ok {
			keys = append(keys, strings.TrimSpace(k))
		}
	}
	slices.Sort(keys)

	return slices.Compact(keys), nil
}

func livePlan(p planner.Plan) string {
	var b strings.Builder
	for _, j := range p.Jobs {
		fmt.Fprintf(&b, "job %s %s group=%s interval=%s\n", j.Query, j.Database, j.Group, j.Interval)
	}
	for _, s := range p.Skips {
		fmt.Fprintf(&b, "skipped %s\n", skipLine(s))
	}

	return b.String()
}

// offlinePlan lists the whole-query skips that need no facts and the interval and group of the other queries.
func offlinePlan(c *config) string {
	s := c.settings
	var b strings.Builder
	for i := range c.defs {
		d := &c.defs[i]
		reason := ""
		switch {
		case s.ExcludedQuery(d.Key):
			reason = planner.ReasonExcludedQuery
		case d.Disabled != "":
			reason = planner.ReasonDisabled + d.Disabled
		case planner.AllMetricsExcluded(d, s):
			reason = planner.ReasonAllMetrics
		}
		if reason != "" {
			fmt.Fprintf(&b, "skipped %s: %s\n", d.Key, reason)
			continue
		}
		group, interval := planner.EffectiveInterval(d, s)
		fmt.Fprintf(&b, "unplanned %s group=%s interval=%s\n", d.Key, group, interval)
	}

	return b.String()
}
