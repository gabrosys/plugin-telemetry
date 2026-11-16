package queries_test

import (
	"os"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

var opts = queries.Options{MaxLabelsPerSeries: 32}

var _ = Describe("Define", func() {
	DescribeTable("loads CNPG's default queries as the golden file says",
		func(ref string) {
			defs := defineAll(configMapQueries(readTestdata("default-monitoring-" + ref + ".yaml")))
			Expect(defs).To(HaveLen(20))
			Expect(defs).To(HaveKey("collector"))

			Expect(defs["pg_stat_bgwriter_17"].Namespace).To(Equal("cnpg_pg_stat_bgwriter"))
			Expect(defs["pg_stat_replication"].PrimaryOnly).To(BeTrue())
			Expect(defs["pg_extensions"].TargetDatabases).To(Equal([]string{"*"}))
			for key, def := range defs {
				Expect(def.Warnings).To(BeEmpty(), key)
				Expect(def.Disabled).To(BeEmpty(), key)
			}

			got := marshalSummary(defs)
			golden := "defaults-" + ref + ".golden.json"
			if os.Getenv("UPDATE_GOLDEN") != "" {
				Expect(testdata().WriteFile(golden, got, 0o600)).To(Succeed())
			}

			want := readTestdata(golden)
			Expect(string(got)).To(Equal(string(want)))
		},
		Entry("v1.30.1", "v1.30.1"),
		Entry("main", "main"),
	)

	DescribeTable("names metrics cnpg_<key or name>_<column or name>",
		func(doc, column, want string) {
			def := defineOne("stats", doc, opts)
			Expect(def.Columns).To(HaveKey(column))
			Expect(def.Columns[column].Metric).To(Equal(want))
		},
		Entry("counter", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: COUNTER}\n", "c", "cnpg_stats_c"),
		Entry("gauge", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: GAUGE}\n", "c", "cnpg_stats_c"),
		Entry("mapped metric", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: MAPPEDMETRIC}\n", "c", "cnpg_stats_c"),
		Entry("duration", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: DURATION}\n", "c", "cnpg_stats_c_milliseconds"),
		Entry("histogram", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: HISTOGRAM}\n", "c", "cnpg_stats_c"),
		Entry("query name", "stats:\n  query: SELECT 1\n  name: other\n  metrics:\n    - c: {usage: GAUGE}\n", "c", "cnpg_other_c"),
		Entry("column name", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: GAUGE, name: d}\n", "c", "cnpg_stats_d"),
		Entry("duration with column name", "stats:\n  query: SELECT 1\n  metrics:\n    - c: {usage: DURATION, name: d}\n", "c", "cnpg_stats_d_milliseconds"),
	)

	It("sets the namespace from the name, else the key", func() {
		Expect(defineOne("stats", "stats:\n  query: SELECT 1\n", opts).Namespace).To(Equal("cnpg_stats"))
		Expect(defineOne("stats", "stats:\n  query: SELECT 1\n  name: other\n", opts).Namespace).To(Equal("cnpg_other"))
	})

	It("gives each usage its kind and role", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - l: {usage: LABEL}
    - d: {usage: DISCARD}
    - c: {usage: COUNTER}
    - g: {usage: GAUGE}
    - m: {usage: MAPPEDMETRIC}
    - t: {usage: DURATION}
`, opts)
		Expect(def.Columns).To(HaveLen(6))
		Expect(def.Columns["l"].Role).To(Equal(queries.RoleLabel))
		Expect(def.Columns["l"].Metric).To(BeEmpty())
		Expect(def.Columns["d"].Role).To(Equal(queries.RoleDiscard))
		Expect(def.Columns["d"].Metric).To(BeEmpty())
		Expect(def.Columns["c"].Role).To(Equal(queries.RoleValue))
		Expect(def.Columns["c"].Kind).To(Equal(queries.KindCounter))
		for _, name := range []string{"g", "m", "t"} {
			Expect(def.Columns[name].Role).To(Equal(queries.RoleValue), name)
			Expect(def.Columns[name].Kind).To(Equal(queries.KindGauge), name)
		}
	})

	It("keys labels by column name even when the column has a name", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - datname: {usage: LABEL, name: db}
    - v: {usage: GAUGE}
    - rel: {usage: LABEL}
`, opts)
		Expect(def.Columns).To(HaveKey("datname"))
		Expect(def.Columns).ToNot(HaveKey("db"))
		Expect(def.Columns["datname"].Role).To(Equal(queries.RoleLabel))
		Expect(def.Labels).To(Equal([]string{"datname", "rel"}))
	})

	It("expects four columns for a HISTOGRAM column x: x, x_bucket, x_sum, x_count", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - x: {usage: HISTOGRAM, description: "Wait time", name: wait}
`, opts)
		Expect(def.Columns).To(HaveLen(4))
		Expect(def.Columns["x"].Role).To(Equal(queries.RoleValue))
		Expect(def.Columns["x"].Kind).To(Equal(queries.KindHistogram))
		Expect(def.Columns["x"].Metric).To(Equal("cnpg_stats_wait"))
		Expect(def.Columns["x"].Description).To(Equal("Wait time"))
		for _, name := range []string{"x_bucket", "x_sum", "x_count"} {
			Expect(def.Columns).To(HaveKey(name))
			Expect(def.Columns[name].Role).To(Equal(queries.RoleHistogramPart), name)
			Expect(def.Columns[name].Metric).To(BeEmpty(), name)
		}
	})

	It("lets a later column with the same name overwrite an earlier one", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - v: {usage: LABEL}
    - v: {usage: GAUGE}
`, opts)
		Expect(def.Columns).To(HaveLen(1))
		Expect(def.Columns["v"].Role).To(Equal(queries.RoleValue))
		Expect(def.Labels).To(BeEmpty())
	})

	It("takes metric_mapping over metrics_mapping when both are set, with a warning", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - s:
        usage: MAPPEDMETRIC
        metric_mapping: {up: 1}
        metrics_mapping: {up: 5, down: 0}
`, opts)
		Expect(def.Columns["s"].Mapping).To(Equal(map[string]float64{"up": 1}))
		Expect(def.Warnings).To(ConsistOf(ContainSubstring("metrics_mapping")))
		Expect(def.Disabled).To(BeEmpty())
	})

	It("uses an empty metric_mapping over metrics_mapping, with a warning", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - s: {usage: MAPPEDMETRIC, metric_mapping: {}, metrics_mapping: {up: 1}}
`, opts)
		Expect(def.Columns["s"].Mapping).To(BeEmpty())
		Expect(def.Warnings).To(ConsistOf(ContainSubstring("metrics_mapping")))
	})

	It("accepts metrics_mapping alone", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - s: {usage: MAPPEDMETRIC, metrics_mapping: {up: 1, down: 0}}
`, opts)
		Expect(def.Columns["s"].Mapping).To(Equal(map[string]float64{"up": 1, "down": 0}))
		Expect(def.Warnings).To(BeEmpty())
	})

	DescribeTable("ignores a metrics_mapping that is not a map of numbers, with a warning, and still loads the file",
		func(value string) {
			defs := defineAll(`
bad:
  query: SELECT 1
  metrics:
    - s: {usage: MAPPEDMETRIC, metrics_mapping: ` + value + `}
good:
  query: SELECT 1
  metrics:
    - g: {usage: GAUGE}
`)
			Expect(defs).To(HaveKey("good"))
			Expect(defs["good"].Warnings).To(BeEmpty())
			Expect(defs["bad"].Disabled).To(BeEmpty())
			Expect(defs["bad"].Columns["s"].Mapping).To(BeEmpty())
			Expect(defs["bad"].Warnings).To(ConsistOf(ContainSubstring(`"s"`)))
		},
		Entry("a non-numeric value", "{up: x}"),
		Entry("a scalar", "up"),
		Entry("a list", "[1, 2]"),
		Entry("a boolean value", "{up: true}"),
	)

	It("reads integer and float values of metrics_mapping", func() {
		def := defineOne("stats", "stats:\n  query: SELECT 1\n  metrics:\n    - s: {usage: MAPPEDMETRIC, metrics_mapping: {a: 1, b: 2.5}}\n", opts)
		Expect(def.Columns["s"].Mapping).To(Equal(map[string]float64{"a": 1, "b": 2.5}))
		Expect(def.Warnings).To(BeEmpty())
	})

	It("leaves a column with an unknown usage out of Columns, with a warning", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - v: {usage: GAUGE}
    - w: {usage: gauge}
    - z: {description: "no usage"}
`, opts)
		Expect(def.Columns).To(HaveLen(1))
		Expect(def.Columns).To(HaveKey("v"))
		Expect(def.Warnings).To(ConsistOf(ContainSubstring(`"w"`), ContainSubstring(`"z"`)))
		Expect(def.Disabled).To(BeEmpty())
	})

	It("disables a query with an invalid runonserver", func() {
		def := defineOne("stats", "stats:\n  query: SELECT 1\n  runonserver: 'not a range'\n", opts)
		Expect(def.Disabled).To(ContainSubstring("runonserver"))
		Expect(def.RunOnServer).To(BeNil())
	})

	It("keeps a valid runonserver as a constraint", func() {
		def := defineOne("stats", "stats:\n  query: SELECT 1\n  runonserver: '>=17.0.0'\n", opts)
		Expect(def.Disabled).To(BeEmpty())
		Expect(def.RunOnServer).ToNot(BeNil())
	})

	It("ignores an invalid pg_version with a warning", func() {
		def := defineOne("stats", `
stats:
  query: SELECT 1
  metrics:
    - v: {usage: GAUGE, pg_version: "not a range"}
    - w: {usage: GAUGE, pg_version: ">=15 <17"}
`, opts)
		Expect(def.Disabled).To(BeEmpty())
		Expect(def.Columns["v"].Versions).To(BeNil())
		Expect(def.Columns["w"].Versions).ToNot(BeNil())
		Expect(def.Warnings).To(ConsistOf(ContainSubstring("pg_version")))
	})

	It("disables a query whose metric names start with cnpg_otel_", func() {
		byKey := defineOne("otel_x", "otel_x:\n  query: SELECT 1\n  metrics:\n    - v: {usage: GAUGE}\n", opts)
		Expect(byKey.Disabled).To(ContainSubstring("cnpg_otel_"))

		byName := defineOne("x", "x:\n  query: SELECT 1\n  name: otel_y\n  metrics:\n    - v: {usage: GAUGE}\n", opts)
		Expect(byName.Disabled).To(ContainSubstring("cnpg_otel_"))

		similar := defineOne("otelx", "otelx:\n  query: SELECT 1\n  metrics:\n    - v: {usage: GAUGE}\n", opts)
		Expect(similar.Disabled).To(BeEmpty())
	})

	It("disables a query with more labels than MaxLabelsPerSeries", func() {
		doc := "q:\n  query: SELECT 1\n  metrics:\n    - a: {usage: LABEL}\n    - b: {usage: LABEL}\n    - c: {usage: LABEL}\n"

		Expect(defineOne("q", doc, queries.Options{MaxLabelsPerSeries: 2}).Disabled).To(ContainSubstring("labels"))
		Expect(defineOne("q", doc, queries.Options{MaxLabelsPerSeries: 3}).Disabled).To(BeEmpty())
		Expect(defineOne("q", doc, queries.Options{}).Disabled).To(BeEmpty())
	})

	It("disables a query with empty SQL", func() {
		Expect(defineOne("q", "q:\n  metrics:\n    - v: {usage: GAUGE}\n", opts).Disabled).To(ContainSubstring("query"))
		Expect(defineOne("q", "q:\n  query: \"  \\n\"\n", opts).Disabled).To(ContainSubstring("query"))
	})

	It("treats an empty predicate_query as no predicate", func() {
		def := defineOne("q", "q:\n  query: SELECT 1\n", opts)
		Expect(def.Predicate).To(BeEmpty())
		Expect(def.Disabled).To(BeEmpty())
	})

	It("copies the scheduling fields", func() {
		def := defineOne("q", `
q:
  query: SELECT 1
  predicate_query: SELECT true
  master: true
  cache_seconds: 30
  target_databases: [app, "t*"]
`, opts)
		Expect(def.Key).To(Equal("q"))
		Expect(def.SQL).To(Equal("SELECT 1"))
		Expect(def.Predicate).To(Equal("SELECT true"))
		Expect(def.PrimaryOnly).To(BeTrue())
		Expect(def.CacheSeconds).To(BeEquivalentTo(30))
		Expect(def.TargetDatabases).To(Equal([]string{"app", "t*"}))
	})

	It("defines the built-in collector query", func() {
		builtin := queries.Builtin()
		Expect(builtin).To(HaveLen(6))
		Expect(builtin).To(HaveKey("collector"))

		def := queries.Define("collector", builtin["collector"], opts)
		Expect(def.Namespace).To(Equal("cnpg_collector"))
		Expect(def.TargetDatabases).To(Equal([]string{"*"}))
		Expect(def.Labels).To(Equal([]string{"datname"}))
		Expect(def.Columns["lo_pages"].Metric).To(Equal("cnpg_collector_lo_pages"))
		Expect(def.Columns["lo_pages"].Kind).To(Equal(queries.KindGauge))
	})

	DescribeTable("quotes nothing from a sensitive source",
		func(doc string, quoted string, named ...string) {
			def := defineOne("q", doc, queries.Options{MaxLabelsPerSeries: 32, Sensitive: true})
			text := strings.Join(append(def.Warnings, def.Disabled), "\n")
			Expect(text).ToNot(BeEmpty())
			Expect(text).ToNot(ContainSubstring(quoted))
			for _, n := range named {
				Expect(text).To(ContainSubstring(n))
			}
		},
		Entry("an invalid pg_version", "q:\n  query: SELECT 1\n  metrics:\n    - v: {usage: GAUGE, pg_version: 'abc!'}\n", "abc!", `"q"`, `"v"`),
		Entry("an unknown usage", "q:\n  query: SELECT 1\n  metrics:\n    - v: {usage: GAUGES}\n", "GAUGES", `"q"`, `"v"`),
		Entry("an invalid runonserver", "q:\n  query: SELECT 1\n  runonserver: 'nope!'\n", "nope!", `"q"`),
		Entry("a cluster-wide relation", "q:\n  query: SELECT 1 AS v FROM pg_stat_database\n  target_databases: ['*']\n  metrics:\n    - v: {usage: GAUGE}\n", "pg_stat_database", `"q"`),
	)

	DescribeTable("warns when a cluster-wide relation runs in more than one database",
		func(sql, targets string, warns bool) {
			doc := "q:\n  query: \"" + sql + "\"\n  target_databases: " + targets + "\n  metrics:\n    - v: {usage: GAUGE}\n"
			def := defineOne("q", doc, opts)
			if warns {
				Expect(def.Warnings).To(HaveLen(1))
				Expect(def.Warnings[0]).To(ContainSubstring("cluster-wide"))
			} else {
				Expect(def.Warnings).To(BeEmpty())
			}
			Expect(def.Disabled).To(BeEmpty())
		},
		Entry("pg_stat_activity on a glob", "SELECT count(*) AS v FROM pg_stat_activity", "['*']", true),
		Entry("pg_stat_activity on two databases", "SELECT count(*) AS v FROM pg_stat_activity", "[a, b]", true),
		Entry("schema-qualified, upper case", "SELECT count(*) AS v FROM PG_CATALOG.PG_STAT_ACTIVITY", "[a, b]", true),
		Entry("a progress view", "SELECT 1 AS v FROM pg_stat_progress_vacuum", "[a, b]", true),
		Entry("a shared catalog", "SELECT 1 AS v FROM pg_catalog.pg_database", "['d*']", true),
		Entry("a list of relations", "SELECT 1 AS v FROM pg_locks,pg_settings", "[a, b]", true),
		Entry("pg_stat_user_tables is per database", "SELECT 1 AS v FROM pg_stat_user_tables", "['*']", false),
		Entry("one target", "SELECT count(*) AS v FROM pg_stat_activity", "[a]", false),
		Entry("no target", "SELECT count(*) AS v FROM pg_stat_activity", "[]", false),
		Entry("another schema", "SELECT 1 AS v FROM app.pg_stat_activity", "[a, b]", false),
		Entry("a longer identifier", "SELECT 1 AS v FROM pg_stat_activity_copy", "[a, b]", false),
		Entry("a column of that name", "SELECT 1 AS v FROM t WHERE x_pg_locks = 1", "[a, b]", false),
		Entry("a line comment", "SELECT 1 AS v -- pg_stat_activity", "[a, b]", false),
		Entry("a block comment", "SELECT /* pg_stat_activity */ 1 AS v", "[a, b]", false),
		Entry("a string literal", "SELECT 1 AS v FROM pg_class WHERE relname = 'pg_stat_activity'", "[a, b]", false),
		Entry("after a quoted name with a quote", `SELECT 1 AS \"a'b\" FROM pg_stat_activity`, "[a, b]", true),
		Entry("after a quoted name with dashes", `SELECT 1 AS \"--c\" FROM pg_stat_activity`, "[a, b]", true),
		Entry("a literal after a quoted name with a quote", `SELECT 1 AS \"x'\", 'pg_stat_activity'`, "[a, b]", false),
	)

	DescribeTable("ReadsSettings",
		func(query, predicate string, want bool) {
			def := queries.Define("q", queries.UserQuery{Query: query, PredicateQuery: predicate}, opts)
			Expect(def.ReadsSettings).To(Equal(want))
		},
		Entry("pg_catalog.pg_settings", "SELECT name, setting FROM pg_catalog.pg_settings", "", true),
		Entry("upper-case pg_settings", "select * from PG_SETTINGS", "", true),
		Entry("pg_settings in the predicate", "SELECT 1", "SELECT count(*) > 0 FROM pg_settings", true),
		Entry("pg_show_all_settings()", "SELECT * FROM pg_catalog.pg_show_all_settings()", "", true),
		Entry("no relation", "SELECT 1", "", false),
		Entry("a longer identifier", "SELECT my_pg_settings FROM t", "", false),
		Entry("another schema", "SELECT * FROM other.pg_settings", "", false),
		Entry("current_setting", "SELECT current_setting('jit')", "", false),
		Entry("a line comment", "SELECT 1 -- pg_settings", "", false),
		Entry("a block comment", "SELECT /* pg_settings */ 1", "", false),
		Entry("a nested block comment", "SELECT /* a /* b */ pg_settings */ 1", "", false),
		Entry("a string literal", "SELECT 1 FROM pg_class WHERE relname = 'pg_settings'", "", false),
		Entry("a comment in the predicate", "SELECT 1", "SELECT true -- pg_settings", false),
		Entry("after a literal with a quote", "SELECT 'it''s', name FROM pg_settings", "", true),
		Entry("an escape string", `SELECT E'\' pg_settings '`, "", false),
		Entry("after an escape string", `SELECT E'a\'b', name FROM pg_settings`, "", true),
		Entry("a dollar-quoted string", "SELECT $$ pg_settings $$", "", false),
		Entry("a tagged dollar-quoted string", "SELECT $q$ $$ pg_settings $q$", "", false),
		Entry("after a dollar-quoted string", "SELECT $q$x$q$, name FROM pg_settings", "", true),
		Entry("after a positional parameter", "SELECT $1, name FROM pg_settings", "", true),
		Entry("after a quoted name with a quote", `SELECT "a'b" FROM pg_settings`, "", true),
		Entry("after a quoted name with dashes", `SELECT "--c", name FROM pg_settings`, "", true),
		Entry("after a quoted name with a doubled quote", `SELECT "a""--", name FROM pg_settings`, "", true),
		Entry("a literal after a quoted name with a quote", `SELECT "x'" , 'pg_settings'`, "", false),
		Entry("a quoted pg_settings", `SELECT name FROM "pg_settings"`, "", true),
		Entry("after a comment", "SELECT name /* x */ FROM pg_settings -- y", "", true),
	)

	Describe("fingerprint", func() {
		const base = `
q:
  query: SELECT 1 AS state, 2 AS v
  predicate_query: SELECT true
  primary: false
  metrics:
    - state:
        usage: MAPPEDMETRIC
        description: "d"
        metric_mapping: {up: 1, down: 0}
        pg_version: ">=15"
    - v:
        usage: GAUGE
        description: "v"
`

		fingerprint := func(doc string) string {
			GinkgoHelper()

			return defineOne("q", doc, opts).Fingerprint
		}

		It("is a hex sha256", func() {
			Expect(fingerprint(base)).To(MatchRegexp(`^[0-9a-f]{64}$`))
		})

		DescribeTable("changes when something that shapes samples changes",
			func(from, to string) {
				Expect(base).To(ContainSubstring(from))
				Expect(fingerprint(strings.Replace(base, from, to, 1))).ToNot(Equal(fingerprint(base)))
			},
			Entry("SQL", "SELECT 1 AS state", "SELECT 3 AS state"),
			Entry("predicate", "SELECT true", "SELECT false"),
			Entry("mapping", "up: 1", "up: 2"),
			Entry("query name", "  primary: false\n", "  primary: false\n  name: other\n"),
			Entry("description", `description: "d"`, `description: "e"`),
			Entry("pg_version", `">=15"`, `">=16"`),
			Entry("primary", "primary: false", "primary: true"),
			Entry("usage", "usage: GAUGE", "usage: COUNTER"),
			Entry("column name", "    - v:\n        usage: GAUGE\n", "    - v:\n        name: w\n        usage: GAUGE\n"),
		)

		DescribeTable("stays when nothing that shapes samples changes",
			func(from, to string) {
				Expect(base).To(ContainSubstring(from))
				Expect(fingerprint(strings.Replace(base, from, to, 1))).To(Equal(fingerprint(base)))
			},
			Entry("a comment", "q:\n", "q:  # a comment\n"),
			Entry("an unknown key", "q:\n", "q:\n  future: 1\n"),
			Entry("mapping key order", "{up: 1, down: 0}", "{down: 0, up: 1}"),
			Entry("the mapping key spelling", "metric_mapping:", "metrics_mapping:"),
			Entry("cache_seconds", "  primary: false\n", "  primary: false\n  cache_seconds: 60\n"),
			Entry("target_databases", "  primary: false\n", "  primary: false\n  target_databases: [a, b]\n"),
			Entry("runonserver", "  primary: false\n", "  primary: false\n  runonserver: '>=17.0.0'\n"),
			Entry("an invalid runonserver that disables the query", "  primary: false\n", "  primary: false\n  runonserver: nope\n"),
			Entry("a column with an unknown usage", "  metrics:\n", "  metrics:\n    - z: {usage: nope}\n"),
		)

		It("is the same for the same query under two keys", func() {
			named := strings.Replace(base, "  primary: false\n", "  primary: false\n  name: shared\n", 1)
			other := strings.Replace(named, "q:\n", "r:\n", 1)

			a := defineOne("q", named, opts)
			b := defineOne("r", other, opts)
			Expect(a.Key).ToNot(Equal(b.Key))
			Expect(b.Fingerprint).To(Equal(a.Fingerprint))
		})

		It("is the same on every call", func() {
			Expect(fingerprint(base)).To(Equal(fingerprint(base)))
		})

		It("ignores warnings", func() {
			a := defineOne("q", base, opts)
			b := defineOne("q", strings.Replace(base, "  metrics:\n", "  metrics:\n    - z: {usage: nope}\n", 1), opts)
			Expect(a.Warnings).To(BeEmpty())
			Expect(b.Warnings).ToNot(BeEmpty())
			Expect(b.Fingerprint).To(Equal(a.Fingerprint))
		})
	})
})
