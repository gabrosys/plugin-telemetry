package planner_test

import (
	"time"

	"github.com/Masterminds/semver/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

func def(key string, mutate ...func(*queries.Definition)) queries.Definition {
	d := queries.Definition{
		Key:         key,
		Namespace:   "cnpg_" + key,
		Fingerprint: "fp-" + key,
		Columns: map[string]queries.Column{
			"datname": {Name: "datname", Role: queries.RoleLabel},
			"value":   {Name: "value", Role: queries.RoleValue, Metric: "cnpg_" + key + "_value", Kind: queries.KindGauge},
		},
	}
	for _, m := range mutate {
		m(&d)
	}
	return d
}

func constraint(s string) *semver.Constraints {
	GinkgoHelper()
	c, err := semver.NewConstraint(s)
	Expect(err).NotTo(HaveOccurred())
	return c
}

func baseSettings() settings.Settings {
	return settings.Settings{
		Postgres:        settings.Postgres{Database: "postgres"},
		DefaultInterval: 30 * time.Second,
		Limits:          settings.Limits{MaxDatabases: 100},
	}
}

func facts(dbs ...string) planner.Facts {
	return planner.Facts{Version: *semver.MustParse("18.0.7"), Databases: dbs}
}

func jobDBs(p planner.Plan) []string {
	var out []string
	for _, j := range p.Jobs {
		out = append(out, j.Query+"@"+j.Database)
	}
	return out
}

var _ = Describe("ParseVersionNum", func() {
	DescribeTable("parses server_version_num as CNPG does",
		func(in, want string) {
			v, err := planner.ParseVersionNum(in)
			Expect(err).NotTo(HaveOccurred())
			Expect(v.String()).To(Equal(want))
		},
		Entry("18.7", "180007", "18.0.7"),
		Entry("17.4", "170004", "17.0.4"),
		Entry("9.6.24", "90624", "9.6.24"),
	)

	It("rejects a non-number", func() {
		_, err := planner.ParseVersionNum("abc")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("Build", func() {
	It("runs primary and master queries only when not in recovery", func() {
		defs := []queries.Definition{def("q", func(d *queries.Definition) { d.PrimaryOnly = true })}

		f := facts()
		p := planner.Build(defs, baseSettings(), f)
		Expect(jobDBs(p)).To(Equal([]string{"q@postgres"}))

		f.InRecovery = true
		p = planner.Build(defs, baseSettings(), f)
		Expect(p.Jobs).To(BeEmpty())
		Expect(p.Skips).To(Equal([]planner.Skip{{Query: "q", Reason: planner.ReasonPrimaryOnly}}))
		Expect(planner.ReasonPrimaryOnly).To(Equal("primary only"))
	})

	It("checks runonserver against the server version", func() {
		defs := []queries.Definition{
			def("new", func(d *queries.Definition) { d.RunOnServer = constraint(">=17.0.0") }),
			def("old", func(d *queries.Definition) { d.RunOnServer = constraint("<17.0.0") }),
			def("any"),
		}
		p := planner.Build(defs, baseSettings(), facts())
		Expect(jobDBs(p)).To(Equal([]string{"any@postgres", "new@postgres"}))
		Expect(p.Skips).To(Equal([]planner.Skip{{Query: "old", Reason: "runonserver"}}))
	})

	It("targets the default database when target_databases is empty", func() {
		p := planner.Build([]queries.Definition{def("q")}, baseSettings(), facts("app"))
		Expect(jobDBs(p)).To(Equal([]string{"q@postgres"}))
	})

	DescribeTable("expands globs over the facts and keeps literal names",
		func(targets []string, want []string) {
			defs := []queries.Definition{def("q", func(d *queries.Definition) { d.TargetDatabases = targets })}
			p := planner.Build(defs, baseSettings(), facts("app", "apple", "bpp", "postgres"))
			Expect(jobDBs(p)).To(Equal(want))
		},
		Entry("star", []string{"*"}, []string{"q@app", "q@apple", "q@bpp", "q@postgres"}),
		Entry("prefix", []string{"app*"}, []string{"q@app", "q@apple"}),
		Entry("class", []string{"[ab]pp"}, []string{"q@app", "q@bpp"}),
		Entry("literal absent from facts", []string{"ghost"}, []string{"q@ghost"}),
		Entry("literal and glob without duplicates", []string{"app", "app*"}, []string{"q@app", "q@apple"}),
	)

	It("skips a query whose glob matched nothing", func() {
		defs := []queries.Definition{def("q", func(d *queries.Definition) { d.TargetDatabases = []string{"zz*"} })}
		p := planner.Build(defs, baseSettings(), facts("app"))
		Expect(p.Jobs).To(BeEmpty())
		Expect(p.Skips).To(Equal([]planner.Skip{{Query: "q", Reason: "no matching database"}}))
	})

	It("drops excluded databases and caps a query at MaxDatabases", func() {
		s := baseSettings()
		s.Exclude.Databases = []string{"bpp"}
		s.Limits.MaxDatabases = 2
		defs := []queries.Definition{def("q", func(d *queries.Definition) { d.TargetDatabases = []string{"*"} })}
		p := planner.Build(defs, s, facts("app", "apple", "bpp", "postgres"))
		Expect(jobDBs(p)).To(Equal([]string{"q@app", "q@apple"}))
		Expect(p.Skips).To(Equal([]planner.Skip{
			{Query: "q", Database: "bpp", Reason: "excluded database"},
			{Query: "q", Database: "postgres", Reason: "maxDatabases"},
		}))
	})

	It("skips excluded queries and disabled definitions", func() {
		s := baseSettings()
		s.Exclude.Queries = []string{"x*"}
		defs := []queries.Definition{
			def("xa"),
			def("b", func(d *queries.Definition) { d.Disabled = "query is empty" }),
		}
		p := planner.Build(defs, s, facts())
		Expect(p.Jobs).To(BeEmpty())
		Expect(p.Skips).To(Equal([]planner.Skip{
			{Query: "b", Reason: "disabled: query is empty"},
			{Query: "xa", Reason: "excluded query"},
		}))
	})

	Describe("metric exclusion", func() {
		twoValues := func(d *queries.Definition) {
			d.Columns["other"] = queries.Column{Name: "other", Role: queries.RoleValue, Metric: "cnpg_q_other", Kind: queries.KindGauge}
		}

		It("discards the excluded value columns in a copy of the definition", func() {
			s := baseSettings()
			s.Exclude.Metrics = []string{"cnpg_q_other"}
			defs := []queries.Definition{def("q", twoValues)}
			p := planner.Build(defs, s, facts())
			Expect(p.Jobs).To(HaveLen(1))
			got := p.Jobs[0].Def
			Expect(got.Columns).To(HaveLen(3))
			Expect(got.Columns["other"].Role).To(Equal(queries.RoleDiscard))
			Expect(got.Columns["value"].Role).To(Equal(queries.RoleValue))
			Expect(defs[0].Columns["other"].Role).To(Equal(queries.RoleValue), "the input is left alone")
		})

		It("changes the fingerprint only of the query with the exclusion", func() {
			defs := []queries.Definition{def("q", twoValues), def("r")}
			before := planner.Build(defs, baseSettings(), facts())

			s := baseSettings()
			s.Exclude.Metrics = []string{"cnpg_q_other"}
			after := planner.Build(defs, s, facts())

			Expect(after.Jobs[0].Fingerprint).NotTo(Equal(before.Jobs[0].Fingerprint))
			Expect(after.Jobs[1].Fingerprint).To(Equal(before.Jobs[1].Fingerprint))
			Expect(before.Jobs[0].Fingerprint).To(MatchRegexp(`^[0-9a-f]{64}$`))
		})

		It("skips a query whose every metric is excluded", func() {
			s := baseSettings()
			s.Exclude.Metrics = []string{"cnpg_q_*"}
			p := planner.Build([]queries.Definition{def("q", twoValues)}, s, facts())
			Expect(p.Jobs).To(BeEmpty())
			Expect(p.Skips).To(Equal([]planner.Skip{{Query: "q", Reason: "all metrics excluded"}}))
		})
	})

	It("sets the interval to the larger of the group interval and cache_seconds", func() {
		s := baseSettings()
		s.Groups = []settings.Group{{Name: "slow", Interval: time.Minute, Queries: []string{"s*"}}}
		defs := []queries.Definition{
			def("cached", func(d *queries.Definition) { d.CacheSeconds = 120 }),
			def("quick", func(d *queries.Definition) { d.CacheSeconds = 10 }),
			def("slowcached", func(d *queries.Definition) { d.CacheSeconds = 300 }),
			def("slowplain"),
		}
		p := planner.Build(defs, s, facts())
		got := map[string]time.Duration{}
		for _, j := range p.Jobs {
			got[j.Query] = j.Interval
		}
		Expect(got).To(Equal(map[string]time.Duration{
			"cached":     120 * time.Second,
			"quick":      30 * time.Second,
			"slowcached": 300 * time.Second,
			"slowplain":  time.Minute,
		}))
	})

	It("puts a query in the first matching group, else the default interval", func() {
		s := baseSettings()
		s.Groups = []settings.Group{
			{Name: "a", Interval: time.Minute, Queries: []string{"pg_*"}},
			{Name: "b", Interval: time.Hour, Queries: []string{"pg_stat"}},
		}
		defs := []queries.Definition{def("pg_stat"), def("other")}
		p := planner.Build(defs, s, facts())
		Expect(p.Jobs).To(HaveLen(2))
		Expect(p.Jobs[0].Query).To(Equal("other"))
		Expect(p.Jobs[0].Group).To(Equal(""))
		Expect(p.Jobs[0].Interval).To(Equal(30 * time.Second))
		Expect(p.Jobs[1].Query).To(Equal("pg_stat"))
		Expect(p.Jobs[1].Group).To(Equal("a"))
		Expect(p.Jobs[1].Interval).To(Equal(time.Minute))
	})

	It("sorts jobs by database, then query", func() {
		defs := []queries.Definition{
			def("b", func(d *queries.Definition) { d.TargetDatabases = []string{"app", "postgres"} }),
			def("a", func(d *queries.Definition) { d.TargetDatabases = []string{"postgres"} }),
		}
		p := planner.Build(defs, baseSettings(), facts())
		Expect(jobDBs(p)).To(Equal([]string{"b@app", "a@postgres", "b@postgres"}))
		Expect(p.Jobs[0].Def.Key).To(Equal("b"))
	})
})

var _ = Describe("EffectiveInterval", func() {
	s := baseSettings()
	s.Groups = []settings.Group{{Name: "slow", Interval: time.Minute, Queries: []string{"slow_*"}}}

	It("uses the default interval and no group when none matches", func() {
		d := def("fast")
		g, i := planner.EffectiveInterval(&d, s)
		Expect(g).To(BeEmpty())
		Expect(i).To(Equal(30 * time.Second))
	})

	It("uses the group's interval", func() {
		d := def("slow_one")
		g, i := planner.EffectiveInterval(&d, s)
		Expect(g).To(Equal("slow"))
		Expect(i).To(Equal(time.Minute))
	})

	It("raises the interval to cache_seconds", func() {
		d := def("cached", func(d *queries.Definition) { d.CacheSeconds = 300 })
		_, i := planner.EffectiveInterval(&d, s)
		Expect(i).To(Equal(5 * time.Minute))
	})
})

var _ = Describe("AllMetricsExcluded", func() {
	It("is true only when every value metric is excluded", func() {
		d := def("probe")
		s := baseSettings()
		Expect(planner.AllMetricsExcluded(&d, s)).To(BeFalse())
		s.Exclude.Metrics = []string{"cnpg_probe_value"}
		Expect(planner.AllMetricsExcluded(&d, s)).To(BeTrue())
	})

	It("is false for a query without value columns", func() {
		d := def("labels", func(d *queries.Definition) { delete(d.Columns, "value") })
		s := baseSettings()
		s.Exclude.Metrics = []string{"*"}
		Expect(planner.AllMetricsExcluded(&d, s)).To(BeFalse())
	})
})
