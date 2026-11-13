package queries_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	yamlv3 "go.yaml.in/yaml/v3"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

var _ = Describe("Parse", func() {
	It("parses the postgres_exporter example as CNPG does", func() {
		result, err := queries.Parse(readTestdata("cnpg-pg-exporter.yaml"))
		Expect(err).ToNot(HaveOccurred())

		Expect(result).ToNot(BeNil())
		Expect(result).To(HaveLen(7))

		Expect(result["pg_replication"].Query).To(Equal("SELECT CASE WHEN NOT pg_is_in_recovery() [...]"))
		Expect(result["pg_replication"].Primary).To(BeTrue())
		Expect(result["pg_replication"].Master).To(BeFalse())
		Expect(result["pg_replication"].Metrics).To(HaveLen(2))
		Expect(result["pg_replication"].Metrics[0]).To(HaveLen(1))
		Expect(result["pg_replication"].Metrics[0]["lag"].Usage).To(Equal(queries.Usage("GAUGE")))
		Expect(result["pg_replication"].Metrics[0]["lag"].Description).To(Equal(
			"Replication lag behind primary in seconds"))

		Expect(result["pg_postmaster"].Primary).To(BeFalse())
		Expect(result["pg_postmaster"].Master).To(BeTrue())
	})

	It("parses a query on several databases as CNPG does", func() {
		result, err := queries.Parse([]byte(`
some_query:
  query: |
    SELECT current_database() as datname, count(*) as rows FROM some_table
  predicate_query: |
    SELECT 1 as row FROM some_table WHERE some filters
  cache_seconds: 100
  metrics:
  - datname:
      usage: "LABEL"
      description: "Name of current database"
      pg_version: ">9.4 <11"
  - rows:
      usage: "GAUGE"
      description: "number of rows"
  target_databases:
  - test
  - app
`))
		Expect(err).ToNot(HaveOccurred())

		Expect(result).ToNot(BeNil())
		Expect(result).To(HaveLen(1))

		Expect(result["some_query"].Query).To(Equal("SELECT current_database() as datname, count(*)" +
			" as rows FROM some_table\n"))
		Expect(result["some_query"].PredicateQuery).To(Equal("SELECT 1 as row" +
			" FROM some_table WHERE some filters\n"))
		Expect(result["some_query"].Primary).To(BeFalse())
		Expect(result["some_query"].TargetDatabases).To(ContainElements("test", "app"))
		Expect(result["some_query"].CacheSeconds).To(BeEquivalentTo(100))
		Expect(result["some_query"].Master).To(BeFalse())
		Expect(result["some_query"].Metrics).To(HaveLen(2))
		Expect(result["some_query"].Metrics[0]["datname"].Usage).To(Equal(queries.Usage("LABEL")))
		Expect(result["some_query"].Metrics[0]["datname"].Description).To(Equal(
			"Name of current database"))
		Expect(result["some_query"].Metrics[0]["datname"].PGVersion).To(Equal(">9.4 <11"))
	})

	It("rejects YAML that doesn't parse", func() {
		result, err := queries.Parse([]byte(`
test:
  toast
    bread`))
		Expect(err).To(HaveOccurred())
		Expect(result).To(BeNil())
	})

	It("ignores unknown keys", func() {
		result, err := queries.Parse([]byte(`
q:
  query: SELECT 1 AS v
  unknown_field: 3
  metrics:
    - v:
        usage: GAUGE
        unknown_column_field: [a, b]
`))
		Expect(err).ToNot(HaveOccurred())
		Expect(result["q"].Query).To(Equal("SELECT 1 AS v"))
		Expect(result["q"].Metrics[0]["v"].Usage).To(Equal(queries.Usage("GAUGE")))
	})

	It("reads metrics_mapping next to metric_mapping", func() {
		result, err := queries.Parse([]byte(`
q:
  metrics:
    - a:
        usage: MAPPEDMETRIC
        metrics_mapping: {up: 1}
    - b:
        usage: MAPPEDMETRIC
        metric_mapping: {down: 0}
`))
		Expect(err).ToNot(HaveOccurred())
		Expect(result["q"].Metrics[0]["a"].MetricsMapping).To(Equal(map[string]any{"up": 1}))
		Expect(result["q"].Metrics[1]["b"].MetricMapping).To(Equal(map[string]float64{"down": 0}))
	})

	DescribeTable("decodes CNPG's corpus to the same structs as yaml/v3",
		func(file string, extract bool) {
			content := readTestdata(file)
			if extract {
				content = []byte(configMapQueries(content))
			}

			var v3 queries.UserQueries
			Expect(yamlv3.Unmarshal(content, &v3)).To(Succeed())

			got, err := queries.Parse(content)
			Expect(err).ToNot(HaveOccurred())
			Expect(got).To(Equal(v3))
			Expect(got).ToNot(BeEmpty())
		},
		Entry("default queries at v1.30.1", "default-monitoring-v1.30.1.yaml", true),
		Entry("default queries at main", "default-monitoring-main.yaml", true),
		Entry("the postgres_exporter example", "cnpg-pg-exporter.yaml", false),
	)

	It("fails to decode what is not a mapping of queries", func() {
		_, err := queries.Parse([]byte("- a\n- b\n"))
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprint(err)).To(ContainSubstring("parse"))
	})
})
