// Package queries reads CloudNativePG's custom-queries YAML and turns each query into a Definition.
package queries

import (
	"fmt"

	"go.yaml.in/yaml/v4"
)

// Usage says how a result column is used. The values are case-sensitive, as in CloudNativePG.
type Usage string

const (
	// UsageLabel makes the column a label of every metric of the query.
	UsageLabel Usage = "LABEL"
	// UsageCounter makes the column a monotonic sum.
	UsageCounter Usage = "COUNTER"
	// UsageGauge makes the column a gauge.
	UsageGauge Usage = "GAUGE"
	// UsageHistogram makes the column, with its _bucket, _sum and _count columns, a histogram.
	UsageHistogram Usage = "HISTOGRAM"
	// UsageMappedMetric turns the column's text into a gauge through a mapping.
	UsageMappedMetric Usage = "MAPPEDMETRIC"
	// UsageDuration turns the column's duration text into a gauge in milliseconds.
	UsageDuration Usage = "DURATION"
	// UsageDiscard ignores the column.
	UsageDiscard Usage = "DISCARD"
)

// ColumnMapping describes one result column. Field tags follow CloudNativePG v1.30.1
// pkg/management/postgres/metrics/parser.go:51-64, plus MetricsMapping.
type ColumnMapping struct {
	Usage         Usage              `yaml:"usage"`
	Description   string             `yaml:"description"`
	MetricMapping map[string]float64 `yaml:"metric_mapping"`
	// MetricsMapping is decoded loosely, so a malformed value can't fail the file; Define checks it.
	MetricsMapping any    `yaml:"metrics_mapping"`
	PGVersion      string `yaml:"pg_version"`
	Name           string `yaml:"name"`
}

// Mapping is one item of a query's metrics list.
type Mapping map[string]ColumnMapping

// UserQuery is one custom query. Field tags follow CloudNativePG v1.30.1
// pkg/management/postgres/metrics/parser.go:35-46.
type UserQuery struct {
	Query           string    `yaml:"query"`
	PredicateQuery  string    `yaml:"predicate_query"`
	Metrics         []Mapping `yaml:"metrics"`
	Master          bool      `yaml:"master"`
	Primary         bool      `yaml:"primary"`
	CacheSeconds    uint64    `yaml:"cache_seconds"`
	RunOnServer     string    `yaml:"runonserver"`
	TargetDatabases []string  `yaml:"target_databases"`
	Name            string    `yaml:"name"`
}

// UserQueries maps a query key to its query.
type UserQueries map[string]UserQuery

// Parse decodes CloudNativePG's custom-queries YAML, ignoring unknown keys.
func Parse(content []byte) (UserQueries, error) {
	var result UserQueries
	if err := yaml.Unmarshal(content, &result); err != nil {
		return nil, fmt.Errorf("parse user queries: %w", err)
	}

	return result, nil
}
