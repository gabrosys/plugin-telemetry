package queries

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// Kind is the instrument type of a value column.
type Kind int

// The kinds of value columns. The zero Kind means the column has no value.
const (
	KindCounter Kind = iota + 1
	KindGauge
	KindHistogram
)

func (k Kind) String() string {
	switch k {
	case KindCounter:
		return "counter"
	case KindGauge:
		return "gauge"
	case KindHistogram:
		return "histogram"
	default:
		return ""
	}
}

// Role is what a result column does for the query.
type Role int

// The roles of result columns.
const (
	RoleValue Role = iota
	RoleLabel
	RoleDiscard
	RoleHistogramPart
)

func (r Role) String() string {
	switch r {
	case RoleValue:
		return "value"
	case RoleLabel:
		return "label"
	case RoleDiscard:
		return "discard"
	case RoleHistogramPart:
		return "histogram-part"
	default:
		return ""
	}
}

// otelPrefix is the prefix of the engine's own metrics.
const otelPrefix = "cnpg_otel_"

// Column is one expected result column.
type Column struct {
	Name        string // result column
	Role        Role
	Usage       Usage
	Metric      string // "cnpg_<ns>_<column or name>", "_milliseconds" for DURATION; empty unless RoleValue
	Kind        Kind
	Description string
	Mapping     map[string]float64  // MAPPEDMETRIC
	Versions    *semver.Constraints // nil: every version
}

// Definition is a query ready to plan: the columns its result must have and the names of its metrics.
type Definition struct {
	Key             string
	Namespace       string // "cnpg_" + (Name or Key)
	SQL, Predicate  string
	PrimaryOnly     bool                // primary || master
	RunOnServer     *semver.Constraints // nil: every version
	TargetDatabases []string            // as written; empty means the default database
	CacheSeconds    uint64
	Columns         map[string]Column // one entry per expected result column
	Labels          []string          // label columns in YAML order
	Fingerprint     string            // hex sha256 over everything that shapes samples
	Warnings        []string
	Disabled        string // non-empty: never planned, with this reason
}

// Options tune Define.
type Options struct {
	// MaxLabelsPerSeries disables a query with more label columns. Zero turns the check off.
	MaxLabelsPerSeries int
}

// Builtin is CNPG's hard-coded "collector" query (pg_collector.go:639-665 at v1.30.1).
func Builtin() UserQueries {
	return UserQueries{
		"collector": UserQuery{
			Query: "SELECT pg_catalog.current_database() as datname, relpages as lo_pages " +
				"FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace) " +
				"WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_largeobject';",
			TargetDatabases: []string{"*"},
			Metrics: []Mapping{
				{"datname": ColumnMapping{Usage: UsageLabel, Description: "Name of the database"}},
				{"lo_pages": ColumnMapping{Usage: UsageGauge, Description: "Estimated number of pages in the pg_largeobject table"}},
			},
		},
	}
}

// Define turns a query into its Definition. Problems that leave the query usable become Warnings;
// the others set Disabled.
func Define(key string, q UserQuery, opts Options) Definition {
	name := key
	if q.Name != "" {
		name = q.Name
	}

	def := Definition{
		Key:             key,
		Namespace:       "cnpg_" + name,
		SQL:             q.Query,
		Predicate:       q.PredicateQuery,
		PrimaryOnly:     q.Primary || q.Master,
		TargetDatabases: append([]string(nil), q.TargetDatabases...),
		CacheSeconds:    q.CacheSeconds,
		Columns:         make(map[string]Column),
	}

	var runOnServerSource string
	var disabled []string
	if strings.TrimSpace(q.Query) == "" {
		disabled = append(disabled, "query is empty")
	}
	if q.RunOnServer != "" {
		constraint, err := semver.NewConstraint(q.RunOnServer)
		if err != nil {
			disabled = append(disabled, fmt.Sprintf("invalid runonserver %q: %v", q.RunOnServer, err))
		} else {
			def.RunOnServer = constraint
			runOnServerSource = q.RunOnServer
		}
	}

	sources := def.addColumns(q)
	def.Labels = def.labelColumns(q)

	for _, column := range def.Columns {
		if column.Role == RoleValue && strings.HasPrefix(column.Metric, otelPrefix) {
			disabled = append(disabled, fmt.Sprintf("metric names start with %s", otelPrefix))

			break
		}
	}
	if opts.MaxLabelsPerSeries > 0 && len(def.Labels) > opts.MaxLabelsPerSeries {
		disabled = append(disabled, fmt.Sprintf("%d labels, more than the limit of %d", len(def.Labels), opts.MaxLabelsPerSeries))
	}
	if len(disabled) > 0 {
		def.Disabled = disabled[0]
	}

	if selectsSeveralDatabases(q.TargetDatabases) {
		if relations := clusterWideRelations(q.Query); len(relations) > 0 {
			def.Warnings = append(def.Warnings, fmt.Sprintf(
				"cluster-wide relations (%s) are read in every selected database, so each reports the same rows",
				strings.Join(relations, ", ")))
		}
	}

	fingerprint, err := def.fingerprint(runOnServerSource, sources)
	if err != nil {
		def.Disabled = fmt.Sprintf("fingerprint: %v", err)
	}
	def.Fingerprint = fingerprint

	return def
}

// addColumns fills Columns as CNPG's ToMetricMap does (mappings.go:79-232 at v1.30.1) and returns
// the pg_version text each column was built with.
func (d *Definition) addColumns(q UserQuery) map[string]string {
	sources := make(map[string]string)

	for _, item := range q.Metrics {
		for _, columnName := range sortedKeys(item) {
			cm := item[columnName]

			var versions *semver.Constraints
			var source string
			if cm.PGVersion != "" {
				constraint, err := semver.NewConstraint(cm.PGVersion)
				if err != nil {
					d.Warnings = append(d.Warnings, fmt.Sprintf("column %q: ignoring invalid pg_version %q: %v", columnName, cm.PGVersion, err))
				} else {
					versions, source = constraint, cm.PGVersion
				}
			}

			metricName := d.Namespace + "_" + columnName
			if cm.Name != "" {
				metricName = d.Namespace + "_" + cm.Name
			}

			base := Column{Name: columnName, Usage: cm.Usage, Versions: versions}
			switch cm.Usage {
			case UsageDiscard:
				base.Role = RoleDiscard
				d.setColumn(base, source, sources)
			case UsageLabel:
				base.Role = RoleLabel
				d.setColumn(base, source, sources)
			case UsageCounter:
				d.setValue(base, source, sources, metricName, cm.Description, KindCounter)
			case UsageGauge:
				d.setValue(base, source, sources, metricName, cm.Description, KindGauge)
			case UsageHistogram:
				d.setValue(base, source, sources, metricName, cm.Description, KindHistogram)
				for _, suffix := range []string{"_bucket", "_sum", "_count"} {
					part := base
					part.Name = columnName + suffix
					part.Role = RoleHistogramPart
					d.setColumn(part, source, sources)
				}
			case UsageMappedMetric:
				base.Mapping = d.mapping(columnName, cm)
				d.setValue(base, source, sources, metricName, cm.Description, KindGauge)
			case UsageDuration:
				d.setValue(base, source, sources, metricName+"_milliseconds", cm.Description, KindGauge)
			default:
				d.Warnings = append(d.Warnings, fmt.Sprintf("column %q: unknown usage %q, the column is not mapped", columnName, cm.Usage))
			}
		}
	}

	return sources
}

func (d *Definition) setColumn(c Column, source string, sources map[string]string) {
	d.Columns[c.Name] = c
	sources[c.Name] = source
}

func (d *Definition) setValue(c Column, source string, sources map[string]string, metric, description string, kind Kind) {
	c.Role = RoleValue
	c.Metric = metric
	c.Description = description
	c.Kind = kind
	d.setColumn(c, source, sources)
}

// mapping picks metric_mapping, else metrics_mapping.
func (d *Definition) mapping(columnName string, cm ColumnMapping) map[string]float64 {
	chosen := cm.MetricMapping
	if len(cm.MetricMapping) > 0 && len(cm.MetricsMapping) > 0 {
		d.Warnings = append(d.Warnings, fmt.Sprintf("column %q: both metric_mapping and metrics_mapping are set, using metric_mapping", columnName))
	}
	if len(chosen) == 0 {
		chosen = cm.MetricsMapping
	}

	out := make(map[string]float64, len(chosen))
	for k, v := range chosen {
		out[k] = v
	}

	return out
}

// labelColumns lists the label columns in YAML order, once each, as they stand after overwrites.
func (d *Definition) labelColumns(q UserQuery) []string {
	var labels []string
	seen := make(map[string]struct{})
	for _, item := range q.Metrics {
		for _, columnName := range sortedKeys(item) {
			if _, done := seen[columnName]; done {
				continue
			}
			if column, ok := d.Columns[columnName]; ok && column.Role == RoleLabel {
				labels = append(labels, columnName)
				seen[columnName] = struct{}{}
			}
		}
	}

	return labels
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

type canonicalColumn struct {
	Name        string            `json:"name"`
	Role        string            `json:"role"`
	Usage       string            `json:"usage"`
	Metric      string            `json:"metric"`
	Kind        string            `json:"kind"`
	Description string            `json:"description"`
	Mapping     map[string]string `json:"mapping"`
	Versions    string            `json:"versions"`
}

type canonicalDefinition struct {
	Key             string            `json:"key"`
	Namespace       string            `json:"namespace"`
	SQL             string            `json:"sql"`
	Predicate       string            `json:"predicate"`
	PrimaryOnly     bool              `json:"primaryOnly"`
	RunOnServer     string            `json:"runOnServer"`
	TargetDatabases []string          `json:"targetDatabases"`
	CacheSeconds    uint64            `json:"cacheSeconds"`
	Columns         []canonicalColumn `json:"columns"`
	Labels          []string          `json:"labels"`
	Disabled        string            `json:"disabled"`
}

// fingerprint hashes the Definition without Warnings and Fingerprint, written from plain values
// (version constraints by source text, sorted columns, mapping values as text) so it is stable across runs.
func (d *Definition) fingerprint(runOnServer string, versionSources map[string]string) (string, error) {
	canonical := canonicalDefinition{
		Key:             d.Key,
		Namespace:       d.Namespace,
		SQL:             d.SQL,
		Predicate:       d.Predicate,
		PrimaryOnly:     d.PrimaryOnly,
		RunOnServer:     runOnServer,
		TargetDatabases: d.TargetDatabases,
		CacheSeconds:    d.CacheSeconds,
		Columns:         []canonicalColumn{},
		Labels:          d.Labels,
		Disabled:        d.Disabled,
	}
	for _, name := range sortedKeys(d.Columns) {
		column := d.Columns[name]
		mapping := make(map[string]string, len(column.Mapping))
		for k, v := range column.Mapping {
			mapping[k] = strconv.FormatFloat(v, 'g', -1, 64)
		}
		canonical.Columns = append(canonical.Columns, canonicalColumn{
			Name:        column.Name,
			Role:        column.Role.String(),
			Usage:       string(column.Usage),
			Metric:      column.Metric,
			Kind:        column.Kind.String(),
			Description: column.Description,
			Mapping:     mapping,
			Versions:    versionSources[name],
		})
	}

	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode definition: %w", err)
	}
	sum := sha256.Sum256(encoded)

	return hex.EncodeToString(sum[:]), nil
}
