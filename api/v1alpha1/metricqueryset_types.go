package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// MetricQuerySet holds custom metric queries in the CloudNativePG format: the same field names and
// meanings as the queries in a CloudNativePG monitoring ConfigMap, as a schema Kubernetes validates
// when the resource is applied. A TelemetryExporter lists the sets it uses in spec.metrics.querySets.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=mqs
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type MetricQuerySet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec holds the queries.
	Spec MetricQuerySetSpec `json:"spec"`
}

// MetricQuerySetList is a list of MetricQuerySet.
//
// +kubebuilder:object:root=true
type MetricQuerySetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MetricQuerySet `json:"items"`
}

// MetricQuerySetSpec holds the queries of a MetricQuerySet.
type MetricQuerySetSpec struct {
	// Queries maps a query key to its definition. A key starts with a letter or underscore and
	// continues with letters, digits and underscores, up to 63 characters. Unless the query sets name,
	// its metrics are named cnpg_<key>_<column>. A metric name must not start with cnpg_otel_.
	//
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=128
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-zA-Z_][a-zA-Z0-9_]{0,62}$'))",message="query names must match ^[a-zA-Z_][a-zA-Z0-9_]{0,62}$"
	Queries map[string]Query `json:"queries"`
}

// Query is one query and the metrics its result columns produce. Field names and meanings are those
// of a CloudNativePG custom query.
type Query struct {
	// Query is the SQL to run. It runs in a read-only transaction that sets the sidecar's limits
	// (statement_timeout, lock_timeout, idle_in_transaction_session_timeout, jit,
	// max_parallel_workers_per_gather), unless Query or PredicateQuery names pg_settings or
	// pg_show_all_settings, bare or as pg_catalog.pg_settings or pg_catalog.pg_show_all_settings,
	// outside comments and string constants. Such a query reports the server's values and is bounded
	// by the run deadline instead. A query that reaches pg_settings only through a view or a function
	// reports the sidecar's limits.
	Query string `json:"query"`

	// PredicateQuery is SQL that runs first, in the same transaction. Query runs only when the first
	// column of the first row is true.
	//
	// +optional
	PredicateQuery string `json:"predicate_query,omitempty"`

	// Metrics lists the result columns, one single-key item per column: the key is the column name
	// and the value says how to use it. The result must have as many columns as the list has, and a
	// HISTOGRAM column also needs its _bucket, _sum and _count columns in the result.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +listType=atomic
	Metrics []MetricColumn `json:"metrics"`

	// Master is the old name of Primary: the query runs on the primary only and is skipped on a replica.
	// The default is false.
	//
	// +optional
	Master bool `json:"master,omitempty"`

	// Primary runs the query on the primary only, and skips it on a replica. The default is false.
	//
	// +optional
	Primary bool `json:"primary,omitempty"`

	// CacheSeconds is the minimum number of seconds between two runs of the query. A shorter interval is
	// raised to this value. The default is 0, no minimum.
	//
	// +kubebuilder:validation:Minimum=0
	// +optional
	CacheSeconds *int64 `json:"cache_seconds,omitempty"`

	// RunOnServer is a semantic version range of PostgreSQL servers the query runs on, such as
	// ">=15.0.0". A query with an invalid range is disabled. When empty, it runs on every version.
	//
	// +optional
	RunOnServer string `json:"runonserver,omitempty"`

	// TargetDatabases lists the databases to run the query in. A name with *, ? or [ is a Go path.Match
	// pattern matched against the databases that accept connections and are not templates. When empty,
	// the query runs in the Cluster's application database, which is postgres when the bootstrap sets none.
	//
	// +kubebuilder:validation:MaxItems=64
	// +optional
	TargetDatabases []string `json:"target_databases,omitempty"`

	// Name replaces the query key in the metric names: cnpg_<name>_<column>.
	//
	// +optional
	Name string `json:"name,omitempty"`
}

// MetricColumn describes one result column: a single entry whose key is the column name.
//
// +kubebuilder:validation:MinProperties=1
// +kubebuilder:validation:MaxProperties=1
// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-zA-Z_][a-zA-Z0-9_]{0,62}$'))",message="column names must match ^[a-zA-Z_][a-zA-Z0-9_]{0,62}$"
type MetricColumn map[string]ColumnSpec

// ColumnSpec says how a result column is used.
type ColumnSpec struct {
	// Usage is what the column is: a LABEL of every metric of the query, a COUNTER (a cumulative sum), a
	// GAUGE, a HISTOGRAM, a MAPPEDMETRIC whose text is turned into a number through MetricMapping, a
	// DURATION whose Go duration text, such as 1.5s, is turned into milliseconds, or DISCARD to ignore it.
	//
	// +kubebuilder:validation:Enum=LABEL;COUNTER;GAUGE;HISTOGRAM;MAPPEDMETRIC;DURATION;DISCARD
	Usage string `json:"usage"`

	// Description is the metric's description. A LABEL or DISCARD column does not use it.
	//
	// +optional
	Description string `json:"description,omitempty"`

	// MetricMapping maps the text values of a MAPPEDMETRIC column to numbers. A value it does not list
	// drops that sample.
	//
	// +kubebuilder:validation:MaxProperties=256
	// +optional
	MetricMapping map[string]float64 `json:"metric_mapping,omitempty"`

	// MetricsMapping is MetricMapping under the other spelling, the one CloudNativePG's documentation
	// uses. MetricMapping wins when both are set.
	//
	// +kubebuilder:validation:MaxProperties=256
	// +optional
	MetricsMapping map[string]float64 `json:"metrics_mapping,omitempty"`

	// Name replaces the column name in the metric name: cnpg_<query name>_<name>. A DURATION metric
	// also ends in _milliseconds.
	//
	// +optional
	Name string `json:"name,omitempty"`

	// PgVersion is a semantic version range of PostgreSQL servers. On another version, the metric of a
	// value column is not exported, but the column must still be in the result. It has no effect on a
	// LABEL or DISCARD column. An invalid range is ignored with a warning, as if the field were empty.
	// When empty, the column applies to every version.
	//
	// +optional
	PgVersion string `json:"pg_version,omitempty"`
}
