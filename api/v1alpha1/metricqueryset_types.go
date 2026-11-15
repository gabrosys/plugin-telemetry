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
	// Queries maps a query name to its definition. A name starts with a letter or underscore and
	// continues with letters, digits and underscores, up to 63 characters.
	//
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=128
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-zA-Z_][a-zA-Z0-9_]{0,62}$'))",message="query names must match ^[a-zA-Z_][a-zA-Z0-9_]{0,62}$"
	Queries map[string]Query `json:"queries"`
}

// Query is one query and the metrics its result columns produce. Field names and meanings are those
// of a CloudNativePG custom query.
type Query struct {
	// Query is the SQL to run.
	Query string `json:"query"`

	// PredicateQuery is SQL that must return a row for Query to run.
	//
	// +optional
	PredicateQuery string `json:"predicate_query,omitempty"`

	// Metrics lists the result columns, one single-key item per column: the key is the column name
	// and the value says how to use it.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +listType=atomic
	Metrics []MetricColumn `json:"metrics"`

	// Master runs the query on the primary only. It is the old name of Primary.
	//
	// +optional
	Master bool `json:"master,omitempty"`

	// Primary runs the query on the primary only.
	//
	// +optional
	Primary bool `json:"primary,omitempty"`

	// CacheSeconds is how long a result is reused before the query runs again.
	//
	// +kubebuilder:validation:Minimum=0
	// +optional
	CacheSeconds *int64 `json:"cache_seconds,omitempty"`

	// RunOnServer is a semantic version range of PostgreSQL servers the query runs on.
	//
	// +optional
	RunOnServer string `json:"runonserver,omitempty"`

	// TargetDatabases lists the databases to run the query in. Names may use wildcards. The default is
	// the cluster's default database.
	//
	// +kubebuilder:validation:MaxItems=64
	// +optional
	TargetDatabases []string `json:"target_databases,omitempty"`

	// Name replaces the query name in the metric names.
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
	// Usage is what the column is: a LABEL of every metric of the query, a COUNTER, a GAUGE, a
	// HISTOGRAM, a MAPPEDMETRIC whose text is turned into a number through MetricMapping, a DURATION
	// whose text is turned into milliseconds, or DISCARD to ignore it.
	//
	// +kubebuilder:validation:Enum=LABEL;COUNTER;GAUGE;HISTOGRAM;MAPPEDMETRIC;DURATION;DISCARD
	Usage string `json:"usage"`

	// Description is the metric's description.
	//
	// +optional
	Description string `json:"description,omitempty"`

	// MetricMapping maps the text values of a MAPPEDMETRIC column to numbers.
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

	// Name replaces the column name in the metric name.
	//
	// +optional
	Name string `json:"name,omitempty"`

	// PgVersion is a semantic version range of PostgreSQL servers the column applies to.
	//
	// +optional
	PgVersion string `json:"pg_version,omitempty"`
}
