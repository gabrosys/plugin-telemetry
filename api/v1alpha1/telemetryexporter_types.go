package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// TelemetryExporter says where and how the plugin sidecar exports telemetry for the Clusters in its
// namespace. One TelemetryExporter can serve every Cluster in the namespace; a Cluster picks it with the
// plugin parameter "exporter", and uses the one named default when the parameter is not set.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=texp
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.spec.exporter.endpoint`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type TelemetryExporter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec holds the exporter settings. It sets exporter.endpoint, or envFrom with an
	// OTEL_EXPORTER_OTLP_ENDPOINT key.
	Spec TelemetryExporterSpec `json:"spec"`
}

// TelemetryExporterList is a list of TelemetryExporter.
//
// +kubebuilder:object:root=true
type TelemetryExporterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TelemetryExporter `json:"items"`
}

// TelemetryExporterSpec holds what every signal shares, then one section per signal.
//
// +kubebuilder:validation:XValidation:rule="(has(self.envFrom) && (has(self.envFrom.configMap) || has(self.envFrom.secret))) || (has(self.exporter) && has(self.exporter.endpoint))",message="set exporter.endpoint or envFrom"
// +kubebuilder:validation:XValidation:rule="!has(self.exporter) || !has(self.exporter.timeout) || !has(self.metrics) || !has(self.metrics.interval) || duration(self.exporter.timeout).getMilliseconds() * 2 <= duration(self.metrics.interval).getMilliseconds()",message="exporter.timeout times 2 must not exceed metrics.interval"
type TelemetryExporterSpec struct {
	// Exporter is the OTLP connection every signal shares.
	//
	// +optional
	Exporter OTLPExporter `json:"exporter,omitempty"`

	// EnvFrom names a ConfigMap and a Secret whose OTEL_EXPORTER_OTLP_* and OTEL_RESOURCE_ATTRIBUTES
	// keys are read as settings.
	//
	// +optional
	EnvFrom *EnvFromSource `json:"envFrom,omitempty"`

	// ResourceLabels lists Cluster label keys to copy into the resource attributes. The instance role
	// label is not allowed, because it changes at switchover.
	//
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=317
	// +kubebuilder:validation:items:Pattern=`^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="!self.exists(l, l == 'cnpg.io/instanceRole')",message="resourceLabels must not include cnpg.io/instanceRole, which changes at switchover"
	// +listType=set
	// +optional
	ResourceLabels []string `json:"resourceLabels,omitempty"`

	// ResourceAttributes adds fixed attributes to every resource.
	//
	// +kubebuilder:validation:MaxProperties=32
	// +optional
	ResourceAttributes map[string]string `json:"resourceAttributes,omitempty"`

	// Metrics holds the metrics settings.
	//
	// +optional
	Metrics MetricsSpec `json:"metrics,omitempty"`
}

// OTLPExporter is the OTLP connection every signal shares.
//
// +kubebuilder:validation:XValidation:rule=`!has(self.endpoint) || self.endpoint.matches(r'^https?://[^@/?#\s]+(/[^?#\s]*)?$')`,fieldPath=".endpoint",message="endpoint must be a base http(s) URL with a host, and no user info, query or fragment"
// +kubebuilder:validation:XValidation:rule="!has(self.tls) || !has(self.endpoint) || self.endpoint.startsWith('https://')",message="tls requires an https:// endpoint"
type OTLPExporter struct {
	// Protocol is the OTLP transport: http/protobuf or grpc.
	//
	// +kubebuilder:validation:Enum=http/protobuf;grpc
	// +optional
	Protocol string `json:"protocol,omitempty"`

	// Endpoint is the base URL of the collector, such as https://collector:4318. The signal's path is
	// appended for http/protobuf. It has a host, and no user info, query or fragment.
	//
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Compression is gzip or none.
	//
	// +kubebuilder:validation:Enum=gzip;none
	// +optional
	Compression string `json:"compression,omitempty"`

	// Timeout is the time one export attempt may take, such as 10s. At least 1s, and at most half of
	// spec.metrics.interval, which is 30s when not set.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="timeout must be at least 1s"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Headers is a Secret key holding request headers in the OTEL_EXPORTER_OTLP_HEADERS format.
	//
	// +optional
	Headers *KeySelector `json:"headers,omitempty"`

	// TLS sets the TLS options. It needs an https:// endpoint.
	//
	// +optional
	TLS *OTLPTLS `json:"tls,omitempty"`
}

// MetricsSpec holds the metrics settings.
type MetricsSpec struct {
	// Interval is how often metrics are exported, such as 30s. At least 1s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="interval must be at least 1s"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// MaxPointsPerRequest caps the data points in one export request.
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +optional
	MaxPointsPerRequest *int32 `json:"maxPointsPerRequest,omitempty"`

	// UseClusterMonitoring reads the queries of the Cluster's spec.monitoring ConfigMaps and Secrets,
	// as CloudNativePG does. The default is true.
	//
	// +optional
	UseClusterMonitoring *bool `json:"useClusterMonitoring,omitempty"`

	// QuerySets lists the MetricQuerySet names in this namespace to load.
	//
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=253
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +listType=set
	// +optional
	QuerySets []string `json:"querySets,omitempty"`

	// PlanInterval is how often the sidecar re-reads the query sources and plans the queries. At least 1s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="planInterval must be at least 1s"
	// +optional
	PlanInterval *metav1.Duration `json:"planInterval,omitempty"`

	// DefaultInterval is how often a query runs when no group sets its own. At least 1s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="defaultInterval must be at least 1s"
	// +optional
	DefaultInterval *metav1.Duration `json:"defaultInterval,omitempty"`

	// Groups run sets of queries at their own interval.
	//
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Groups []QueryGroup `json:"groups,omitempty"`

	// Limits bounds what the queries may cost. Each limit has a default in the sidecar.
	//
	// +optional
	Limits Limits `json:"limits,omitempty"`

	// Exclude removes queries, metrics and databases from collection.
	//
	// +optional
	Exclude Exclude `json:"exclude,omitempty"`
}

// OTLPTLS sets the TLS options of the OTLP connection.
type OTLPTLS struct {
	// CA is the certificate authority bundle that signs the collector's certificate.
	//
	// +optional
	CA *KeyReference `json:"ca,omitempty"`

	// ClientCertificate is a kubernetes.io/tls Secret with the client certificate, for mutual TLS.
	//
	// +optional
	ClientCertificate *LocalObjectReference `json:"clientCertificate,omitempty"`

	// ServerName is the name to check in the collector's certificate.
	//
	// +optional
	ServerName string `json:"serverName,omitempty"`

	// MinVersion is the lowest TLS version to accept: 1.2 or 1.3.
	//
	// +kubebuilder:validation:Enum="1.2";"1.3"
	// +optional
	MinVersion string `json:"minVersion,omitempty"`
}

// KeyReference points to a key of a Secret or of a ConfigMap.
//
// +kubebuilder:validation:XValidation:rule="has(self.secret) != has(self.configMap)",message="set exactly one of secret or configMap"
type KeyReference struct {
	// Secret is a key of a Secret in this namespace.
	//
	// +optional
	Secret *KeySelector `json:"secret,omitempty"`

	// ConfigMap is a key of a ConfigMap in this namespace.
	//
	// +optional
	ConfigMap *KeySelector `json:"configMap,omitempty"`
}

// KeySelector names a key of an object in this namespace.
type KeySelector struct {
	// Name is the object's name.
	//
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key is the key in the object's data.
	//
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// LocalObjectReference names an object in this namespace.
type LocalObjectReference struct {
	// Name is the object's name.
	//
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// EnvFromSource names the objects that hold OTEL_* settings.
//
// +kubebuilder:validation:XValidation:rule="has(self.configMap) || has(self.secret)",message="set at least one of configMap or secret"
type EnvFromSource struct {
	// ConfigMap is a ConfigMap in this namespace.
	//
	// +optional
	ConfigMap *LocalObjectReference `json:"configMap,omitempty"`

	// Secret is a Secret in this namespace.
	//
	// +optional
	Secret *LocalObjectReference `json:"secret,omitempty"`
}

// QueryGroup runs a set of queries at its own interval.
type QueryGroup struct {
	// Name identifies the group.
	//
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Interval is how often the group's queries run. At least 1s. The default is spec.metrics.defaultInterval.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="interval must be at least 1s"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// Queries lists query names or Go path.Match patterns, such as pg_stat_*.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	Queries []string `json:"queries"`
}

// Limits bounds what the queries may cost. A limit left unset keeps the sidecar's default.
type Limits struct {
	// MaxConnections is the most database connections the sidecar opens.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxConnections *int32 `json:"maxConnections,omitempty"`

	// MaxRowsPerQuery is the most rows read from one query.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxRowsPerQuery *int32 `json:"maxRowsPerQuery,omitempty"`

	// MaxSeriesPerQuery is the most series one query may produce.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxSeriesPerQuery *int32 `json:"maxSeriesPerQuery,omitempty"`

	// MaxSeriesTotal is the most series all queries may produce together.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxSeriesTotal *int32 `json:"maxSeriesTotal,omitempty"`

	// MaxDatabases is the most databases a query runs in.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxDatabases *int32 `json:"maxDatabases,omitempty"`

	// MaxLabelValueLength is the longest label value kept.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxLabelValueLength *int32 `json:"maxLabelValueLength,omitempty"`

	// MaxLabelsPerSeries is the most labels a series may have.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxLabelsPerSeries *int32 `json:"maxLabelsPerSeries,omitempty"`

	// QueryTimeout is the time one query may run. At least 100ms, at most 1073741823ms (twice it must fit
	// PostgreSQL's idle_in_transaction_session_timeout).
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('100ms')",message="queryTimeout must be at least 100ms"
	// +kubebuilder:validation:XValidation:rule="duration(self) <= duration('1073741823ms')",message="queryTimeout must be at most 1073741823ms"
	// +optional
	QueryTimeout *metav1.Duration `json:"queryTimeout,omitempty"`

	// LockTimeout is the time a query waits for a lock. More than zero, at most 2147483647ms (PostgreSQL's
	// lock_timeout maximum).
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="lockTimeout must be more than 0s"
	// +kubebuilder:validation:XValidation:rule="duration(self) <= duration('2147483647ms')",message="lockTimeout must be at most 2147483647ms"
	// +optional
	LockTimeout *metav1.Duration `json:"lockTimeout,omitempty"`
}

// Exclude removes queries, metrics and databases from collection.
type Exclude struct {
	// Queries lists query names never planned, so their SQL never runs. A name may be a Go path.Match pattern.
	//
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Queries []string `json:"queries,omitempty"`

	// Metrics lists metric names to drop. A name may be a Go path.Match pattern.
	//
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Metrics []string `json:"metrics,omitempty"`

	// Databases lists database names to skip. A name may be a Go path.Match pattern.
	//
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Databases []string `json:"databases,omitempty"`
}
