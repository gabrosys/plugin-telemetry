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

	// Spec holds the exporter settings. The endpoint is required: set spec.exporter.endpoint, or put
	// OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT in envFrom.
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
// +kubebuilder:validation:XValidation:rule="!has(self.exporter) || !has(self.exporter.timeout) || duration(self.exporter.timeout).getMilliseconds() * 2 <= (has(self.metrics) && has(self.metrics.interval) ? duration(self.metrics.interval).getMilliseconds() : 30000)",message="exporter.timeout times 2 must not exceed metrics.interval, which is 30s when not set"
type TelemetryExporterSpec struct {
	// Exporter is the OTLP connection every signal shares.
	//
	// +optional
	Exporter OTLPExporter `json:"exporter,omitempty"`

	// EnvFrom names a ConfigMap and a Secret whose OpenTelemetry environment keys are read as settings.
	// The sidecar reads the ENDPOINT, PROTOCOL, COMPRESSION, TIMEOUT (in milliseconds) and HEADERS keys
	// of OTEL_EXPORTER_OTLP_, where OTEL_EXPORTER_OTLP_METRICS_<NAME> wins over OTEL_EXPORTER_OTLP_<NAME>,
	// and OTEL_RESOURCE_ATTRIBUTES. A Secret key wins over the same ConfigMap key, and a field of
	// spec.exporter wins over both. HEADERS is read from the Secret only. The keys CERTIFICATE,
	// CLIENT_KEY and CLIENT_CERTIFICATE are rejected: use spec.exporter.tls. OTEL_SERVICE_NAME is
	// ignored, because service.name is the Cluster name.
	//
	// +optional
	EnvFrom *EnvFromSource `json:"envFrom,omitempty"`

	// ResourceLabels lists Cluster label keys to copy into the resource attributes, each as
	// cnpg.cluster.label.<key>. A label the Cluster does not have is skipped. Such an attribute wins
	// over the same key in resourceAttributes. The instance role label (cnpg.io/instanceRole) is not
	// allowed, because it changes at switchover.
	//
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=317
	// +kubebuilder:validation:items:Pattern=`^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="!self.exists(l, l == 'cnpg.io/instanceRole')",message="resourceLabels must not include cnpg.io/instanceRole, which changes at switchover"
	// +listType=set
	// +optional
	ResourceLabels []string `json:"resourceLabels,omitempty"`

	// ResourceAttributes adds fixed attributes to the resource of every export. They win over
	// OTEL_RESOURCE_ATTRIBUTES from envFrom. The sidecar sets service.name, service.namespace,
	// service.instance.id and the k8s.namespace.name, k8s.pod.name, k8s.pod.uid, k8s.node.name and
	// k8s.container.name attributes itself, and an entry with one of these keys is dropped.
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
// +kubebuilder:validation:XValidation:rule="!has(self.endpoint) || isURL(self.endpoint)",fieldPath=".endpoint",message="endpoint must parse as a URL"
// +kubebuilder:validation:XValidation:rule="!has(self.tls) || !has(self.endpoint) || self.endpoint.startsWith('https://')",message="tls requires an https:// endpoint"
type OTLPExporter struct {
	// Protocol is the OTLP transport: http/protobuf or grpc. When empty, the PROTOCOL key of envFrom
	// is used, else http/protobuf.
	//
	// +kubebuilder:validation:Enum=http/protobuf;grpc
	// +optional
	Protocol string `json:"protocol,omitempty"`

	// Endpoint is the URL of the collector, such as https://collector:4318. An http:// endpoint is
	// plaintext and an https:// one uses TLS. For http/protobuf, v1/metrics is appended to it. It has a
	// host, and no user info, query or fragment. When empty, the ENDPOINT key of envFrom is used.
	//
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Compression is gzip or none. When empty, the COMPRESSION key of envFrom is used, else gzip.
	//
	// +kubebuilder:validation:Enum=gzip;none
	// +optional
	Compression string `json:"compression,omitempty"`

	// Timeout is the time one export attempt may take, such as 10s. It must be at least 1s and at most
	// half of spec.metrics.interval, which is 30s when not set. When empty, the TIMEOUT key of envFrom is used,
	// else the lower of 10s and half the interval.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="timeout must be at least 1s"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Headers is a key of a Secret in this namespace that holds request headers in the
	// OTEL_EXPORTER_OTLP_HEADERS format: name=value pairs separated by commas, values percent-encoded.
	// When empty, the HEADERS key of the envFrom Secret is used.
	//
	// +optional
	Headers *KeySelector `json:"headers,omitempty"`

	// TLS sets the TLS options. It needs an https:// endpoint. When empty, the system roots verify the
	// collector, no client certificate is sent and TLS 1.2 is the lowest version.
	//
	// +optional
	TLS *OTLPTLS `json:"tls,omitempty"`
}

// MetricsSpec holds the metrics settings.
type MetricsSpec struct {
	// Interval is how often metrics are exported, such as 30s. It must be at least 1s. The default is 30s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="interval must be at least 1s"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// MaxPointsPerRequest caps the data points in one export request. An export with more points is
	// split into several requests. The default is 2000.
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +optional
	MaxPointsPerRequest *int32 `json:"maxPointsPerRequest,omitempty"`

	// UseClusterMonitoring loads the queries listed in the Cluster's spec.monitoring.customQueriesConfigMap
	// and spec.monitoring.customQueriesSecret, as CloudNativePG does. The default is true. The plugin's
	// own built-in queries load either way.
	//
	// +optional
	UseClusterMonitoring *bool `json:"useClusterMonitoring,omitempty"`

	// QuerySets lists the MetricQuerySet names in this namespace to load, after the Cluster's monitoring
	// queries. When a query key appears twice, the later source wins.
	//
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=253
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +listType=set
	// +optional
	QuerySets []string `json:"querySets,omitempty"`

	// PlanInterval is how often the sidecar reads the server version, the databases and the settings it
	// checks from PostgreSQL, and plans the queries again. Changes to the query sources apply when they
	// happen, not on this timer. At least 1s. The default is 30s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="planInterval must be at least 1s"
	// +optional
	PlanInterval *metav1.Duration `json:"planInterval,omitempty"`

	// DefaultInterval is how often a query runs when no group matches it. At least 1s. When empty, the
	// Cluster's spec.monitoring.metricsQueriesTTL is used if it is at least 1s, else 30s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="defaultInterval must be at least 1s"
	// +optional
	DefaultInterval *metav1.Duration `json:"defaultInterval,omitempty"`

	// Groups run sets of queries at their own interval. A query that no group matches runs at
	// defaultInterval.
	//
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Groups []QueryGroup `json:"groups,omitempty"`

	// Limits bounds what the queries may cost. A limit left out keeps its default.
	//
	// +optional
	Limits Limits `json:"limits,omitempty"`

	// Exclude removes queries, metrics and databases from collection. It is empty by default.
	//
	// +optional
	Exclude Exclude `json:"exclude,omitempty"`
}

// OTLPTLS sets the TLS options of the OTLP connection.
type OTLPTLS struct {
	// CA is a PEM bundle of the certificate authorities that sign the collector's certificate. It is
	// added to the system roots.
	//
	// +optional
	CA *KeyReference `json:"ca,omitempty"`

	// ClientCertificate is a Secret in this namespace that holds the client certificate in tls.crt and
	// its key in tls.key, for mutual TLS.
	//
	// +optional
	ClientCertificate *LocalObjectReference `json:"clientCertificate,omitempty"`

	// ServerName is the name to check in the collector's certificate. When empty, the host of the
	// endpoint is used.
	//
	// +optional
	ServerName string `json:"serverName,omitempty"`

	// MinVersion is the lowest TLS version to accept: 1.2 or 1.3. The default is 1.2.
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
	// Name identifies the group. It is unique in the list.
	//
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Interval is how often the group's queries run. At least 1s. The default is spec.metrics.defaultInterval.
	// A query whose cache_seconds is higher runs at that interval instead.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s')",message="interval must be at least 1s"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// Queries lists query keys or Go path.Match patterns, such as pg_stat_*. A key is the name under
	// which the query is defined, not its name field. A query that several groups match uses the first
	// of them in the list.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	Queries []string `json:"queries"`
}

// Limits bounds what the queries may cost. A limit left unset keeps the sidecar's default.
type Limits struct {
	// MaxConnections is the most database connections the sidecar opens for queries. The default is 4.
	// Every 10 seconds it also opens one more short connection, outside this cap, to see whether
	// PostgreSQL is shutting down, so the monitoring role needs a connection limit of maxConnections + 1
	// or more.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxConnections *int32 `json:"maxConnections,omitempty"`

	// MaxRowsPerQuery is the most rows one query may return. A query with more is stopped and exports
	// nothing in that run. The default is 10000.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxRowsPerQuery *int32 `json:"maxRowsPerQuery,omitempty"`

	// MaxSeriesPerQuery is the most series one query may produce in one database. A query with more
	// exports nothing in that run. The default is 10000.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxSeriesPerQuery *int32 `json:"maxSeriesPerQuery,omitempty"`

	// MaxSeriesTotal is the most series all queries may produce together. A result that would pass it is
	// not stored, and that query stops exporting until a later result fits. The default is 50000.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxSeriesTotal *int32 `json:"maxSeriesTotal,omitempty"`

	// MaxDatabases is the most databases one query runs in. The databases it matches are taken in name
	// order and the rest are skipped. The default is 50.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxDatabases *int32 `json:"maxDatabases,omitempty"`

	// MaxLabelValueLength is the longest label value, in bytes. A row with a longer value is dropped.
	// The default is 1024.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxLabelValueLength *int32 `json:"maxLabelValueLength,omitempty"`

	// MaxLabelsPerSeries is the most label columns a query may have. A query with more is disabled.
	// The default is 32.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxLabelsPerSeries *int32 `json:"maxLabelsPerSeries,omitempty"`

	// QueryTimeout is the time one query may run, set as statement_timeout. It must be at least 100ms and
	// at most 1073741823ms, so that twice it fits PostgreSQL's idle_in_transaction_session_timeout. The
	// default is 10s.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('100ms')",message="queryTimeout must be at least 100ms"
	// +kubebuilder:validation:XValidation:rule="duration(self) <= duration('1073741823ms')",message="queryTimeout must be at most 1073741823ms"
	// +optional
	QueryTimeout *metav1.Duration `json:"queryTimeout,omitempty"`

	// LockTimeout is the time a query waits for a lock, set as lock_timeout. It must be more than zero and
	// at most 2147483647ms (PostgreSQL's lock_timeout maximum). The default is 1s.
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
	// Queries lists query keys never planned, so their SQL never runs. A key may be a Go path.Match pattern.
	//
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Queries []string `json:"queries,omitempty"`

	// Metrics lists full metric names to drop, such as cnpg_pg_stat_database_xact_commit. A name may be
	// a Go path.Match pattern. A query whose metrics are all dropped does not run.
	//
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Metrics []string `json:"metrics,omitempty"`

	// Databases lists database names to skip: no query runs in them. A name may be a Go path.Match pattern.
	//
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Databases []string `json:"databases,omitempty"`
}
