# API Reference

## Packages
- [telemetry.gabrosys.github.io/v1alpha1](#telemetrygabrosysgithubiov1alpha1)

## telemetry.gabrosys.github.io/v1alpha1

Package v1alpha1 holds the TelemetryExporter and MetricQuerySet custom resources.

### Resource Types
- [MetricQuerySet](#metricqueryset)
- [TelemetryExporter](#telemetryexporter)

#### ColumnSpec

ColumnSpec says how a result column is used.

_Appears in:_
- [MetricColumn](#metriccolumn)

| Field | Description | Validation |
| --- | --- | --- |
| `usage` _string_ | Usage is what the column is: a LABEL of every metric of the query, a COUNTER (a cumulative sum), a<br />GAUGE, a HISTOGRAM, a MAPPEDMETRIC whose text is turned into a number through MetricMapping, a<br />DURATION whose Go duration text, such as 1.5s, is turned into milliseconds, or DISCARD to ignore it. | Enum: [LABEL COUNTER GAUGE HISTOGRAM MAPPEDMETRIC DURATION DISCARD] <br /> |
| `description` _string_ | Description is the metric's description. A LABEL or DISCARD column does not use it. | Optional: \{\} <br /> |
| `metric_mapping` _object (keys:string, values:float)_ | MetricMapping maps the text values of a MAPPEDMETRIC column to numbers. A value it does not list<br />drops that sample. | MaxProperties: 256 <br />Optional: \{\} <br /> |
| `metrics_mapping` _object (keys:string, values:float)_ | MetricsMapping is MetricMapping under the other spelling, the one CloudNativePG's documentation<br />uses. MetricMapping wins when both are set. | MaxProperties: 256 <br />Optional: \{\} <br /> |
| `name` _string_ | Name replaces the column name in the metric name: `cnpg_<query name>_<name>`. A DURATION metric<br />also ends in _milliseconds. | Optional: \{\} <br /> |
| `pg_version` _string_ | PgVersion is a semantic version range of PostgreSQL servers. On another version, the metric of a<br />value column is not exported, but the column must still be in the result. It has no effect on a<br />LABEL or DISCARD column. An invalid range is ignored with a warning, as if the field were empty.<br />When empty, the column applies to every version. | Optional: \{\} <br /> |

#### EnvFromSource

EnvFromSource names the objects that hold OTEL_* settings. Set at least one of configMap or secret.

_Appears in:_
- [TelemetryExporterSpec](#telemetryexporterspec)

| Field | Description | Validation |
| --- | --- | --- |
| `configMap` _[LocalObjectReference](#localobjectreference)_ | ConfigMap is a ConfigMap in this namespace. | Optional: \{\} <br /> |
| `secret` _[LocalObjectReference](#localobjectreference)_ | Secret is a Secret in this namespace. | Optional: \{\} <br /> |

#### Exclude

Exclude removes queries, metrics and databases from collection.

_Appears in:_
- [MetricsSpec](#metricsspec)

| Field | Description | Validation |
| --- | --- | --- |
| `queries` _string array_ | Queries lists query keys never planned, so their SQL never runs. A key may be a Go path.Match pattern. | MaxItems: 64 <br />Optional: \{\} <br /> |
| `metrics` _string array_ | Metrics lists full metric names to drop, such as cnpg_pg_stat_database_xact_commit. A name may be<br />a Go path.Match pattern. A query whose metrics are all dropped does not run. | MaxItems: 64 <br />Optional: \{\} <br /> |
| `databases` _string array_ | Databases lists database names to skip: no query runs in them. A name may be a Go path.Match pattern. | MaxItems: 64 <br />Optional: \{\} <br /> |

#### KeyReference

KeyReference points to a key of a Secret or of a ConfigMap. Set exactly one of secret or configMap.

_Appears in:_
- [OTLPTLS](#otlptls)

| Field | Description | Validation |
| --- | --- | --- |
| `secret` _[KeySelector](#keyselector)_ | Secret is a key of a Secret in this namespace. | Optional: \{\} <br /> |
| `configMap` _[KeySelector](#keyselector)_ | ConfigMap is a key of a ConfigMap in this namespace. | Optional: \{\} <br /> |

#### KeySelector

KeySelector names a key of an object in this namespace.

_Appears in:_
- [KeyReference](#keyreference)
- [OTLPExporter](#otlpexporter)

| Field | Description | Validation |
| --- | --- | --- |
| `name` _string_ | Name is the object's name. | MinLength: 1 <br /> |
| `key` _string_ | Key is the key in the object's data. | MinLength: 1 <br /> |

#### Limits

Limits bounds what the queries may cost. A limit left out keeps its default.

_Appears in:_
- [MetricsSpec](#metricsspec)

| Field | Description | Validation |
| --- | --- | --- |
| `maxConnections` _integer_ | MaxConnections is the most database connections the sidecar opens for queries. The default is 4.<br />Every 10 seconds it also opens one more short connection, outside this cap, to see whether<br />PostgreSQL is shutting down, so the monitoring role needs a connection limit of maxConnections + 1<br />or more. | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxRowsPerQuery` _integer_ | MaxRowsPerQuery is the most rows one query may return. A query with more is stopped and exports<br />nothing in that run. The default is 10000. | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxSeriesPerQuery` _integer_ | MaxSeriesPerQuery is the most series one query may produce in one database. A query with more<br />exports nothing in that run. The default is 10000. | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxSeriesTotal` _integer_ | MaxSeriesTotal is the most series all queries may produce together. A result that would pass it is<br />not stored, and that query stops exporting until a later result fits. The default is 50000. | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxDatabases` _integer_ | MaxDatabases is the most databases one query runs in. The databases it matches are taken in name<br />order and the rest are skipped. The default is 50. | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxLabelValueLength` _integer_ | MaxLabelValueLength is the longest label value, in bytes. A row with a longer value is dropped.<br />The default is 1024. | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxLabelsPerSeries` _integer_ | MaxLabelsPerSeries is the most label columns a query may have. A query with more is disabled.<br />The default is 32. | Minimum: 1 <br />Optional: \{\} <br /> |
| `queryTimeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | QueryTimeout is the time one query may run, set as statement_timeout. It must be at least 100ms and<br />at most 1073741823ms, so that twice it fits PostgreSQL's idle_in_transaction_session_timeout. The<br />default is 10s. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |
| `lockTimeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | LockTimeout is the time a query waits for a lock, set as lock_timeout. It must be more than zero and<br />at most 2147483647ms (PostgreSQL's lock_timeout maximum). The default is 1s. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |

#### LocalObjectReference

LocalObjectReference names an object in this namespace.

_Appears in:_
- [EnvFromSource](#envfromsource)
- [OTLPTLS](#otlptls)

| Field | Description | Validation |
| --- | --- | --- |
| `name` _string_ | Name is the object's name. | MinLength: 1 <br /> |

#### MetricColumn

_Underlying type:_ _object (keys:string, values:[ColumnSpec](#columnspec))_

MetricColumn describes one result column: a single entry whose key is the column name.
The name must match `^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`: it starts with a letter or underscore and has up to 63
letters, digits or underscores.

_Validation:_
- MaxProperties: 1
- MinProperties: 1

_Appears in:_
- [Query](#query)

#### MetricQuerySet

MetricQuerySet holds custom metric queries in the CloudNativePG format: the same field names and
meanings as the queries in a CloudNativePG monitoring ConfigMap, as a schema Kubernetes validates
when the resource is applied. A TelemetryExporter lists the sets it uses in spec.metrics.querySets.

| Field | Description | Validation |
| --- | --- | --- |
| `apiVersion` _string_ | `telemetry.gabrosys.github.io/v1alpha1` | |
| `kind` _string_ | `MetricQuerySet` | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |
| `spec` _[MetricQuerySetSpec](#metricquerysetspec)_ | Spec holds the queries. |  |

#### MetricQuerySetSpec

MetricQuerySetSpec holds the queries of a MetricQuerySet.

_Appears in:_
- [MetricQuerySet](#metricqueryset)

| Field | Description | Validation |
| --- | --- | --- |
| `queries` _object (keys:string, values:[Query](#query))_ | Queries maps a query key to its definition. A key starts with a letter or underscore and<br />continues with letters, digits and underscores, up to 63 characters. Unless the query sets name,<br />its metrics are named `cnpg_<key>_<column>`. A metric name must not start with cnpg_otel_. | MaxProperties: 128 <br />MinProperties: 1 <br /> |

#### MetricsSpec

MetricsSpec holds the metrics settings.

_Appears in:_
- [TelemetryExporterSpec](#telemetryexporterspec)

| Field | Description | Validation |
| --- | --- | --- |
| `interval` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | Interval is how often metrics are exported, such as 30s. It must be at least 1s. The default is 30s. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |
| `maxPointsPerRequest` _integer_ | MaxPointsPerRequest caps the data points in one export request. An export with more points is<br />split into several requests. The default is 2000. | Maximum: 100000 <br />Minimum: 1 <br />Optional: \{\} <br /> |
| `useClusterMonitoring` _boolean_ | UseClusterMonitoring loads the queries listed in the Cluster's spec.monitoring.customQueriesConfigMap<br />and spec.monitoring.customQueriesSecret, as CloudNativePG does. The default is true. The plugin's<br />own built-in queries load either way. | Optional: \{\} <br /> |
| `querySets` _string array_ | QuerySets lists the MetricQuerySet names in this namespace to load, after the Cluster's monitoring<br />queries. When a query key appears twice, the later source wins. | MaxItems: 16 <br />items:MaxLength: 253 <br />items:Pattern: `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$` <br />Optional: \{\} <br /> |
| `planInterval` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | PlanInterval is how often the sidecar reads the server version, the databases and the settings it<br />checks from PostgreSQL. It plans the queries again only when the version, the primary or replica<br />role or the list of databases changed. Changes to the query sources apply when they happen, not<br />on this timer. At least 1s. The default is 30s. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |
| `defaultInterval` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | DefaultInterval is how often a query runs when no group matches it. At least 1s. When empty, the<br />Cluster's spec.monitoring.metricsQueriesTTL is used if it is at least 1s, else 30s. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |
| `groups` _[QueryGroup](#querygroup) array_ | Groups run sets of queries at their own interval. A query that no group matches runs at<br />defaultInterval. | MaxItems: 32 <br />Optional: \{\} <br /> |
| `limits` _[Limits](#limits)_ | Limits bounds what the queries may cost. A limit left out keeps its default. | Optional: \{\} <br /> |
| `exclude` _[Exclude](#exclude)_ | Exclude removes queries, metrics and databases from collection. It is empty by default. | Optional: \{\} <br /> |

#### OTLPExporter

OTLPExporter is the OTLP connection.

_Appears in:_
- [TelemetryExporterSpec](#telemetryexporterspec)

| Field | Description | Validation |
| --- | --- | --- |
| `protocol` _string_ | Protocol is the OTLP transport: http/protobuf or grpc. When empty, the PROTOCOL key of envFrom<br />is used, else http/protobuf. | Enum: [http/protobuf grpc] <br />Optional: \{\} <br /> |
| `endpoint` _string_ | Endpoint is the URL of the collector, such as https://collector:4318. An http:// endpoint is<br />plaintext and an https:// one uses TLS. For http/protobuf, v1/metrics is appended to it. It has a<br />host, and no user info, query or fragment. When empty, the ENDPOINT key of envFrom is used. | MaxLength: 2048 <br />Optional: \{\} <br /> |
| `compression` _string_ | Compression is gzip or none. When empty, the COMPRESSION key of envFrom is used, else gzip. | Enum: [gzip none] <br />Optional: \{\} <br /> |
| `timeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | Timeout is the time one export attempt may take, such as 10s. It must be at least 1s and at most<br />half of spec.metrics.interval, which is 30s when not set. When empty, the TIMEOUT key of envFrom is used,<br />else the lower of 10s and half the interval. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |
| `headers` _[KeySelector](#keyselector)_ | Headers is a key of a Secret in this namespace that holds request headers in the<br />OTEL_EXPORTER_OTLP_HEADERS format: name=value pairs separated by commas, values percent-encoded.<br />When empty, the HEADERS key of the envFrom Secret is used. | Optional: \{\} <br /> |
| `tls` _[OTLPTLS](#otlptls)_ | TLS sets the TLS options. It needs an https:// endpoint. When empty, the system roots verify the<br />collector, no client certificate is sent and TLS 1.2 is the lowest version. | Optional: \{\} <br /> |

#### OTLPTLS

OTLPTLS sets the TLS options of the OTLP connection.

_Appears in:_
- [OTLPExporter](#otlpexporter)

| Field | Description | Validation |
| --- | --- | --- |
| `ca` _[KeyReference](#keyreference)_ | CA is a PEM bundle of the certificate authorities that sign the collector's certificate. It is<br />added to the system roots. | Optional: \{\} <br /> |
| `clientCertificate` _[LocalObjectReference](#localobjectreference)_ | ClientCertificate is a Secret in this namespace that holds the client certificate in tls.crt and<br />its key in tls.key, for mutual TLS. | Optional: \{\} <br /> |
| `serverName` _string_ | ServerName is the name to check in the collector's certificate. When empty, the host of the<br />endpoint is used. | Optional: \{\} <br /> |
| `minVersion` _string_ | MinVersion is the lowest TLS version to accept: 1.2 or 1.3. The default is 1.2. | Enum: [1.2 1.3] <br />Optional: \{\} <br /> |

#### Query

Query is one query and the metrics its result columns produce. Field names and meanings are those
of a CloudNativePG custom query.

_Appears in:_
- [MetricQuerySetSpec](#metricquerysetspec)

| Field | Description | Validation |
| --- | --- | --- |
| `query` _string_ | Query is the SQL to run. It runs in a read-only transaction that sets the sidecar's limits<br />(statement_timeout, lock_timeout, idle_in_transaction_session_timeout, jit,<br />max_parallel_workers_per_gather), unless Query or PredicateQuery names pg_settings or<br />pg_show_all_settings, bare or as pg_catalog.pg_settings or pg_catalog.pg_show_all_settings,<br />outside comments and string constants. Such a query reports the server's values and is bounded<br />by the run deadline instead. A query that reaches pg_settings only through a view or a function<br />reports the sidecar's limits. |  |
| `predicate_query` _string_ | PredicateQuery is SQL that runs first, in the same transaction. Query runs only when the first<br />column of the first row is true. | Optional: \{\} <br /> |
| `metrics` _[MetricColumn](#metriccolumn) array_ | Metrics lists the result columns, one single-key item per column: the key is the column name<br />and the value says how to use it. The result must have as many columns as the list has, and a<br />HISTOGRAM column also needs its _bucket, _sum and _count columns in the result. | MaxItems: 128 <br />MaxProperties: 1 <br />MinItems: 1 <br />MinProperties: 1 <br /> |
| `master` _boolean_ | Master is the old name of Primary: the query runs on the primary only and is skipped on a replica.<br />The default is false. | Optional: \{\} <br /> |
| `primary` _boolean_ | Primary runs the query on the primary only, and skips it on a replica. The default is false. | Optional: \{\} <br /> |
| `cache_seconds` _integer_ | CacheSeconds is the minimum number of seconds between two runs of the query. A shorter interval is<br />raised to this value. The default is 0, no minimum. | Minimum: 0 <br />Optional: \{\} <br /> |
| `runonserver` _string_ | RunOnServer is a semantic version range of PostgreSQL servers the query runs on, such as<br />">=15.0.0". A query with an invalid range is disabled. When empty, it runs on every version. | Optional: \{\} <br /> |
| `target_databases` _string array_ | TargetDatabases lists the databases to run the query in. A name with *, ? or [ is a Go path.Match<br />pattern matched against the databases that accept connections, are not templates, let the<br />monitoring role CONNECT and have valid UTF-8 names. When empty, the query runs in the<br />Cluster's application database, which is postgres when the bootstrap sets none. | MaxItems: 64 <br />Optional: \{\} <br /> |
| `name` _string_ | Name replaces the query key in the metric names: `cnpg_<name>_<column>`. | Optional: \{\} <br /> |

#### QueryGroup

QueryGroup runs a set of queries at its own interval.

_Appears in:_
- [MetricsSpec](#metricsspec)

| Field | Description | Validation |
| --- | --- | --- |
| `name` _string_ | Name identifies the group. It is unique in the list. | MinLength: 1 <br /> |
| `interval` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#duration-v1-meta)_ | Interval is how often the group's queries run. At least 1s. The default is spec.metrics.defaultInterval.<br />A query whose cache_seconds is higher runs at that interval instead. | MaxLength: 32 <br />Type: string <br />Optional: \{\} <br /> |
| `queries` _string array_ | Queries lists query keys or Go path.Match patterns, such as pg_stat_*. A key is the name under<br />which the query is defined, not its name field. A query that several groups match uses the first<br />of them in the list. | MaxItems: 64 <br />MinItems: 1 <br /> |

#### TelemetryExporter

TelemetryExporter says where and how the plugin sidecar exports telemetry for the Clusters in its
namespace. One TelemetryExporter can serve every Cluster in the namespace; a Cluster picks it with the
plugin parameter "exporter", and uses the one named default when the parameter is not set.

| Field | Description | Validation |
| --- | --- | --- |
| `apiVersion` _string_ | `telemetry.gabrosys.github.io/v1alpha1` | |
| `kind` _string_ | `TelemetryExporter` | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |
| `spec` _[TelemetryExporterSpec](#telemetryexporterspec)_ | Spec holds the exporter settings. The endpoint is required: set spec.exporter.endpoint, or put<br />OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT in envFrom. |  |

#### TelemetryExporterSpec

TelemetryExporterSpec holds the OTLP connection, the resource attributes and the metrics settings.

_Appears in:_
- [TelemetryExporter](#telemetryexporter)

| Field | Description | Validation |
| --- | --- | --- |
| `exporter` _[OTLPExporter](#otlpexporter)_ | Exporter is the OTLP connection. | Optional: \{\} <br /> |
| `envFrom` _[EnvFromSource](#envfromsource)_ | EnvFrom names a ConfigMap and a Secret whose OpenTelemetry environment keys are read as settings.<br />The sidecar reads the ENDPOINT, PROTOCOL, COMPRESSION, TIMEOUT (in milliseconds) and HEADERS keys<br />of OTEL_EXPORTER_OTLP_, where `OTEL_EXPORTER_OTLP_METRICS_<NAME>` wins over `OTEL_EXPORTER_OTLP_<NAME>`,<br />and OTEL_RESOURCE_ATTRIBUTES. A Secret key wins over the same ConfigMap key, and a field of<br />spec.exporter wins over both. OTEL_EXPORTER_OTLP_METRICS_ENDPOINT is used as given.<br />OTEL_EXPORTER_OTLP_ENDPOINT gets v1/metrics appended when the protocol is http/protobuf. HEADERS<br />and METRICS_HEADERS must be in the Secret, and the key in the ConfigMap is rejected. The keys<br />CERTIFICATE, CLIENT_KEY and CLIENT_CERTIFICATE are rejected: use spec.exporter.tls.<br />OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE other than<br />cumulative and OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION other than<br />explicit_bucket_histogram are rejected. INSECURE is ignored with a warning, because the endpoint<br />scheme selects TLS. OTEL_SERVICE_NAME is ignored with a warning, because service.name is the<br />Cluster name. | Optional: \{\} <br /> |
| `resourceLabels` _string array_ | ResourceLabels lists Cluster label keys to copy into the resource attributes, each as<br />`cnpg.cluster.label.<key>`. A label the Cluster does not have is skipped. Such an attribute wins<br />over the same key in resourceAttributes. The instance role label (cnpg.io/instanceRole) is not<br />allowed, because it changes at switchover. | MaxItems: 32 <br />items:MaxLength: 317 <br />items:Pattern: `^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$` <br />Optional: \{\} <br /> |
| `resourceAttributes` _object (keys:string, values:string)_ | ResourceAttributes adds fixed attributes to the resource of every export. They win over<br />OTEL_RESOURCE_ATTRIBUTES from envFrom. The sidecar sets service.name, service.namespace,<br />service.instance.id and the k8s.namespace.name, k8s.pod.name, k8s.pod.uid, k8s.node.name and<br />k8s.container.name attributes itself, and an entry with one of these keys is dropped. | MaxProperties: 32 <br />Optional: \{\} <br /> |
| `metrics` _[MetricsSpec](#metricsspec)_ | Metrics holds the metrics settings. | Optional: \{\} <br /> |
