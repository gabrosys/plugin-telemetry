package clusterconfig

import (
	"errors"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const (
	instanceRoleLabel = "cnpg.io/instanceRole"
	labelAttrPrefix   = "cnpg.cluster.label."

	passwordHost    = "127.0.0.1"
	passwordKey     = "password"
	clientCertKey   = "tls.crt"
	clientKeyKey    = "tls.key"
	defaultDatabase = "postgres"
)

// PasswordUsernameKey is the key of the passwordSecret that holds the user name.
const PasswordUsernameKey = "username"

// Inputs are what the settings are built from; the sidecar fills them from its watches.
type Inputs struct {
	Cluster          *apiv1.Cluster
	Params           podpatch.Params
	Exporter         *v1alpha1.TelemetryExporter
	EnvConfigMap     map[string]string // data of envFrom.configMap; nil when not set or missing
	EnvSecret        map[string]string // data of envFrom.secret
	PasswordUsername string            // the passwordSecret's username key, password mode only
}

// Build returns validated settings whose file fields hold virtual names, plus warnings.
// An error never contains a value read from a Secret.
func Build(in Inputs) (settings.Settings, []string, error) {
	if in.Cluster == nil {
		return settings.Settings{}, nil, errors.New("build settings: no Cluster")
	}
	if in.Exporter == nil {
		return settings.Settings{}, nil, errors.New("build settings: no TelemetryExporter")
	}

	s := settings.Defaults()
	s.Cluster = in.Cluster.Name
	s.Namespace = in.Cluster.Namespace
	s.Postgres.Database = applicationDatabase(in.Cluster)

	var errs []error
	if err := applyAuth(&s, in); err != nil {
		errs = append(errs, err)
	}

	spec := in.Exporter.Spec
	warnings := applyMetrics(&s, in.Cluster, spec.Metrics)
	applyExporter(&s.Exporter, spec.Exporter)

	env := otlpEnv{configMap: in.EnvConfigMap, secret: in.EnvSecret}
	if spec.EnvFrom != nil && spec.EnvFrom.Secret != nil {
		env.secretName = spec.EnvFrom.Secret.Name
	}
	envWarnings, envErrs := env.apply(&s.Exporter, spec.Exporter)
	warnings = append(warnings, envWarnings...)
	errs = append(errs, envErrs...)
	errs = append(errs, env.unsupported()...)
	warnings = append(warnings, env.ignored()...)
	if spec.Exporter.Headers == nil {
		s.Exporter.HeadersFile = env.headersFile()
	}
	if s.Exporter.Protocol == settings.ProtocolHTTP && spec.Exporter.Endpoint != "" {
		s.Exporter.Endpoint = SignalURL(spec.Exporter.Endpoint)
	}
	if s.Exporter.Timeout == 0 {
		s.Exporter.Timeout = settings.DefaultExporterTimeout(s.Exporter.Interval)
	}

	attrs, attrWarnings := resourceAttributes(in, env)
	s.ResourceAttributes = attrs
	warnings = append(warnings, attrWarnings...)

	if len(errs) > 0 {
		return settings.Settings{}, nil, fmt.Errorf("build settings: %w", withCRDFields(errors.Join(errs...)))
	}
	if err := s.Validate(); err != nil {
		return settings.Settings{}, nil, fmt.Errorf("invalid settings: %w", withCRDFields(err))
	}
	return s, warnings, nil
}

// applicationDatabase returns the first non-empty database of spec.bootstrap.recovery, pg_basebackup and
// initdb, else "postgres". Ported from CloudNativePG v1.30.1 api/v1/cluster_funcs.go:594-613 and
// internal/management/controller/instance_controller.go:1044-1047.
func applicationDatabase(c *apiv1.Cluster) string {
	if b := c.Spec.Bootstrap; b != nil {
		switch {
		case b.Recovery != nil && b.Recovery.Database != "":
			return b.Recovery.Database
		case b.PgBaseBackup != nil && b.PgBaseBackup.Database != "":
			return b.PgBaseBackup.Database
		case b.InitDB != nil && b.InitDB.Database != "":
			return b.InitDB.Database
		}
	}
	return defaultDatabase
}

func applyAuth(s *settings.Settings, in Inputs) error {
	if in.Params.Auth != podpatch.AuthPassword {
		return nil
	}
	if in.Params.PasswordSecret == "" {
		return errors.New("password mode needs the passwordSecret parameter")
	}
	if in.PasswordUsername == "" {
		return fmt.Errorf("password Secret %q: key %q is empty", in.Params.PasswordSecret, PasswordUsernameKey)
	}
	s.Postgres.Host = passwordHost
	s.Postgres.User = in.PasswordUsername
	s.Postgres.PasswordFile = SecretFile(in.Params.PasswordSecret, passwordKey)
	return nil
}

func applyMetrics(s *settings.Settings, c *apiv1.Cluster, m v1alpha1.MetricsSpec) (warnings []string) {
	if m.Interval != nil {
		s.Exporter.Interval = m.Interval.Duration
	}
	if m.MaxPointsPerRequest != nil {
		s.Exporter.MaxPointsPerRequest = int(*m.MaxPointsPerRequest)
	}
	if m.PlanInterval != nil {
		s.PlanInterval = m.PlanInterval.Duration
	}
	switch {
	case m.DefaultInterval != nil:
		s.DefaultInterval = m.DefaultInterval.Duration
	case c.Spec.Monitoring != nil && c.Spec.Monitoring.MetricsQueriesTTL != nil:
		if ttl := c.Spec.Monitoring.MetricsQueriesTTL.Duration; ttl >= time.Second {
			s.DefaultInterval = ttl
		} else {
			warnings = append(warnings, fmt.Sprintf(
				"spec.monitoring.metricsQueriesTTL: below 1s, using the default interval of %s", s.DefaultInterval))
		}
	}

	for _, g := range m.Groups {
		group := settings.Group{Name: g.Name, Interval: s.DefaultInterval, Queries: g.Queries}
		if g.Interval != nil {
			group.Interval = g.Interval.Duration
		}
		s.Groups = append(s.Groups, group)
	}

	l := m.Limits
	setInt(&s.Postgres.MaxConnections, l.MaxConnections)
	setInt(&s.Limits.MaxRowsPerQuery, l.MaxRowsPerQuery)
	setInt(&s.Limits.MaxSeriesPerQuery, l.MaxSeriesPerQuery)
	setInt(&s.Limits.MaxSeriesTotal, l.MaxSeriesTotal)
	setInt(&s.Limits.MaxDatabases, l.MaxDatabases)
	setInt(&s.Limits.MaxLabelValueLength, l.MaxLabelValueLength)
	setInt(&s.Limits.MaxLabelsPerSeries, l.MaxLabelsPerSeries)
	if l.QueryTimeout != nil {
		s.Limits.QueryTimeout = l.QueryTimeout.Duration
	}
	if l.LockTimeout != nil {
		s.Limits.LockTimeout = l.LockTimeout.Duration
	}
	s.Exclude = settings.Exclude{Queries: m.Exclude.Queries, Metrics: m.Exclude.Metrics, Databases: m.Exclude.Databases}

	if monitoringEnabled(m.UseClusterMonitoring) && c.Spec.Monitoring != nil {
		for _, ref := range c.Spec.Monitoring.CustomQueriesConfigMap {
			s.Sources = append(s.Sources, ConfigMapFile(ref.Name, ref.Key))
		}
		for _, ref := range c.Spec.Monitoring.CustomQueriesSecret {
			s.Sources = append(s.Sources, SecretFile(ref.Name, ref.Key))
		}
	}
	for _, name := range m.QuerySets {
		s.Sources = append(s.Sources, QuerySetFile(name))
	}
	return warnings
}

func setInt(dst *int, v *int32) {
	if v != nil {
		*dst = int(*v)
	}
}

// applyExporter sets the fields spec.exporter names. The endpoint is completed once the protocol is final.
func applyExporter(e *settings.Exporter, x v1alpha1.OTLPExporter) {
	if x.Protocol != "" {
		e.Protocol = x.Protocol
	}
	if x.Endpoint != "" {
		e.Endpoint = x.Endpoint
	}
	if x.Compression != "" {
		e.Compression = x.Compression
	}
	if x.Timeout != nil {
		e.Timeout = x.Timeout.Duration
	}
	if x.Headers != nil {
		e.HeadersFile = SecretFile(x.Headers.Name, x.Headers.Key)
	}
	if x.TLS == nil {
		return
	}
	if ca := x.TLS.CA; ca != nil {
		switch {
		case ca.Secret != nil:
			e.TLS.CAFile = SecretFile(ca.Secret.Name, ca.Secret.Key)
		case ca.ConfigMap != nil:
			e.TLS.CAFile = ConfigMapFile(ca.ConfigMap.Name, ca.ConfigMap.Key)
		}
	}
	if cc := x.TLS.ClientCertificate; cc != nil {
		e.TLS.CertFile = SecretFile(cc.Name, clientCertKey)
		e.TLS.KeyFile = SecretFile(cc.Name, clientKeyKey)
	}
	if x.TLS.ServerName != "" {
		e.TLS.ServerName = x.TLS.ServerName
	}
	if x.TLS.MinVersion != "" {
		e.TLS.MinVersion = x.TLS.MinVersion
	}
}

// resourceAttributes merges OTEL_RESOURCE_ATTRIBUTES, spec.resourceAttributes and the allowlisted
// Cluster labels, later ones winning. It returns nil when there are none.
func resourceAttributes(in Inputs, env otlpEnv) (map[string]string, []string) {
	var warnings []string
	out := map[string]string{}

	if v, ok := env.get(envResourceAttrs); ok {
		parsed, err := ParseResourceAttributes(v.value)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: ignored, %v", envResourceAttrs, err))
		}
		for k, val := range parsed {
			out[k] = val
		}
	}
	for k, v := range in.Exporter.Spec.ResourceAttributes {
		out[k] = v
	}
	for _, key := range in.Exporter.Spec.ResourceLabels {
		if key == instanceRoleLabel {
			warnings = append(warnings, "spec.resourceLabels: "+instanceRoleLabel+" is ignored, it changes at switchover")
			continue
		}
		if v, ok := in.Cluster.Labels[key]; ok {
			out[labelAttrPrefix+key] = v
		}
	}

	if len(out) == 0 {
		return nil, warnings
	}
	return out, warnings
}
