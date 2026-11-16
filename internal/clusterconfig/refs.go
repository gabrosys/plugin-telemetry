package clusterconfig

import (
	"slices"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

// ConfigMapFile is the virtual file name of a ConfigMap key.
func ConfigMapFile(name, key string) string { return "configmap/" + name + "/" + key }

// SecretFile is the virtual file name of a Secret key.
func SecretFile(name, key string) string { return "secret/" + name + "/" + key }

// QuerySetFile is the virtual file name of a MetricQuerySet.
func QuerySetFile(name string) string { return "metricqueryset/" + name }

// Refs are the objects a Cluster's sidecar reads: what it watches, and what its Role grants.
type Refs struct {
	Cluster, Exporter              string
	ConfigMaps, Secrets, QuerySets []string // sorted, unique, never empty strings
}

// References lists them. e is nil when the exporter is missing: then Refs holds only the Cluster,
// the exporter name, the spec.monitoring objects and, in password mode, the password Secret.
func References(c *apiv1.Cluster, p podpatch.Params, e *v1alpha1.TelemetryExporter) Refs {
	r := Refs{Cluster: c.Name, Exporter: p.Exporter}
	if r.Exporter == "" {
		r.Exporter = podpatch.DefaultExporter
	}

	var configMaps, secrets, querySets []string
	if monitoringEnabled(monitoringFlag(e)) && c.Spec.Monitoring != nil {
		for _, ref := range c.Spec.Monitoring.CustomQueriesConfigMap {
			configMaps = append(configMaps, ref.Name)
		}
		for _, ref := range c.Spec.Monitoring.CustomQueriesSecret {
			secrets = append(secrets, ref.Name)
		}
	}
	if p.Auth == podpatch.AuthPassword {
		secrets = append(secrets, p.PasswordSecret)
	}

	if e != nil {
		querySets = append(querySets, e.Spec.Metrics.QuerySets...)
		x := e.Spec.Exporter
		if x.Headers != nil {
			secrets = append(secrets, x.Headers.Name)
		}
		if x.TLS != nil {
			if ca := x.TLS.CA; ca != nil {
				if ca.Secret != nil {
					secrets = append(secrets, ca.Secret.Name)
				}
				if ca.ConfigMap != nil {
					configMaps = append(configMaps, ca.ConfigMap.Name)
				}
			}
			if x.TLS.ClientCertificate != nil {
				secrets = append(secrets, x.TLS.ClientCertificate.Name)
			}
		}
		if env := e.Spec.EnvFrom; env != nil {
			if env.ConfigMap != nil {
				configMaps = append(configMaps, env.ConfigMap.Name)
			}
			if env.Secret != nil {
				secrets = append(secrets, env.Secret.Name)
			}
		}
	}

	r.ConfigMaps, r.Secrets, r.QuerySets = names(configMaps), names(secrets), names(querySets)
	return r
}

// monitoringEnabled reports whether the spec.monitoring queries are read; the default is true.
func monitoringEnabled(useClusterMonitoring *bool) bool {
	return useClusterMonitoring == nil || *useClusterMonitoring
}

func monitoringFlag(e *v1alpha1.TelemetryExporter) *bool {
	if e == nil {
		return nil
	}
	return e.Spec.Metrics.UseClusterMonitoring
}

// names sorts and deduplicates in, dropping empty names.
func names(in []string) []string {
	out := slices.DeleteFunc(slices.Clone(in), func(s string) bool { return s == "" })
	slices.Sort(out)
	return slices.Compact(out)
}
