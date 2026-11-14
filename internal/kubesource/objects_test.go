package kubesource_test

import (
	"encoding/base64"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func object(apiVersion, kind, name string, fields map[string]any) *unstructured.Unstructured {
	o := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": "ns"},
	}
	for k, v := range fields {
		o[k] = v
	}
	return &unstructured.Unstructured{Object: o}
}

type clusterOpts struct {
	plugins     []any
	monitoring  map[string]any
	status      map[string]any
	annotations map[string]string
}

func pluginEntry(params map[string]any, extra map[string]any) map[string]any {
	e := map[string]any{"name": "telemetry.gabrosys.github.io", "parameters": params}
	for k, v := range extra {
		e[k] = v
	}
	return e
}

func cluster(o clusterOpts) *unstructured.Unstructured {
	spec := map[string]any{}
	if o.plugins != nil {
		spec["plugins"] = o.plugins
	}
	if o.monitoring != nil {
		spec["monitoring"] = o.monitoring
	}
	c := object("postgresql.cnpg.io/v1", "Cluster", "pg", map[string]any{"spec": spec})
	if o.status != nil {
		c.Object["status"] = o.status
	}
	if o.annotations != nil {
		c.SetAnnotations(o.annotations)
	}
	return c
}

func enabledCluster(params map[string]any) *unstructured.Unstructured {
	return cluster(clusterOpts{plugins: []any{pluginEntry(params, nil)}})
}

func exporter(spec map[string]any) *unstructured.Unstructured {
	if spec == nil {
		spec = map[string]any{"exporter": map[string]any{"endpoint": "http://c:4318"}}
	}
	return object("telemetry.gabrosys.github.io/v1alpha1", "TelemetryExporter", "default", map[string]any{"spec": spec})
}

func configMap(name string, data map[string]any) *unstructured.Unstructured {
	return object("v1", "ConfigMap", name, map[string]any{"data": data})
}

func secret(name string, data map[string]string) *unstructured.Unstructured {
	enc := map[string]any{}
	for k, v := range data {
		enc[k] = base64.StdEncoding.EncodeToString([]byte(v))
	}
	return object("v1", "Secret", name, map[string]any{"data": enc})
}

func querySet(name string, queries map[string]any) *unstructured.Unstructured {
	return object("telemetry.gabrosys.github.io/v1alpha1", "MetricQuerySet", name,
		map[string]any{"spec": map[string]any{"queries": queries}})
}
