package export

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

// containerName is the sidecar container, whose name is k8s.container.name.
const containerName = "otel-metrics"

// Instance names the process instance: POD_NAME, else the host name, else "unknown" with the host name error.
func Instance(getenv func(string) string, hostname func() (string, error)) (string, error) {
	if pod := getenv("POD_NAME"); pod != "" {
		return pod, nil
	}
	host, err := hostname()
	if err != nil {
		return "unknown", fmt.Errorf("read host name: %w", err)
	}

	return host, nil
}

// fixedKeys are the resource attributes Resource sets itself; ResourceAttributes never supplies them.
var fixedKeys = []attribute.Key{
	semconv.ServiceNameKey, semconv.ServiceNamespaceKey, semconv.ServiceInstanceIDKey,
	semconv.K8SNamespaceNameKey, semconv.K8SPodNameKey, semconv.K8SPodUIDKey, semconv.K8SNodeNameKey,
	semconv.K8SContainerNameKey,
}

// Resource builds the exported resource: service.name is the cluster, service.namespace the namespace,
// service.instance.id "<namespace>.<instance>.otel-metrics" (see Instance), and k8s.namespace.name,
// k8s.pod.name, k8s.pod.uid and k8s.node.name come from NAMESPACE, POD_NAME, POD_UID and NODE_NAME when
// set, with k8s.container.name when POD_NAME is set. ResourceAttributes add the other keys and never
// supply a fixed key.
func Resource(s settings.Settings, getenv func(string) string, instance string) *resource.Resource {
	values := make(map[attribute.Key]string, len(s.ResourceAttributes)+8)
	for k, v := range s.ResourceAttributes {
		values[attribute.Key(k)] = v
	}
	for _, k := range fixedKeys {
		delete(values, k)
	}

	values[semconv.ServiceNameKey] = s.Cluster
	values[semconv.ServiceNamespaceKey] = s.Namespace
	values[semconv.ServiceInstanceIDKey] = s.Namespace + "." + instance + "." + containerName
	for key, env := range map[attribute.Key]string{
		semconv.K8SNamespaceNameKey: "NAMESPACE",
		semconv.K8SPodNameKey:       "POD_NAME",
		semconv.K8SPodUIDKey:        "POD_UID",
		semconv.K8SNodeNameKey:      "NODE_NAME",
	} {
		if v := getenv(env); v != "" {
			values[key] = v
		}
	}
	if getenv("POD_NAME") != "" {
		values[semconv.K8SContainerNameKey] = containerName
	}

	kvs := make([]attribute.KeyValue, 0, len(values))
	for k, v := range values {
		kvs = append(kvs, attribute.String(string(k), v))
	}

	return resource.NewWithAttributes(semconv.SchemaURL, kvs...)
}
