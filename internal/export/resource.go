package export

import (
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const containerName = "otel-metrics"

// Resource builds design 6.6's resource from settings and the downward-API env
// (POD_NAME, POD_UID, NODE_NAME, NAMESPACE); fixed attributes win over ResourceAttributes.
func Resource(s settings.Settings, getenv func(string) string) *resource.Resource {
	values := make(map[attribute.Key]string, len(s.ResourceAttributes)+8)
	for k, v := range s.ResourceAttributes {
		values[attribute.Key(k)] = v
	}

	pod := getenv("POD_NAME")
	instance := pod
	if instance == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		instance = host
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
	if pod != "" {
		values[semconv.K8SContainerNameKey] = containerName
	}

	kvs := make([]attribute.KeyValue, 0, len(values))
	for k, v := range values {
		kvs = append(kvs, attribute.String(string(k), v))
	}

	return resource.NewWithAttributes(semconv.SchemaURL, kvs...)
}
