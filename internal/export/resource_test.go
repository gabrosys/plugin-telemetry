package export_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func attrs(kvs ...attribute.KeyValue) map[string]string {
	out := map[string]string{}
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value.AsString()
	}

	return out
}

var _ = Describe("Resource", func() {
	base := settings.Settings{Cluster: "pg", Namespace: "db"}

	It("sets service.name, service.namespace, service.instance.id <ns>.<pod>.otel-metrics and the k8s.* attributes", func() {
		r := export.Resource(base, envOf(map[string]string{
			"POD_NAME": "pg-1", "POD_UID": "uid-1", "NODE_NAME": "node-a", "NAMESPACE": "db-ns",
			"OTEL_SERVICE_NAME": "ignored", "OTEL_RESOURCE_ATTRIBUTES": "x=y",
		}))

		Expect(r.SchemaURL()).To(Equal(semconv.SchemaURL))
		Expect(attrs(r.Attributes()...)).To(Equal(map[string]string{
			"service.name":        "pg",
			"service.namespace":   "db",
			"service.instance.id": "db.pg-1.otel-metrics",
			"k8s.namespace.name":  "db-ns",
			"k8s.pod.name":        "pg-1",
			"k8s.pod.uid":         "uid-1",
			"k8s.node.name":       "node-a",
			"k8s.container.name":  "otel-metrics",
		}))
	})

	It("uses the hostname for service.instance.id and omits k8s.* when POD_NAME is unset", func() {
		r := export.Resource(base, envOf(nil))

		got := attrs(r.Attributes()...)
		Expect(got).To(HaveLen(3))
		Expect(got).To(HaveKeyWithValue("service.name", "pg"))
		Expect(got["service.instance.id"]).To(MatchRegexp(`^db\..+\.otel-metrics$`))
	})

	It("lets ResourceAttributes add attributes but not replace fixed ones", func() {
		s := base
		s.ResourceAttributes = map[string]string{"deployment.environment": "prod", "service.name": "evil", "k8s.pod.name": "evil"}
		r := export.Resource(s, envOf(map[string]string{"POD_NAME": "pg-1"}))

		got := attrs(r.Attributes()...)
		Expect(got).To(HaveKeyWithValue("deployment.environment", "prod"))
		Expect(got).To(HaveKeyWithValue("service.name", "pg"))
		Expect(got).To(HaveKeyWithValue("k8s.pod.name", "pg-1"))
	})
})
