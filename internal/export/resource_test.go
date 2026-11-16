package export_test

import (
	"errors"

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

var errHost = errors.New("no host name")

var _ = Describe("Resource", func() {
	base := settings.Settings{Cluster: "pg", Namespace: "db"}

	DescribeTable("Instance",
		func(env map[string]string, host func() (string, error), want string, wantErr bool) {
			got, err := export.Instance(envOf(env), host)
			Expect(got).To(Equal(want))
			if wantErr {
				Expect(err).To(MatchError(errHost))
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		},
		Entry("the Pod", map[string]string{"POD_NAME": "pg-1"}, func() (string, error) { return "host", nil }, "pg-1", false),
		Entry("the host name", nil, func() (string, error) { return "host", nil }, "host", false),
		Entry("unknown, with the error", nil, func() (string, error) { return "", errHost }, "unknown", true),
	)

	It("sets service.name, service.namespace, service.instance.id <ns>.<pod>.otel-metrics and the k8s.* attributes", func() {
		r := export.Resource(base, envOf(map[string]string{
			"POD_NAME": "pg-1", "POD_UID": "uid-1", "NODE_NAME": "node-a", "NAMESPACE": "db-ns",
			"OTEL_SERVICE_NAME": "ignored", "OTEL_RESOURCE_ATTRIBUTES": "x=y",
		}), "pg-1")

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
		r := export.Resource(base, envOf(nil), "host-1")

		got := attrs(r.Attributes()...)
		Expect(got).To(HaveLen(3))
		Expect(got).To(HaveKeyWithValue("service.name", "pg"))
		Expect(got).To(HaveKeyWithValue("service.instance.id", "db.host-1.otel-metrics"))
	})

	It("lets ResourceAttributes add attributes but not replace fixed ones", func() {
		s := base
		s.ResourceAttributes = map[string]string{"deployment.environment": "prod", "service.name": "evil", "k8s.pod.name": "evil"}
		r := export.Resource(s, envOf(map[string]string{"POD_NAME": "pg-1"}), "pg-1")

		got := attrs(r.Attributes()...)
		Expect(got).To(HaveKeyWithValue("deployment.environment", "prod"))
		Expect(got).To(HaveKeyWithValue("service.name", "pg"))
		Expect(got).To(HaveKeyWithValue("k8s.pod.name", "pg-1"))
	})

	It("takes no service.* or k8s.* key from ResourceAttributes when the env sets none", func() {
		s := base
		s.ResourceAttributes = map[string]string{"k8s.pod.name": "x", "service.name": "y"}
		r := export.Resource(s, envOf(nil), "host-1")

		got := attrs(r.Attributes()...)
		Expect(got).NotTo(HaveKey("k8s.pod.name"))
		Expect(got).To(HaveKeyWithValue("service.name", "pg"))
	})
})
