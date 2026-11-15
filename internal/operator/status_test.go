package operator_test

import (
	"context"
	"encoding/json"
	"sort"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	cnpgoperator "github.com/cloudnative-pg/cnpg-i/pkg/operator"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

var _ = Describe("ClusterStatus", func() {
	It("is healthy only when the exporter is ok and the identity ready", func() {
		Expect(operator.ClusterStatus{ExporterState: "ok", Identity: "ready"}.Healthy()).To(BeTrue())
		Expect(operator.ClusterStatus{ExporterState: "ok", Identity: "pending"}.Healthy()).To(BeFalse())
		Expect(operator.ClusterStatus{ExporterState: "missing", Identity: "ready"}.Healthy()).To(BeFalse())
		Expect(operator.ClusterStatus{ExporterState: "invalid", Identity: "ready"}.Healthy()).To(BeFalse())
	})
})

var _ = Describe("Status.SetStatusInCluster", func() {
	const goodIdent = "cnpg_metrics_exporter nobody cnpg_metrics_exporter"

	var (
		ctx     context.Context
		objs    []client.Object
		funcs   interceptor.Funcs
		cluster *apiv1.Cluster
	)

	// identityObjects returns the four identity objects as the plugin applies them, the token Secret with or
	// without data.
	identityObjects := func(token bool) []client.Object {
		want, err := identity.Desired(newCluster("ns", "pg"), clusterconfig.Refs{Cluster: "pg"})
		Expect(err).NotTo(HaveOccurred())
		if token {
			want.Token.Data = map[string][]byte{corev1.ServiceAccountTokenKey: []byte("t")}
		}

		return []client.Object{want.Token, want.RoleBinding, want.Role, want.ServiceAccount}
	}

	status := func() *operator.Status {
		c := newClient(objs, funcs)

		return &operator.Status{Reader: c, Identity: &identity.Ensurer{Reader: c, Live: c, Writer: c}}
	}

	call := func(s *operator.Status, c *apiv1.Cluster) []byte {
		GinkgoHelper()

		resp, err := s.SetStatusInCluster(ctx, hookStatusRequest(c))
		Expect(err).NotTo(HaveOccurred())

		return resp.GetJsonStatus()
	}

	decode := func(raw []byte) operator.ClusterStatus {
		GinkgoHelper()

		var st operator.ClusterStatus
		Expect(json.Unmarshal(raw, &st)).To(Succeed())

		return st
	}

	BeforeEach(func(c context.Context) {
		ctx = c
		funcs = interceptor.Funcs{}
		cluster = newCluster("ns", "pg", ours(nil))
		cluster.Spec.PostgresConfiguration.PgIdent = []string{goodIdent}
		objs = append([]client.Object{newExporterWithEndpoint()}, identityObjects(true)...)
	})

	It("reports a healthy status the first time", func() {
		st := decode(call(status(), cluster))

		Expect(st).To(Equal(operator.ClusterStatus{Exporter: "default", ExporterState: "ok", Identity: "ready"}))
	})

	It("reports the exporter missing", func() {
		objs = identityObjects(true)

		st := decode(call(status(), cluster))

		Expect(st.Exporter).To(Equal("default"))
		Expect(st.ExporterState).To(Equal("missing"))
	})

	It("reports the exporter invalid with the first error", func() {
		e := newExporterWithEndpoint()
		e.Spec.Exporter.Endpoint = "ftp://c"
		objs = append([]client.Object{e}, identityObjects(true)...)

		st := decode(call(status(), cluster))

		Expect(st.ExporterState).To(Equal("invalid"))
		Expect(st.Message).To(ContainSubstring("endpoint"))
	})

	It("reports the exporter named by the parameters", func() {
		cluster = newCluster("ns", "pg", ours(map[string]string{"exporter": "other"}))
		cluster.Spec.PostgresConfiguration.PgIdent = []string{goodIdent}

		st := decode(call(status(), cluster))

		Expect(st.Exporter).To(Equal("other"))
		Expect(st.ExporterState).To(Equal("missing"))
	})

	It("reports identity pending until the four objects exist and the token has data", func() {
		objs = append([]client.Object{newExporterWithEndpoint()}, identityObjects(false)...)
		Expect(decode(call(status(), cluster)).Identity).To(Equal("pending"))

		objs = []client.Object{newExporterWithEndpoint()}
		Expect(decode(call(status(), cluster)).Identity).To(Equal("pending"))
	})

	It("reports the identity from its four objects whatever other Clusters the namespace holds", func() {
		objs = append(append([]client.Object{newExporterWithEndpoint()}, identityObjects(true)...), newCluster("ns", metadata.ServiceAccountName("pg")))

		st := decode(call(status(), cluster))

		Expect(st.Identity).To(Equal("ready"))
		Expect(st.Problems).To(BeEmpty())
	})

	It("reports identity pending with each identity object the plugin doesn't manage", func() {
		objs = []client.Object{
			newExporterWithEndpoint(),
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}},
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}},
		}

		st := decode(call(status(), cluster))

		Expect(st.Identity).To(Equal("pending"))
		Expect(st.Problems).To(ConsistOf(
			"Role/pg.telemetry exists and is not managed by the plugin: no identity is applied",
			"ServiceAccount/pg.telemetry exists and is not managed by the plugin: no identity is applied",
		))
	})

	It("lists the spec problems and parameter problems, sorted", func() {
		cluster = newCluster("ns", "pg", ours(map[string]string{"zzz": "1", "sidecarCPU": "x"}))

		st := decode(call(status(), cluster))

		Expect(st.Problems).To(HaveLen(3))
		Expect(sort.StringsAreSorted(st.Problems)).To(BeTrue())
		Expect(st.Problems).To(ContainElement(`unknown plugin parameter "zzz" is ignored`))
		Expect(st.Problems).To(ContainElement(ContainSubstring("peer mode needs the pg_ident line")))
	})

	It("reports an invalid auth", func() {
		cluster = newCluster("ns", "pg", ours(map[string]string{"auth": "kerberos"}))

		st := decode(call(status(), cluster))

		Expect(st.Problems).To(ContainElement(`auth "kerberos" is invalid: no sidecar is injected`))
		Expect(st.ExporterState).To(Equal("ok"))
	})

	It("reports all parameter problems with an invalid auth", func() {
		cluster = newCluster("ns", "pg", ours(map[string]string{"auth": "kerberos", "zzz": "1", "sidecarCPU": "x"}))

		st := decode(call(status(), cluster))

		Expect(st.Problems).To(HaveLen(3))
		Expect(st.Problems).To(ContainElement(`auth "kerberos" is invalid: no sidecar is injected`))
		Expect(st.Problems).To(ContainElement(`unknown plugin parameter "zzz" is ignored`))
		Expect(st.Problems).To(ContainElement(ContainSubstring("sidecarCPU")))
	})

	It("returns NoOpResponse when healthy and equal to the stored status", func() {
		first := call(status(), cluster)
		Expect(first).NotTo(BeEmpty())

		Expect(call(status(), withStored(cluster, string(first)))).To(BeEmpty())
	})

	DescribeTable("returns NoOpResponse for an unchanged missing exporter, whatever the identity",
		func(token bool) {
			objs = identityObjects(token)
			first := call(status(), cluster)
			Expect(decode(first).ExporterState).To(Equal("missing"))

			Expect(call(status(), withStored(cluster, string(first)))).To(BeEmpty())
		},
		Entry("identity ready", true),
		Entry("identity pending", false),
	)

	It("returns the status the first time the exporter is missing", func() {
		first := call(status(), cluster)
		Expect(decode(first).Healthy()).To(BeTrue())
		objs = identityObjects(true)

		second := call(status(), withStored(cluster, string(first)))
		Expect(decode(second).ExporterState).To(Equal("missing"))
	})

	DescribeTable("returns the status while invalid or pending even when unchanged",
		func(objects func() []client.Object) {
			objs = objects()
			first := call(status(), cluster)
			Expect(decode(first).Healthy()).To(BeFalse())
			Expect(decode(first).ExporterState).NotTo(Equal("missing"))

			Expect(call(status(), withStored(cluster, string(first)))).To(Equal(first))
		},
		Entry("exporter invalid", func() []client.Object {
			e := newExporterWithEndpoint()
			e.Spec.Exporter.Endpoint = "ftp://c"

			return append([]client.Object{e}, identityObjects(true)...)
		}),
		Entry("identity pending", func() []client.Object {
			return append([]client.Object{newExporterWithEndpoint()}, identityObjects(false)...)
		}),
	)

	It("returns the new status when it changed", func() {
		first := call(status(), cluster)
		objs = identityObjects(true)

		Expect(call(status(), withStored(cluster, string(first)))).NotTo(Equal(first))
	})

	It("returns a changed healthy status, then nothing once it is stored", func() {
		first := call(status(), cluster)
		Expect(decode(first).Healthy()).To(BeTrue())

		changed := newCluster("ns", "pg", ours(map[string]string{"zzz": "1"}))
		changed.Spec.PostgresConfiguration.PgIdent = []string{goodIdent}
		changed.Status.PluginStatus = []apiv1.PluginStatus{{Name: metadata.PluginName, Status: string(first)}}
		second := call(status(), changed)
		Expect(second).NotTo(BeEmpty())
		Expect(second).NotTo(Equal(first))
		Expect(decode(second).Healthy()).To(BeTrue())

		changed.Status.PluginStatus[0].Status = string(second)
		Expect(call(status(), changed)).To(BeEmpty())
	})

	It("ignores the stored status of another plugin", func() {
		first := call(status(), cluster)
		c := withStored(cluster, string(first))
		c.Status.PluginStatus[0].Name = "other"

		Expect(call(status(), c)).To(Equal(first))
	})

	It("marshals the same Cluster to byte-identical JSON on every call, with problems sorted", func() {
		cluster = newCluster("ns", "pg", ours(map[string]string{"b": "1", "a": "2", "c": "3"}))
		objs = identityObjects(false)
		s := status()

		first := call(s, cluster)
		for range 5 {
			Expect(call(s, cluster)).To(Equal(first))
		}
		Expect(sort.StringsAreSorted(decode(first).Problems)).To(BeTrue())
	})

	It("answers nothing when the plugin is not enabled for the Cluster", func() {
		Expect(call(status(), newCluster("ns", "pg", disabled(nil)))).To(BeEmpty())
		Expect(call(status(), newCluster("ns", "pg"))).To(BeEmpty())
	})

	It("never returns an error, for undecodable input too", func() {
		s := status()

		resp, err := s.SetStatusInCluster(ctx, &cnpgoperator.SetStatusInClusterRequest{Cluster: []byte("{")})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonStatus()).To(BeEmpty())

		resp, err = s.SetStatusInCluster(ctx, &cnpgoperator.SetStatusInClusterRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonStatus()).To(BeEmpty())
	})

	It("never returns an error when the cache fails", func() {
		funcs.Get = func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errApply
		}

		resp, err := status().SetStatusInCluster(ctx, hookStatusRequest(cluster))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonStatus()).To(BeEmpty())
	})
})

func withStored(c *apiv1.Cluster, stored string) *apiv1.Cluster {
	out := c.DeepCopy()
	out.Status.PluginStatus = []apiv1.PluginStatus{{Name: metadata.PluginName, Status: stored}}

	return out
}

func newExporterWithEndpoint() *v1alpha1.TelemetryExporter {
	e := newExporter("ns", "default")
	e.Spec.Exporter.Endpoint = "http://collector:4318"

	return e
}

func hookStatusRequest(c *apiv1.Cluster) *cnpgoperator.SetStatusInClusterRequest {
	GinkgoHelper()

	raw, err := json.Marshal(c)
	Expect(err).NotTo(HaveOccurred())

	return &cnpgoperator.SetStatusInClusterRequest{Cluster: raw}
}
