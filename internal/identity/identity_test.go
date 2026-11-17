package identity_test

import (
	"encoding/json"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
)

const goldenFile = "desired.golden.json"

var _ = Describe("Desired", func() {
	It("computes the four objects for a Cluster, matching testdata/desired.golden.json", func() {
		objs, err := identity.Desired(newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		got, err := json.MarshalIndent(objs, "", "  ")
		Expect(err).NotTo(HaveOccurred())
		got = append(got, '\n')

		root, err := os.OpenRoot("testdata")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(root.Close)

		if os.Getenv("UPDATE_GOLDEN") != "" {
			Expect(root.WriteFile(goldenFile, got, 0o600)).To(Succeed())
		}

		want, err := root.ReadFile(goldenFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(want)))
	})

	It("never emits a rule with empty resourceNames, and omits kinds with no names", func() {
		r := clusterconfig.Refs{Cluster: "pg", Exporter: "default", Secrets: []string{"s"}}
		objs, err := identity.Desired(newCluster(), r)
		Expect(err).NotTo(HaveOccurred())

		var resources []string
		for _, rule := range objs.Role.Rules {
			Expect(rule.ResourceNames).NotTo(BeEmpty())
			Expect(rule.Verbs).To(Equal([]string{"get", "list", "watch"}))
			resources = append(resources, rule.Resources...)
		}
		Expect(resources).To(ConsistOf("clusters", "telemetryexporters", "secrets"))
	})

	It("drops empty names for every kind", func() {
		r := clusterconfig.Refs{Secrets: []string{""}, ConfigMaps: []string{"", "a"}, QuerySets: []string{""}}
		objs, err := identity.Desired(newCluster(), r)
		Expect(err).NotTo(HaveOccurred())

		Expect(objs.Role.Rules).To(HaveLen(1))
		Expect(objs.Role.Rules[0].Resources).To(Equal([]string{"configmaps"}))
		Expect(objs.Role.Rules[0].ResourceNames).To(Equal([]string{"a"}))
		for _, rule := range objs.Role.Rules {
			Expect(rule.ResourceNames).NotTo(BeEmpty())
			Expect(rule.ResourceNames).NotTo(ContainElement(""))
		}
	})

	It("sets the owner reference from the live UID with controller true and blockOwnerDeletion false", func() {
		objs, err := identity.Desired(newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		for _, owners := range [][]metav1.OwnerReference{
			objs.ServiceAccount.OwnerReferences, objs.Role.OwnerReferences,
			objs.RoleBinding.OwnerReferences, objs.Token.OwnerReferences,
		} {
			Expect(owners).To(HaveLen(1))
			Expect(owners[0].APIVersion).To(Equal("postgresql.cnpg.io/v1"))
			Expect(owners[0].Kind).To(Equal("Cluster"))
			Expect(owners[0].Name).To(Equal("pg"))
			Expect(string(owners[0].UID)).To(Equal(string(liveUID)))
			Expect(owners[0].Controller).To(Equal(ptr.To(true)))
			Expect(owners[0].BlockOwnerDeletion).To(Equal(ptr.To(false)))
		}
	})

	It("labels every object so the cache sees it", func() {
		objs, err := identity.Desired(newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		for _, labels := range []map[string]string{
			objs.ServiceAccount.Labels, objs.Role.Labels, objs.RoleBinding.Labels, objs.Token.Labels,
		} {
			Expect(labels).To(Equal(map[string]string{
				"app.kubernetes.io/managed-by":         "plugin-telemetry",
				"telemetry.gabrosys.github.io/cluster": "pg",
			}))
		}
	})

	It("gives the token Secret only its type and the service-account.name annotation", func() {
		objs, err := identity.Desired(newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		Expect(objs.Token.Name).To(Equal("pg.telemetry-token"))
		Expect(objs.Token.Type).To(Equal(corev1.SecretTypeServiceAccountToken))
		Expect(objs.Token.Annotations).To(Equal(map[string]string{"kubernetes.io/service-account.name": "pg.telemetry"}))
		Expect(objs.Token.Data).To(BeEmpty())
		Expect(objs.Token.StringData).To(BeEmpty())
	})

	It("binds the Role to the ServiceAccount", func() {
		objs, err := identity.Desired(newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		Expect(objs.RoleBinding.RoleRef).To(Equal(rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pg.telemetry"}))
		Expect(objs.RoleBinding.Subjects).To(Equal([]rbacv1.Subject{{Kind: "ServiceAccount", Name: "pg.telemetry", Namespace: "ns"}}))
	})

	It("fails for a Cluster without a UID", func() {
		c := newCluster()
		c.UID = ""
		_, err := identity.Desired(c, refs())
		Expect(err).To(MatchError(identity.ErrNoUID))
	})
})
