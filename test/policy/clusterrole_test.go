package policy_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("the plugin ClusterRole", func() {
	var (
		role    manifest
		binding manifest
	)

	BeforeEach(func() {
		rbac, err := readManifests(rbacFile)
		Expect(err).NotTo(HaveOccurred())
		role = find(rbac, "ClusterRole")
		binding = find(rbac, "ClusterRoleBinding")
	})

	It("grants exactly the documented rules and never serviceaccounts/token, escalate or bind", func() {
		readOnly := []string{"get", "list", "watch"}
		readWrite := []string{"get", "list", "watch", "create", "patch", "delete"}

		Expect(role.Metadata.Name).To(Equal("plugin-telemetry"))
		Expect(role.Rules).To(ConsistOf(
			rule{APIGroups: []string{"postgresql.cnpg.io"}, Resources: []string{"clusters"}, Verbs: readOnly},
			rule{
				APIGroups: []string{"telemetry.gabrosys.github.io"},
				Resources: []string{"telemetryexporters", "metricquerysets"}, Verbs: readOnly,
			},
			rule{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: readOnly},
			rule{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: readWrite},
			rule{APIGroups: []string{""}, Resources: []string{"serviceaccounts"}, Verbs: readWrite},
			rule{
				APIGroups: []string{"rbac.authorization.k8s.io"},
				Resources: []string{"roles", "rolebindings"}, Verbs: readWrite,
			},
		))

		for _, r := range role.Rules {
			Expect(r.Resources).NotTo(ContainElement("serviceaccounts/token"))
			Expect(r.Verbs).NotTo(ContainElements("escalate"))
			Expect(r.Verbs).NotTo(ContainElements("bind"))
			Expect(r.Verbs).NotTo(ContainElements("*"))
			Expect(r.Resources).NotTo(ContainElements("*"))
		}
	})

	It("is bound to the plugin ServiceAccount", func() {
		rbac, err := readManifests(rbacFile)
		Expect(err).NotTo(HaveOccurred())
		account := find(rbac, "ServiceAccount").Metadata.Name

		Expect(binding.Metadata.Name).To(Equal("plugin-telemetry"))
		Expect(binding.RoleRef.Kind).To(Equal("ClusterRole"))
		Expect(binding.RoleRef.Name).To(Equal(role.Metadata.Name))
		Expect(binding.Subjects).To(HaveLen(1))
		Expect(binding.Subjects[0].Kind).To(Equal("ServiceAccount"))
		Expect(binding.Subjects[0].Name).To(Equal(account))
	})
})
