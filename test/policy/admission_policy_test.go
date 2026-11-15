package policy_test

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("the admission policies", func() {
	It("never repeat a policy name in a validation message, which the API server prefixes already", func() {
		manifests, err := readManifests("config/admission-policy.yaml")
		Expect(err).NotTo(HaveOccurred())

		checked := 0
		for _, m := range manifests {
			if m.Kind != "ValidatingAdmissionPolicy" {
				continue
			}
			Expect(m.Spec.Validations).NotTo(BeEmpty(), m.Metadata.Name)
			for i, v := range m.Spec.Validations {
				where := fmt.Sprintf("%s validation %d", m.Metadata.Name, i)
				Expect(v.Message).NotTo(BeEmpty(), where)
				Expect(strings.ToLower(v.Message)).NotTo(ContainSubstring("plugin-telemetry"), where)
				checked++
			}
		}
		Expect(checked).To(Equal(9))
	})
})

var _ = Describe("the policies' match conditions", func() {
	It("find the plugin by account name in any namespace, as CNPG decides where the plugin runs", func() {
		manifests, err := readManifests("config/admission-policy.yaml")
		Expect(err).NotTo(HaveOccurred())

		checked := 0
		for _, m := range manifests {
			for _, mc := range m.Spec.MatchConditions {
				where := m.Metadata.Name + " " + mc.Name
				Expect(mc.Expression).NotTo(ContainSubstring(pluginNamespace), where)
				Expect(mc.Expression).To(ContainSubstring("startsWith('system:serviceaccount:')"), where)
				Expect(mc.Expression).To(ContainSubstring("endsWith(':plugin-telemetry')"), where)
				Expect(mc.Expression).To(ContainSubstring("endsWith('.telemetry')"), where)
				checked++
			}
		}
		Expect(checked).To(Equal(2))
	})
})
