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
		Expect(checked).To(Equal(7))
	})
})
