package policy_test

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/operator"
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

var _ = Describe("the plugin Service", func() {
	It("names the declared capabilities, so a change of them changes the Service and CNPG registers the plugin again", func(ctx context.Context) {
		resp, err := operator.Identity{}.GetPluginCapabilities(ctx, &identity.GetPluginCapabilitiesRequest{})
		Expect(err).NotTo(HaveOccurred())

		var declared []string
		for _, c := range resp.GetCapabilities() {
			name := strings.TrimSuffix(strings.TrimPrefix(c.GetService().GetType().String(), "TYPE_"), "_SERVICE")
			declared = append(declared, strings.ToLower(name))
		}
		sort.Strings(declared)

		manifests, err := readManifests("config/service.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(find(manifests, "Service").Metadata.Annotations).To(
			HaveKeyWithValue("telemetry.gabrosys.github.io/capabilities", strings.Join(declared, ",")))
	})
})
