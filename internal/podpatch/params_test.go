package podpatch_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

var _ = Describe("ParseParams", func() {
	DescribeTable("accepts",
		func(raw map[string]string, auth podpatch.AuthMode, cpu, mem string, ignored []string) {
			p, ign, err := podpatch.ParseParams(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.Auth).To(Equal(auth))
			Expect(p.CPU.Cmp(resource.MustParse(cpu))).To(BeZero())
			Expect(p.Memory.Cmp(resource.MustParse(mem))).To(BeZero())
			Expect(ign).To(ConsistOf(ignored))
		},
		Entry("no parameters", map[string]string(nil), podpatch.AuthPeer, "50m", "64Mi", nil),
		Entry("password mode", map[string]string{"auth": "password", "passwordSecret": "s"}, podpatch.AuthPassword, "50m", "64Mi", nil),
		Entry("resources", map[string]string{"sidecarCPU": "100m", "sidecarMemory": "128Mi"}, podpatch.AuthPeer, "100m", "128Mi", nil),
		Entry("the floors", map[string]string{"sidecarCPU": "10m", "sidecarMemory": "32Mi"}, podpatch.AuthPeer, "10m", "32Mi", nil),
		Entry("keys that don't shape the Pod", map[string]string{"exporter": "default"}, podpatch.AuthPeer, "50m", "64Mi", nil),
		Entry("an unknown key", map[string]string{"sidecarMemroy": "1Gi"}, podpatch.AuthPeer, "50m", "64Mi", []string{"sidecarMemroy"}),
	)

	It("returns ignored keys sorted", func() {
		_, ign, err := podpatch.ParseParams(map[string]string{"zeta": "1", "alpha": "2", "mid": "3"})
		Expect(err).NotTo(HaveOccurred())
		Expect(ign).To(Equal([]string{"alpha", "mid", "zeta"}))
	})

	DescribeTable("rejects",
		func(raw map[string]string) {
			_, _, err := podpatch.ParseParams(raw)
			Expect(err).To(MatchError(podpatch.ErrInvalidParameters))
		},
		Entry("an unknown mode", map[string]string{"auth": "ldap"}),
		Entry("a mode in the wrong case", map[string]string{"auth": "Peer"}),
		Entry("memory without a unit", map[string]string{"sidecarMemory": "64"}),
		Entry("memory below 32Mi", map[string]string{"sidecarMemory": "16Mi"}),
		Entry("CPU below 10m", map[string]string{"sidecarCPU": "5m"}),
		Entry("a negative CPU", map[string]string{"sidecarCPU": "-1"}),
		Entry("a quantity that doesn't parse", map[string]string{"sidecarCPU": "lots"}),
	)
})
