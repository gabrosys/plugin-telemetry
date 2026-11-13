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
			r, err := podpatch.ParseParams(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(r.Params.Auth).To(Equal(auth))
			Expect(r.Params.CPU.Cmp(resource.MustParse(cpu))).To(BeZero())
			Expect(r.Params.Memory.Cmp(resource.MustParse(mem))).To(BeZero())
			Expect(r.Ignored).To(ConsistOf(ignored))
			Expect(r.Defaulted).To(BeEmpty())
		},
		Entry("no parameters", map[string]string(nil), podpatch.AuthPeer, "50m", "64Mi", nil),
		Entry("password mode", map[string]string{"auth": "password", "passwordSecret": "s"}, podpatch.AuthPassword, "50m", "64Mi", nil),
		Entry("resources", map[string]string{"sidecarCPU": "100m", "sidecarMemory": "128Mi"}, podpatch.AuthPeer, "100m", "128Mi", nil),
		Entry("the floors", map[string]string{"sidecarCPU": "10m", "sidecarMemory": "32Mi"}, podpatch.AuthPeer, "10m", "32Mi", nil),
		Entry("keys that don't shape the Pod", map[string]string{"exporter": "default"}, podpatch.AuthPeer, "50m", "64Mi", nil),
		Entry("an unknown key", map[string]string{"sidecarMemroy": "1Gi"}, podpatch.AuthPeer, "50m", "64Mi", []string{"sidecarMemroy"}),
	)

	It("returns ignored keys sorted", func() {
		r, err := podpatch.ParseParams(map[string]string{"zeta": "1", "alpha": "2", "mid": "3"})
		Expect(err).NotTo(HaveOccurred())
		Expect(r.Ignored).To(Equal([]string{"alpha", "mid", "zeta"}))
	})

	DescribeTable("accepts with the default",
		func(raw map[string]string, cpu, mem, key string) {
			r, err := podpatch.ParseParams(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(r.Params.CPU.Cmp(resource.MustParse(cpu))).To(BeZero())
			Expect(r.Params.Memory.Cmp(resource.MustParse(mem))).To(BeZero())
			Expect(r.Defaulted).To(HaveLen(1))
			Expect(r.Defaulted[0]).To(MatchError(podpatch.ErrInvalidParameters))
			Expect(r.Defaulted[0]).To(MatchError(ContainSubstring(key)))
			Expect(r.Defaulted[0]).To(MatchError(ContainSubstring(raw[key])))
		},
		Entry("memory without a unit", map[string]string{"sidecarMemory": "64"}, "50m", "64Mi", "sidecarMemory"),
		Entry("memory below 32Mi", map[string]string{"sidecarMemory": "16Mi"}, "50m", "64Mi", "sidecarMemory"),
		Entry("CPU below 10m", map[string]string{"sidecarCPU": "5m"}, "50m", "64Mi", "sidecarCPU"),
		Entry("a negative CPU", map[string]string{"sidecarCPU": "-1"}, "50m", "64Mi", "sidecarCPU"),
		Entry("a quantity that doesn't parse", map[string]string{"sidecarCPU": "lots"}, "50m", "64Mi", "sidecarCPU"),
		Entry("a valid CPU next to an invalid memory", map[string]string{"sidecarCPU": "100m", "sidecarMemory": "x"}, "100m", "64Mi", "sidecarMemory"),
	)

	It("defaults both invalid resources, CPU first", func() {
		r, err := podpatch.ParseParams(map[string]string{"sidecarMemory": "x", "sidecarCPU": "y"})
		Expect(err).NotTo(HaveOccurred())
		Expect(r.Params.CPU.Cmp(resource.MustParse("50m"))).To(BeZero())
		Expect(r.Params.Memory.Cmp(resource.MustParse("64Mi"))).To(BeZero())
		Expect(r.Defaulted).To(HaveLen(2))
		Expect(r.Defaulted[0]).To(MatchError(ContainSubstring("sidecarCPU")))
		Expect(r.Defaulted[1]).To(MatchError(ContainSubstring("sidecarMemory")))
	})

	DescribeTable("rejects",
		func(raw map[string]string) {
			_, err := podpatch.ParseParams(raw)
			Expect(err).To(MatchError(podpatch.ErrInvalidParameters))
		},
		Entry("an unknown mode", map[string]string{"auth": "ldap"}),
		Entry("a mode in the wrong case", map[string]string{"auth": "Peer"}),
		Entry("an invalid mode next to an invalid resource", map[string]string{"auth": "ldap", "sidecarCPU": "lots"}),
	)

	It("reports the auth error, not the resource, when both are invalid", func() {
		_, err := podpatch.ParseParams(map[string]string{"auth": "ldap", "sidecarCPU": "lots"})
		Expect(err).To(MatchError(ContainSubstring("auth")))
		Expect(err).NotTo(MatchError(ContainSubstring("sidecarCPU")))
	})
})
