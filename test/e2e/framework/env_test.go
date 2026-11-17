package framework_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

func getenv(kubeContext, cnpgVersion, image string) func(string) string {
	vars := map[string]string{
		"E2E_KUBE_CONTEXT": kubeContext,
		"CNPG_VERSION":     cnpgVersion,
		"POSTGRES_IMAGE":   image,
	}

	return func(key string) string { return vars[key] }
}

var _ = Describe("LoadEnv", func() {
	DescribeTable("accepts",
		func(kubeContext, cnpgVersion, image string) {
			env, err := framework.LoadEnv(getenv(kubeContext, cnpgVersion, image))
			Expect(err).NotTo(HaveOccurred())
			Expect(env).To(Equal(framework.Env{
				KubeContext:   kubeContext,
				CNPGVersion:   cnpgVersion,
				PostgresImage: image,
			}))
		},
		Entry("a release", "kind-plugin-telemetry", "v1.30.1", "img"),
		Entry("main", "kind-plugin-telemetry", "main", "img"),
	)

	It("reads CMCTL, empty when unset", func() {
		get := getenv("kind-x", "main", "img")
		env, err := framework.LoadEnv(get)
		Expect(err).NotTo(HaveOccurred())
		Expect(env.CMCTL).To(BeEmpty())

		env, err = framework.LoadEnv(func(key string) string {
			if key == "CMCTL" {
				return "/bin/cmctl"
			}

			return get(key)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(env.CMCTL).To(Equal("/bin/cmctl"))
	})

	DescribeTable("rejects",
		func(kubeContext, cnpgVersion, image string) {
			_, err := framework.LoadEnv(getenv(kubeContext, cnpgVersion, image))
			Expect(err).To(MatchError(framework.ErrInvalidEnv))
		},
		Entry("a missing context", "", "v1.30.1", "img"),
		Entry("a non-kind context", "prod-eu", "v1.30.1", "img"),
		Entry("a version without v", "kind-x", "1.30.1", "img"),
		Entry("a floating tag", "kind-x", "latest", "img"),
		Entry("a missing image", "kind-x", "main", ""),
	)
})

var _ = DescribeTable("MatchesCNPGVersion",
	func(image, cnpgVersion string, want bool) {
		Expect(framework.MatchesCNPGVersion(image, cnpgVersion)).To(Equal(want))
	},
	Entry("release tag", "ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1", "v1.30.1", true),
	Entry("longer patch", "ghcr.io/cloudnative-pg/cloudnative-pg:1.30.10", "v1.30.1", false),
	Entry("main by digest",
		"ghcr.io/cloudnative-pg/cloudnative-pg-testing:main@sha256:b5396004f2c75d8ace41ad3f6ba130e8aa0856dcf821e7d7293bdd843c2e621f",
		"main", true),
	Entry("release for main", "ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1", "main", false),
)
