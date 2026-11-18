package policy_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const pluginNamespace = "cnpg-system"

var clusterScoped = map[string]bool{
	"ValidatingAdmissionPolicy":        true,
	"ValidatingAdmissionPolicyBinding": true,
	"ClusterRole":                      true,
	"ClusterRoleBinding":               true,
	"PriorityClass":                    true,
}

var _ = Describe("the config manifests", func() {
	It("set no namespace in the kustomization and give every namespaced object in config cnpg-system, cluster-scoped ones none", func() {
		files, err := fs.Glob(os.DirFS(repoRoot), "config/*.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(files).NotTo(BeEmpty())

		for _, file := range files {
			manifests := readManifests(file)

			for _, m := range manifests {
				where := fmt.Sprintf("%s %s %s", filepath.Base(file), m.Kind, m.Metadata.Name)
				Expect(m.Kind).NotTo(BeElementOf("Role", "RoleBinding"), where)

				switch {
				case m.Kind == "Kustomization":
					Expect(m.Namespace).To(BeEmpty(), where)
					Expect(m.ConfigMapGenerator).NotTo(BeEmpty(), where)
					for _, g := range m.ConfigMapGenerator {
						Expect(g.Namespace).To(Equal(pluginNamespace), where+" generator "+g.Name)
					}
				case clusterScoped[m.Kind]:
					Expect(m.Metadata.Namespace).To(BeEmpty(), where)
				default:
					Expect(m.Metadata.Namespace).To(Equal(pluginNamespace), where)
				}

				for _, s := range m.Subjects {
					Expect(s.Namespace).To(Equal(pluginNamespace), where+" subject "+s.Name)
				}
			}
		}
	})
})
