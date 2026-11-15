package policy_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.yaml.in/yaml/v4"
)

const rbacFile = "config/rbac.yaml"

type rule struct {
	APIGroups     []string `yaml:"apiGroups"`
	Resources     []string `yaml:"resources"`
	ResourceNames []string `yaml:"resourceNames"`
	Verbs         []string `yaml:"verbs"`
}

type manifest struct {
	Kind      string `yaml:"kind"`
	Namespace string `yaml:"namespace"`
	Metadata  struct {
		Name        string            `yaml:"name"`
		Namespace   string            `yaml:"namespace"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	ConfigMapGenerator []struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"configMapGenerator"`
	Spec struct {
		MatchConditions []struct {
			Name       string `yaml:"name"`
			Expression string `yaml:"expression"`
		} `yaml:"matchConditions"`
		Validations []struct {
			Message string `yaml:"message"`
		} `yaml:"validations"`
	} `yaml:"spec"`
	Rules   []rule `yaml:"rules"`
	RoleRef struct {
		Kind string `yaml:"kind"`
		Name string `yaml:"name"`
	} `yaml:"roleRef"`
	Subjects []struct {
		Kind      string `yaml:"kind"`
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"subjects"`
}

func readManifests(file string) ([]manifest, error) {
	raw, err := fs.ReadFile(os.DirFS(repoRoot), file)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}

	var out []manifest
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	for {
		var m manifest
		err := dec.Decode(&m)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", file, err)
		}
		out = append(out, m)
	}
}

func find(manifests []manifest, kind string) manifest {
	GinkgoHelper()

	for _, m := range manifests {
		if m.Kind == kind {
			return m
		}
	}
	Fail("no " + kind)

	return manifest{}
}

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
