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

const (
	kustomizationFile = "config/kustomization.yaml"
	rbacFile          = "config/rbac.yaml"
	admissionFile     = "config/admission-policy.yaml"
)

type rule struct {
	APIGroups     []string `yaml:"apiGroups"`
	Resources     []string `yaml:"resources"`
	ResourceNames []string `yaml:"resourceNames"`
	Verbs         []string `yaml:"verbs"`
}

type manifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Namespace string `yaml:"namespace"`
	Rules     []rule `yaml:"rules"`
	RoleRef   struct {
		Kind string `yaml:"kind"`
		Name string `yaml:"name"`
	} `yaml:"roleRef"`
	Subjects []struct {
		Kind string `yaml:"kind"`
		Name string `yaml:"name"`
	} `yaml:"subjects"`
	Spec struct {
		FailurePolicy     string   `yaml:"failurePolicy"`
		ValidationActions []string `yaml:"validationActions"`
		MatchConditions   []struct {
			Expression string `yaml:"expression"`
		} `yaml:"matchConditions"`
	} `yaml:"spec"`
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
	for _, m := range manifests {
		if m.Kind == kind {
			return m
		}
	}
	Fail("no " + kind)

	return manifest{}
}

var _ = Describe("the admission policy", func() {
	It("matches the plugin ServiceAccount in the namespace kustomize deploys to", func() {
		kustomization, err := readManifests(kustomizationFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(kustomization).To(HaveLen(1))
		Expect(kustomization[0].Namespace).NotTo(BeEmpty())

		rbac, err := readManifests(rbacFile)
		Expect(err).NotTo(HaveOccurred())
		account := find(rbac, "ServiceAccount").Metadata.Name

		policies, err := readManifests(admissionFile)
		Expect(err).NotTo(HaveOccurred())
		policy := find(policies, "ValidatingAdmissionPolicy")
		binding := find(policies, "ValidatingAdmissionPolicyBinding")

		Expect(policy.Spec.MatchConditions).To(HaveLen(1))
		Expect(policy.Spec.MatchConditions[0].Expression).To(Equal(
			fmt.Sprintf("request.userInfo.username == 'system:serviceaccount:%s:%s'", kustomization[0].Namespace, account)))
		Expect(policy.Spec.FailurePolicy).To(Equal("Fail"))
		Expect(binding.Spec.ValidationActions).To(Equal([]string{"Deny"}))
	})
})
