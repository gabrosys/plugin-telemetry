package policy_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.yaml.in/yaml/v4"
)

const (
	kustomizationFile = "config/kustomization.yaml"
	rbacFile          = "config/rbac.yaml"
	admissionFile     = "config/admission-policy.yaml"
	policyName        = "plugin-telemetry-identities"
	rolesPolicyName   = "plugin-telemetry-roles"
	policyKind        = "ValidatingAdmissionPolicy"
	bindingKind       = "ValidatingAdmissionPolicyBinding"
)

type rule struct {
	APIGroups     []string `yaml:"apiGroups"`
	Resources     []string `yaml:"resources"`
	ResourceNames []string `yaml:"resourceNames"`
	Verbs         []string `yaml:"verbs"`
}

type resourceRule struct {
	APIGroups   []string `yaml:"apiGroups"`
	APIVersions []string `yaml:"apiVersions"`
	Operations  []string `yaml:"operations"`
	Resources   []string `yaml:"resources"`
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
		PolicyName        string   `yaml:"policyName"`
		ValidationActions []string `yaml:"validationActions"`
		MatchConditions   []struct {
			Expression string `yaml:"expression"`
		} `yaml:"matchConditions"`
		MatchConstraints struct {
			ResourceRules []resourceRule `yaml:"resourceRules"`
		} `yaml:"matchConstraints"`
		Validations []struct {
			Expression string `yaml:"expression"`
			Reason     string `yaml:"reason"`
			Message    string `yaml:"message"`
		} `yaml:"validations"`
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

func findAll(manifests []manifest, kind string) []manifest {
	var out []manifest
	for _, m := range manifests {
		if m.Kind == kind {
			out = append(out, m)
		}
	}

	return out
}

func named(manifests []manifest, kind, name string) manifest {
	for _, m := range findAll(manifests, kind) {
		if m.Metadata.Name == name {
			return m
		}
	}
	Fail("no " + kind + " " + name)

	return manifest{}
}

var _ = Describe("the admission policy", func() {
	It("matches the plugin ServiceAccount in the namespace kustomize deploys to, in every policy", func() {
		kustomization, err := readManifests(kustomizationFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(kustomization).To(HaveLen(1))
		Expect(kustomization[0].Namespace).NotTo(BeEmpty())

		rbac, err := readManifests(rbacFile)
		Expect(err).NotTo(HaveOccurred())
		account := find(rbac, "ServiceAccount").Metadata.Name

		manifests, err := readManifests(admissionFile)
		Expect(err).NotTo(HaveOccurred())
		policies := findAll(manifests, policyKind)
		Expect(policies).To(ConsistOf(HaveField("Metadata.Name", policyName), HaveField("Metadata.Name", rolesPolicyName)))

		for _, policy := range policies {
			Expect(policy.Spec.MatchConditions).To(HaveLen(1), policy.Metadata.Name)
			Expect(policy.Spec.MatchConditions[0].Expression).To(Equal(
				fmt.Sprintf("request.userInfo.username == 'system:serviceaccount:%s:%s'", kustomization[0].Namespace, account)),
				policy.Metadata.Name)
			Expect(policy.Spec.FailurePolicy).To(Equal("Fail"), policy.Metadata.Name)

			binding := named(manifests, bindingKind, policy.Metadata.Name)
			Expect(binding.Spec.PolicyName).To(Equal(policy.Metadata.Name))
			Expect(binding.Spec.ValidationActions).To(Equal([]string{"Deny"}), policy.Metadata.Name)
		}
		Expect(findAll(manifests, bindingKind)).To(HaveLen(len(policies)))
	})

	It("matches serviceaccounts/token CREATE and denies it with reason Forbidden", func() {
		policies, err := readManifests(admissionFile)
		Expect(err).NotTo(HaveOccurred())
		policy := named(policies, policyKind, policyName)

		var matched bool
		for _, r := range policy.Spec.MatchConstraints.ResourceRules {
			if slices.Contains(r.Resources, "serviceaccounts/token") {
				matched = true
				Expect(r.APIGroups).To(Equal([]string{""}))
				Expect(r.Operations).To(Equal([]string{"CREATE"}))
			}
		}
		Expect(matched).To(BeTrue())

		var tokenRule bool
		for _, v := range policy.Spec.Validations {
			Expect(v.Reason).To(Equal("Forbidden"))
			Expect(v.Message).To(ContainSubstring(policyName))
			if v.Expression == "!variables.tokenRequest" {
				tokenRule = true
			}
		}
		Expect(tokenRule).To(BeTrue())
	})

	It("lets the Secret and ServiceAccount validation skip TokenRequests", func() {
		policies, err := readManifests(admissionFile)
		Expect(err).NotTo(HaveOccurred())
		policy := named(policies, policyKind, policyName)

		for _, v := range policy.Spec.Validations {
			if strings.Contains(v.Expression, "variables.newAllowed") {
				Expect(v.Expression).To(HavePrefix("variables.tokenRequest ||"))
			}
		}
	})

	It("matches Role and RoleBinding CREATE, UPDATE and DELETE, and denies with reason Forbidden", func() {
		policies, err := readManifests(admissionFile)
		Expect(err).NotTo(HaveOccurred())
		policy := named(policies, policyKind, rolesPolicyName)

		Expect(policy.Spec.MatchConstraints.ResourceRules).To(Equal([]resourceRule{{
			APIGroups:   []string{"rbac.authorization.k8s.io"},
			APIVersions: []string{"v1"},
			Operations:  []string{"CREATE", "UPDATE", "DELETE"},
			Resources:   []string{"roles", "rolebindings"},
		}}))

		Expect(policy.Spec.Validations).NotTo(BeEmpty())
		for _, v := range policy.Spec.Validations {
			Expect(v.Reason).To(Equal("Forbidden"))
			Expect(v.Message).To(HavePrefix(rolesPolicyName + ": "))
		}
	})
})
