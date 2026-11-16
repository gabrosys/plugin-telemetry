package policy_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

var _ = Describe("the plugin Deployment", func() {
	It("sleeps 5 s in preStop and leaves the drain room in the grace period", func() {
		raw, err := fs.ReadFile(os.DirFS(repoRoot), "config/deployment.yaml")
		Expect(err).NotTo(HaveOccurred())
		var d appsv1.Deployment
		Expect(yaml.UnmarshalStrict(raw, &d)).To(Succeed())

		var container *corev1.Container
		for i := range d.Spec.Template.Spec.Containers {
			if d.Spec.Template.Spec.Containers[i].Name == "plugin-telemetry" {
				container = &d.Spec.Template.Spec.Containers[i]
			}
		}
		Expect(container).NotTo(BeNil(), "no plugin-telemetry container")
		Expect(container.Lifecycle).NotTo(BeNil(), "no lifecycle")
		Expect(container.Lifecycle.PreStop).NotTo(BeNil(), "no preStop")
		Expect(container.Lifecycle.PreStop.Sleep).To(Equal(&corev1.SleepAction{Seconds: 5}))
		Expect(d.Spec.Template.Spec.TerminationGracePeriodSeconds).To(HaveValue(BeEquivalentTo(20)))
	})
})

var _ = Describe("the plugin PriorityClass", func() {
	It("defines the plugin-telemetry PriorityClass at 1000000 with preemption, not the global default", func() {
		raw, err := fs.ReadFile(os.DirFS(repoRoot), "config/priorityclass.yaml")
		Expect(err).NotTo(HaveOccurred())
		var pc schedulingv1.PriorityClass
		Expect(yaml.UnmarshalStrict(raw, &pc)).To(Succeed())

		Expect(pc.Name).To(Equal("plugin-telemetry"))
		Expect(pc.Value).To(BeEquivalentTo(1000000))
		Expect(pc.PreemptionPolicy).To(HaveValue(Equal(corev1.PreemptLowerPriority)))
		Expect(pc.GlobalDefault).To(BeFalse())
		Expect(pc.Description).To(ContainSubstring("cluster-wide choice"))
	})

	It("runs the plugin Pods with that PriorityClass", func() {
		raw, err := fs.ReadFile(os.DirFS(repoRoot), "config/deployment.yaml")
		Expect(err).NotTo(HaveOccurred())
		var d appsv1.Deployment
		Expect(yaml.UnmarshalStrict(raw, &d)).To(Succeed())
		Expect(d.Spec.Template.Spec.PriorityClassName).To(Equal("plugin-telemetry"))

		kraw, err := fs.ReadFile(os.DirFS(repoRoot), "config/kustomization.yaml")
		Expect(err).NotTo(HaveOccurred())
		var k struct {
			Resources []string `json:"resources"`
		}
		Expect(yaml.Unmarshal(kraw, &k)).To(Succeed())
		Expect(k.Resources).To(ContainElement("priorityclass.yaml"))
	})
})

// certDoc is the part of a cert-manager Certificate or Issuer, and of a Service, that the chain specs read.
type certDoc struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		SecretName string `json:"secretName"`
		IsCA       bool   `json:"isCA"`
		IssuerRef  struct {
			Name string `json:"name"`
			Kind string `json:"kind"`
		} `json:"issuerRef"`
		PrivateKey struct {
			RotationPolicy string `json:"rotationPolicy"`
		} `json:"privateKey"`
		CA struct {
			SecretName string `json:"secretName"`
		} `json:"ca"`
		SelfSigned *struct{} `json:"selfSigned"`
	} `json:"spec"`
}

func readDocs(path string) map[string]certDoc {
	GinkgoHelper()

	raw, err := fs.ReadFile(os.DirFS(repoRoot), path)
	Expect(err).NotTo(HaveOccurred())

	docs := map[string]certDoc{}
	reader := yamlutil.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		Expect(err).NotTo(HaveOccurred())

		var d certDoc
		Expect(yaml.Unmarshal(doc, &d)).To(Succeed())
		docs[d.Kind+"/"+d.Metadata.Name] = d
	}

	return docs
}

var _ = Describe("the plugin certificates", func() {
	It("issues both leaves from a CA that keeps its key", func() {
		docs := readDocs("config/certificates.yaml")

		Expect(docs).To(HaveKey("Issuer/plugin-telemetry-selfsigned"))
		Expect(docs["Issuer/plugin-telemetry-selfsigned"].Spec.SelfSigned).NotTo(BeNil())

		ca := docs["Certificate/plugin-telemetry-ca"]
		Expect(ca.Spec.IsCA).To(BeTrue())
		Expect(ca.Spec.PrivateKey.RotationPolicy).To(Equal("Never"))
		Expect(ca.Spec.IssuerRef.Name).To(Equal("plugin-telemetry-selfsigned"))

		caIssuer := docs["Issuer/plugin-telemetry-ca"]
		Expect(caIssuer.Spec.CA.SecretName).To(Equal(ca.Spec.SecretName))

		for _, name := range []string{"plugin-telemetry-server", "plugin-telemetry-client"} {
			leaf := docs["Certificate/"+name]
			Expect(leaf.Spec.IsCA).To(BeFalse(), name)
			Expect(leaf.Spec.IssuerRef.Name).To(Equal("plugin-telemetry-ca"), name)
			Expect(leaf.Spec.IssuerRef.Kind).To(Equal("Issuer"), name)
			Expect(leaf.Spec.PrivateKey.RotationPolicy).To(Equal("Never"), name)
		}
	})

	It("names the CA Secret as the server Secret and the client leaf Secret as the client Secret", func() {
		certs := readDocs("config/certificates.yaml")
		svc := readDocs("config/service.yaml")["Service/plugin-telemetry"]

		Expect(svc.Metadata.Annotations).To(HaveKeyWithValue("cnpg.io/pluginServerSecret",
			certs["Certificate/plugin-telemetry-ca"].Spec.SecretName))
		Expect(svc.Metadata.Annotations).To(HaveKeyWithValue("cnpg.io/pluginClientSecret",
			certs["Certificate/plugin-telemetry-client"].Spec.SecretName))
	})

	It("trusts the CA of the client leaf Secret for client certificates", func() {
		raw, err := fs.ReadFile(os.DirFS(repoRoot), "config/deployment.yaml")
		Expect(err).NotTo(HaveOccurred())
		var d appsv1.Deployment
		Expect(yaml.UnmarshalStrict(raw, &d)).To(Succeed())

		Expect(d.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--client-cert=/client/ca.crt"))
		var items []corev1.KeyToPath
		for _, v := range d.Spec.Template.Spec.Volumes {
			if v.Name == "client-tls" {
				items = v.Secret.Items
			}
		}
		Expect(items).To(ContainElement(corev1.KeyToPath{Key: "ca.crt", Path: "ca.crt"}))
	})
})
