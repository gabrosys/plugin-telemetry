package policy_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

var _ = Describe("the plugin Deployment", func() {
	It("sleeps 5 s in preStop and sets a 20 s grace period", func() {
		d := readDeployment()

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
		var pc schedulingv1.PriorityClass
		Expect(yaml.UnmarshalStrict(readRepoFile("config/priorityclass.yaml"), &pc)).To(Succeed())

		Expect(pc.Name).To(Equal("plugin-telemetry"))
		Expect(pc.Value).To(BeEquivalentTo(1000000))
		Expect(pc.PreemptionPolicy).To(HaveValue(Equal(corev1.PreemptLowerPriority)))
		Expect(pc.GlobalDefault).To(BeFalse())
		Expect(pc.Description).To(ContainSubstring("cluster-wide choice"))
	})

	It("runs the plugin Pods with that PriorityClass", func() {
		d := readDeployment()
		Expect(d.Spec.Template.Spec.PriorityClassName).To(Equal("plugin-telemetry"))

		var k struct {
			Resources []string `json:"resources"`
		}
		Expect(yaml.Unmarshal(readRepoFile("config/kustomization.yaml"), &k)).To(Succeed())
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
		SecretName string   `json:"secretName"`
		IsCA       bool     `json:"isCA"`
		Usages     []string `json:"usages"`
		DNSNames   []string `json:"dnsNames"`
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

func readRepoFile(path string) []byte {
	GinkgoHelper()

	return fixture.Read(GinkgoTB(), repoRoot, path)
}

func readDeployment() appsv1.Deployment {
	GinkgoHelper()

	var d appsv1.Deployment
	Expect(yaml.UnmarshalStrict(readRepoFile("config/deployment.yaml"), &d)).To(Succeed())

	return d
}

func readDocs(path string) map[string]certDoc {
	GinkgoHelper()

	docs := map[string]certDoc{}
	reader := yamlutil.NewYAMLReader(bufio.NewReader(bytes.NewReader(readRepoFile(path))))
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

	It("limits the server leaf to server auth and the client leaf to client auth without a DNS name", func() {
		docs := readDocs("config/certificates.yaml")

		server := docs["Certificate/plugin-telemetry-server"]
		Expect(server.Spec.Usages).To(ConsistOf("server auth"))
		Expect(server.Spec.DNSNames).To(ConsistOf("plugin-telemetry", "plugin-telemetry.cnpg-system.svc"))

		client := docs["Certificate/plugin-telemetry-client"]
		Expect(client.Spec.Usages).To(ConsistOf("client auth"))
		Expect(client.Spec.DNSNames).To(BeEmpty())
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
		d := readDeployment()

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
