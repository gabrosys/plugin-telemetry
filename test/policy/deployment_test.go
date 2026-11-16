package policy_test

import (
	"io/fs"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
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
