package policy_test

import (
	"io/fs"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
