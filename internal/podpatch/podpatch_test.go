package podpatch_test

import (
	"encoding/json"
	"os"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

const (
	cluster = "fixture"
	image   = "ghcr.io/gabrosys/plugin-telemetry@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func testdata() *os.Root {
	GinkgoHelper()

	root, err := os.OpenRoot("testdata")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)

	return root
}

func loadPod(fixture string) *corev1.Pod {
	GinkgoHelper()

	data, err := testdata().ReadFile(fixture + ".json")
	Expect(err).NotTo(HaveOccurred())

	pod := &corev1.Pod{}
	Expect(json.Unmarshal(data, pod)).To(Succeed())

	return pod
}

func params(auth podpatch.AuthMode) podpatch.Params {
	return podpatch.Params{Auth: auth, CPU: resource.MustParse("50m"), Memory: resource.MustParse("64Mi")}
}

func marshal(pod *corev1.Pod) []byte {
	GinkgoHelper()

	out, err := json.MarshalIndent(pod, "", "  ")
	Expect(err).NotTo(HaveOccurred())

	return append(out, '\n')
}

func sidecarOf(pod *corev1.Pod) corev1.Container {
	GinkgoHelper()

	var found []corev1.Container
	for _, c := range pod.Spec.InitContainers {
		if c.Name == metadata.SidecarContainerName {
			found = append(found, c)
		}
	}
	Expect(found).To(HaveLen(1))

	return found[0]
}

var _ = Describe("Apply", func() {
	for _, fixture := range []string{"pod-v1.30.1", "pod-main"} {
		Context("on "+fixture, func() {
			var original, pod *corev1.Pod

			BeforeEach(func() {
				original = loadPod(fixture)
				pod = original.DeepCopy()
			})

			It("adds the sidecar in peer mode", func() {
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())

				Expect(pod.Spec.InitContainers).To(HaveLen(len(original.Spec.InitContainers) + 1))
				c := pod.Spec.InitContainers[len(pod.Spec.InitContainers)-1]
				Expect(c.Name).To(Equal("otel-metrics"))
				Expect(c.RestartPolicy).To(HaveValue(Equal(corev1.ContainerRestartPolicyAlways)))
				Expect(c.Image).To(Equal(image))
				Expect(c.ImagePullPolicy).To(Equal(corev1.PullIfNotPresent))
				Expect(c.Args).To(Equal([]string{"instance"}))
				Expect(c.StartupProbe).To(BeNil())
				Expect(c.ReadinessProbe).To(BeNil())
				Expect(c.LivenessProbe).To(BeNil())

				Expect(c.Env).To(Equal([]corev1.EnvVar{
					{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
					{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
					{Name: "NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
					{Name: "CLUSTER_NAME", Value: cluster},
				}))

				Expect(c.Resources.Requests).To(HaveLen(2))
				Expect(c.Resources.Limits).To(HaveLen(2))
				for _, list := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
					cpu, mem := list[corev1.ResourceCPU], list[corev1.ResourceMemory]
					Expect(cpu.Cmp(resource.MustParse("50m"))).To(BeZero())
					Expect(mem.Cmp(resource.MustParse("64Mi"))).To(BeZero())
				}

				Expect(c.SecurityContext).To(Equal(&corev1.SecurityContext{
					RunAsNonRoot:             ptr.To(true),
					RunAsUser:                ptr.To(int64(65534)),
					AllowPrivilegeEscalation: ptr.To(false),
					ReadOnlyRootFilesystem:   ptr.To(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				}))

				Expect(c.VolumeMounts).To(Equal([]corev1.VolumeMount{
					{Name: "scratch-data", MountPath: "/controller/run", SubPath: "run", ReadOnly: true},
					{Name: "telemetry-token", MountPath: metadata.SidecarTokenDir, ReadOnly: true},
					{Name: "telemetry-sa-mask", MountPath: metadata.PodTokenDir, ReadOnly: true},
				}))

				Expect(pod.Spec.Volumes).To(HaveLen(len(original.Spec.Volumes) + 2))
				Expect(pod.Spec.Volumes[len(pod.Spec.Volumes)-2:]).To(Equal([]corev1.Volume{
					{Name: "telemetry-token", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{ //nolint:gosec // a Secret name, not a credential
						SecretName: "fixture-telemetry-token",
						Optional:   ptr.To(true),
					}}},
					{Name: "telemetry-sa-mask", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				}))
			})

			It("leaves the rest of the Pod unchanged", func() {
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())

				pod.Spec.InitContainers = slices.DeleteFunc(pod.Spec.InitContainers, func(c corev1.Container) bool {
					return c.Name == metadata.SidecarContainerName
				})
				pod.Spec.Volumes = slices.DeleteFunc(pod.Spec.Volumes, func(v corev1.Volume) bool {
					return v.Name == "telemetry-token" || v.Name == "telemetry-sa-mask"
				})
				Expect(pod).To(Equal(original))
			})

			It("mounts neither kube-api-access nor PGDATA", func() {
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())

				for _, m := range sidecarOf(pod).VolumeMounts {
					Expect(m.Name).NotTo(Or(Equal("kube-api-access"), Equal("pgdata")))
				}
			})

			It("adapts to password mode", func() {
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPassword), image)).To(Succeed())

				c := sidecarOf(pod)
				Expect(c.SecurityContext.RunAsUser).To(BeNil())
				Expect(c.SecurityContext.RunAsNonRoot).To(HaveValue(BeTrue()))
				Expect(c.VolumeMounts).To(Equal([]corev1.VolumeMount{
					{Name: "telemetry-token", MountPath: metadata.SidecarTokenDir, ReadOnly: true},
					{Name: "telemetry-sa-mask", MountPath: metadata.PodTokenDir, ReadOnly: true},
				}))
			})

			It("is idempotent", func() {
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())
				first := marshal(pod)

				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())
				Expect(marshal(pod)).To(Equal(first))
				sidecarOf(pod)
			})

			It("is deterministic", func() {
				other := original.DeepCopy()
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())
				Expect(podpatch.Apply(other, cluster, params(podpatch.AuthPeer), image)).To(Succeed())
				Expect(marshal(other)).To(Equal(marshal(pod)))
			})

			It("replaces a stale sidecar in place", func() {
				stale := corev1.Container{Name: metadata.SidecarContainerName, Image: "example.com/old:1"}
				after := corev1.Container{Name: "after", Image: "example.com/after:1"}
				pod.Spec.InitContainers = append(pod.Spec.InitContainers, stale, after)
				position := len(original.Spec.InitContainers)

				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPeer), image)).To(Succeed())

				Expect(pod.Spec.InitContainers).To(HaveLen(len(original.Spec.InitContainers) + 2))
				Expect(pod.Spec.InitContainers[position].Name).To(Equal(metadata.SidecarContainerName))
				Expect(pod.Spec.InitContainers[position].Image).To(Equal(image))
				Expect(pod.Spec.InitContainers[position+1].Name).To(Equal("after"))
			})

			It("needs scratch-data in peer mode only", func() {
				pod.Spec.Volumes = slices.DeleteFunc(pod.Spec.Volumes, func(v corev1.Volume) bool {
					return v.Name == "scratch-data"
				})

				Expect(podpatch.Apply(pod.DeepCopy(), cluster, params(podpatch.AuthPeer), image)).To(MatchError(podpatch.ErrNoScratchVolume))
				Expect(podpatch.Apply(pod, cluster, params(podpatch.AuthPassword), image)).To(Succeed())
			})

			for _, mode := range []podpatch.AuthMode{podpatch.AuthPeer, podpatch.AuthPassword} {
				It("matches the golden Pod in "+string(mode)+" mode", func() {
					Expect(podpatch.Apply(pod, cluster, params(mode), image)).To(Succeed())

					root := testdata()
					golden := fixture + "." + string(mode) + ".golden.json"
					if os.Getenv("UPDATE_GOLDEN") != "" {
						Expect(root.WriteFile(golden, marshal(pod), 0o600)).To(Succeed())
					}

					want, err := root.ReadFile(golden)
					Expect(err).NotTo(HaveOccurred())
					Expect(string(marshal(pod))).To(Equal(string(want)))
				})
			}
		})
	}
})
