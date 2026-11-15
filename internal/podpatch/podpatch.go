package podpatch

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

const scratchSubPath = "run"

// Apply upserts the sidecar and its volumes by name. It reads nothing but its arguments.
func Apply(pod *corev1.Pod, cluster string, p Params, image string) error {
	if p.Auth == AuthPeer && !hasVolume(pod.Spec.Volumes, metadata.ScratchVolume) {
		return fmt.Errorf("peer mode: %w", ErrNoScratchVolume)
	}

	pod.Spec.Volumes = upsert(pod.Spec.Volumes, corev1.Volume{
		Name: metadata.TokenVolume,
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: metadata.TokenSecretName(cluster),
			Optional:   ptr.To(true),
		}},
	}, func(v corev1.Volume) string { return v.Name })

	pod.Spec.Volumes = upsert(pod.Spec.Volumes, corev1.Volume{
		Name:         metadata.TokenMaskVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}, func(v corev1.Volume) string { return v.Name })

	pod.Spec.InitContainers = upsert(pod.Spec.InitContainers, sidecar(cluster, p, image),
		func(c corev1.Container) string { return c.Name })

	return nil
}

func sidecar(cluster string, p Params, image string) corev1.Container {
	mounts := make([]corev1.VolumeMount, 0, 3)
	if p.Auth == AuthPeer {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      metadata.ScratchVolume,
			MountPath: metadata.SocketDir,
			SubPath:   scratchSubPath,
			ReadOnly:  true,
		})
	}
	mounts = append(mounts,
		corev1.VolumeMount{Name: metadata.TokenVolume, MountPath: metadata.SidecarTokenDir, ReadOnly: true},
		corev1.VolumeMount{Name: metadata.TokenMaskVolume, MountPath: metadata.PodTokenDir, ReadOnly: true},
	)

	sc := &corev1.SecurityContext{
		RunAsNonRoot:             ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if p.Auth == AuthPeer {
		sc.RunAsUser = ptr.To(metadata.PeerUID)
	}

	return corev1.Container{
		Name:            metadata.SidecarContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            []string{"instance"},
		RestartPolicy:   ptr.To(corev1.ContainerRestartPolicyAlways),
		Env: []corev1.EnvVar{
			fieldEnv("POD_NAME", "metadata.name"),
			fieldEnv("POD_UID", "metadata.uid"),
			fieldEnv("NODE_NAME", "spec.nodeName"),
			fieldEnv("NAMESPACE", "metadata.namespace"),
			{Name: "CLUSTER_NAME", Value: cluster},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: p.CPU.DeepCopy(), corev1.ResourceMemory: p.Memory.DeepCopy()},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: p.CPU.DeepCopy(), corev1.ResourceMemory: p.Memory.DeepCopy()},
		},
		SecurityContext: sc,
		VolumeMounts:    mounts,
	}
}

func fieldEnv(name, path string) corev1.EnvVar {
	return corev1.EnvVar{
		Name:      name,
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}},
	}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, v := range volumes {
		if v.Name == name {
			return true
		}
	}

	return false
}

// upsert replaces the element with the same name in place, or appends it.
func upsert[T any](items []T, item T, name func(T) string) []T {
	for i := range items {
		if name(items[i]) == name(item) {
			items[i] = item

			return items
		}
	}

	return append(items, item)
}
