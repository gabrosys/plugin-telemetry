package framework

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const postgresUID = 26

// PgbenchPod returns a Pod that initialises pgbench at scale 1 and runs it for seconds against <cluster>-rw
// with the credentials of Secret <cluster>-app; the Secret is read through the environment, never printed.
func PgbenchPod(namespace, name, cluster, image string, seconds int) *corev1.Pod {
	secretEnv := func(env, key string) corev1.EnvVar {
		return corev1.EnvVar{Name: env, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: cluster + "-app"},
			Key:                  key,
		}}}
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr.To(false),
			Containers: []corev1.Container{{
				Name:            "pgbench",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"sh", "-c", fmt.Sprintf(
					"set -e; pgbench -i -s 1; exec pgbench -T %d -c 2 -j 1 -P 5", seconds)},
				Env: []corev1.EnvVar{
					{Name: "PGHOST", Value: cluster + "-rw"},
					secretEnv("PGUSER", "username"),
					secretEnv("PGPASSWORD", "password"),
					secretEnv("PGDATABASE", "dbname"),
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot:             ptr.To(true),
					RunAsUser:                ptr.To[int64](postgresUID),
					AllowPrivilegeEscalation: ptr.To(false),
					ReadOnlyRootFilesystem:   ptr.To(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}
}
