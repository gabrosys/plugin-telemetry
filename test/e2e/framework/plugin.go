package framework

import (
	"context"
	"errors"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

const (
	pluginDeployment         = "plugin-telemetry"
	sidecarImageConfigMap    = "telemetry-sidecar-image"
	sidecarImageKey          = "image"
	restartedAtAnnotation    = "kubectl.kubernetes.io/restartedAt"
	progressDeadlineExceeded = "ProgressDeadlineExceeded"
	peerIdentLine            = "cnpg_metrics_exporter nobody cnpg_metrics_exporter"
)

var (
	// ErrRolloutTimedOut is returned when a Deployment exceeded its progress deadline.
	ErrRolloutTimedOut = errors.New("deployment exceeded its progress deadline")
	// ErrNoSidecarImage is returned when the sidecar image ConfigMap holds no image.
	ErrNoSidecarImage = errors.New("sidecar image configmap holds no image")
)

// PeerCluster adds our plugin and the pg_ident line "cnpg_metrics_exporter nobody cnpg_metrics_exporter".
func PeerCluster(namespace, name, image string) *apiv1.Cluster {
	cluster := NewCluster(namespace, name, image)
	cluster.Spec.Plugins = []apiv1.PluginConfiguration{{Name: metadata.PluginName}}
	cluster.Spec.PostgresConfiguration.PgIdent = []string{peerIdentLine}

	return cluster
}

// PasswordCluster adds the plugin with auth=password, the managed role (login, inRoles pg_monitor,
// passwordSecret) and pg_hba lines for 127.0.0.1/32 and ::1/128 with scram-sha-256, then "all reject".
func PasswordCluster(namespace, name, image, role, passwordSecret string) *apiv1.Cluster {
	cluster := NewCluster(namespace, name, image)
	cluster.Spec.Plugins = []apiv1.PluginConfiguration{{
		Name:       metadata.PluginName,
		Parameters: map[string]string{"auth": "password", "passwordSecret": passwordSecret},
	}}
	cluster.Spec.Managed = &apiv1.ManagedConfiguration{Roles: []apiv1.RoleConfiguration{{
		Name:           role,
		Ensure:         apiv1.EnsurePresent,
		Login:          true,
		InRoles:        []string{"pg_monitor"},
		PasswordSecret: &apiv1.LocalObjectReference{Name: passwordSecret},
	}}}
	cluster.Spec.PostgresConfiguration.PgHBA = []string{
		"host all " + role + " 127.0.0.1/32 scram-sha-256",
		"host all " + role + " ::1/128 scram-sha-256",
		"host all " + role + " all reject",
	}

	return cluster
}

// SidecarImage returns the image held by the telemetry-sidecar-image ConfigMap.
func SidecarImage(ctx context.Context, c client.Client) (string, error) {
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: operatorNamespace, Name: sidecarImageConfigMap}
	if err := c.Get(ctx, key, &cm); err != nil {
		return "", fmt.Errorf("get configmap %s: %w", key, err)
	}

	image := cm.Data[sidecarImageKey]
	if image == "" {
		return "", fmt.Errorf("configmap %s key %q: %w", key, sidecarImageKey, ErrNoSidecarImage)
	}

	return image, nil
}

// PluginAvailable reports whether the plugin Deployment has the Available condition.
func PluginAvailable(ctx context.Context, c client.Client) (bool, error) {
	d, err := getPluginDeployment(ctx, c)
	if err != nil {
		return false, err
	}

	for _, cond := range d.Status.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			return cond.Status == corev1.ConditionTrue, nil
		}
	}

	return false, nil
}

// PluginRolledOut reports whether the plugin Deployment finished its rollout.
func PluginRolledOut(ctx context.Context, c client.Client) (bool, error) {
	d, err := getPluginDeployment(ctx, c)
	if err != nil {
		return false, err
	}

	return RolloutComplete(d)
}

// RestartPlugin is `kubectl rollout restart` on the plugin Deployment.
func RestartPlugin(ctx context.Context, c client.Client) error {
	d, err := getPluginDeployment(ctx, c)
	if err != nil {
		return err
	}

	patch := client.MergeFrom(d.DeepCopy())
	if d.Spec.Template.Annotations == nil {
		d.Spec.Template.Annotations = map[string]string{}
	}
	d.Spec.Template.Annotations[restartedAtAnnotation] = time.Now().Format(time.RFC3339)
	if err := c.Patch(ctx, d, patch); err != nil {
		return fmt.Errorf("restart deployment %s/%s: %w", d.Namespace, d.Name, err)
	}

	return nil
}

// RolloutComplete reports whether d finished its rollout, as `kubectl rollout status` does.
func RolloutComplete(d *appsv1.Deployment) (bool, error) {
	if d.Generation > d.Status.ObservedGeneration {
		return false, nil
	}

	for _, cond := range d.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing && cond.Reason == progressDeadlineExceeded {
			return false, fmt.Errorf("deployment %s/%s: %w", d.Namespace, d.Name, ErrRolloutTimedOut)
		}
	}

	switch {
	case d.Spec.Replicas != nil && d.Status.UpdatedReplicas < *d.Spec.Replicas:
		return false, nil
	case d.Status.Replicas > d.Status.UpdatedReplicas:
		return false, nil
	case d.Status.AvailableReplicas < d.Status.UpdatedReplicas:
		return false, nil
	}

	return true, nil
}

func getPluginDeployment(ctx context.Context, c client.Client) (*appsv1.Deployment, error) {
	var d appsv1.Deployment
	key := client.ObjectKey{Namespace: operatorNamespace, Name: pluginDeployment}
	if err := c.Get(ctx, key, &d); err != nil {
		return nil, fmt.Errorf("get deployment %s: %w", key, err)
	}

	return &d, nil
}
