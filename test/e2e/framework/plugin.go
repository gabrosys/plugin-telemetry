package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
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
	hookMessage              = "lifecycle hook"
)

var (
	// ErrRolloutTimedOut is returned when a Deployment exceeded its progress deadline.
	ErrRolloutTimedOut = errors.New("deployment exceeded its progress deadline")
	// ErrNoSidecarImage is returned when the sidecar image ConfigMap holds no image.
	ErrNoSidecarImage = errors.New("sidecar image configmap holds no image")
	// ErrInvalidHookLine is returned for a "lifecycle hook" log line without a valid timestamp.
	ErrInvalidHookLine = errors.New("invalid lifecycle hook log line")
)

// HookCall is one "lifecycle hook" line logged by the plugin.
type HookCall struct {
	Time      time.Time `json:"ts"`
	Operation string    `json:"operation"`
	Namespace string    `json:"namespace"`
	Cluster   string    `json:"cluster"`
	Pod       string    `json:"pod"`
	Outcome   string    `json:"outcome"`
	PluginPod string    `json:"pluginPod,omitempty"`
}

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

// WithExporter sets the plugin's exporter parameter, the TelemetryExporter the Cluster uses.
func WithExporter(cluster *apiv1.Cluster, exporter string) *apiv1.Cluster {
	plugin := &cluster.Spec.Plugins[0]
	if plugin.Parameters == nil {
		plugin.Parameters = map[string]string{}
	}
	plugin.Parameters["exporter"] = exporter

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

// PluginPods returns the names of the plugin Pods that are not being deleted, sorted.
func PluginPods(ctx context.Context, c client.Client) ([]string, error) {
	d, err := getPluginDeployment(ctx, c)
	if err != nil {
		return nil, err
	}

	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("selector of deployment %s/%s: %w", d.Namespace, d.Name, err)
	}

	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(d.Namespace),
		client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("list pods of deployment %s/%s: %w", d.Namespace, d.Name, err)
	}

	names := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil {
			names = append(names, pod.Name)
		}
	}
	slices.Sort(names)

	return names, nil
}

// HookCalls returns the lifecycle hook calls the plugin Pod logged after since.
func HookCalls(ctx context.Context, cfg *rest.Config, pod string, since time.Time) ([]HookCall, error) {
	// SinceTime has second precision; ParseHookCalls drops the earlier lines.
	sinceTime := metav1.NewTime(since)
	logs, err := ContainerLogs(ctx, cfg, types.NamespacedName{Namespace: operatorNamespace, Name: pod}, "",
		&corev1.PodLogOptions{SinceTime: &sinceTime})
	if err != nil {
		return nil, err
	}

	calls, err := ParseHookCalls(logs, since)
	if err != nil {
		return nil, fmt.Errorf("logs of pod %s/%s: %w", operatorNamespace, pod, err)
	}
	for i := range calls {
		calls[i].PluginPod = pod
	}

	return calls, nil
}

// ParseHookCalls returns the "lifecycle hook" lines of logs logged after
// since. Lines that are not JSON objects are skipped.
func ParseHookCalls(logs string, since time.Time) ([]HookCall, error) {
	var calls []HookCall
	for line := range strings.Lines(logs) {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}

		var entry struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("%w: decode log line %q: %w", ErrInvalidHookLine, line, err)
		}
		if entry.Msg != hookMessage {
			continue
		}

		var call HookCall
		if err := json.Unmarshal([]byte(line), &call); err != nil {
			return nil, fmt.Errorf("%w: %q: %w", ErrInvalidHookLine, line, err)
		}
		if call.Time.IsZero() {
			return nil, fmt.Errorf("%w: %q has no ts", ErrInvalidHookLine, line)
		}
		if call.Time.After(since) {
			calls = append(calls, call)
		}
	}

	return calls, nil
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
