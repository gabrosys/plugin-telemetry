package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
)

const (
	// PodSpecAnnotation holds the Pod spec CNPG built for an instance.
	PodSpecAnnotation = "cnpg.io/podSpec"

	clusterLabel    = "cnpg.io/cluster"
	podRoleLabel    = "cnpg.io/podRole"
	podRoleInstance = "instance"
	sidecarBinary   = "/plugin-telemetry"
	podPollInterval = 2 * time.Second
)

var (
	// ErrInvalidReport is returned when the preflight output is not exactly one report.
	ErrInvalidReport = errors.New("invalid preflight report")
	// ErrPodFailed is returned when a Pod run by RunPod ends in phase Failed.
	ErrPodFailed = errors.New("pod failed")
)

// InstancePods returns the PostgreSQL instance Pods of a Cluster.
func InstancePods(ctx context.Context, c client.Client, cluster client.ObjectKey) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		clusterLabel: cluster.Name,
		podRoleLabel: podRoleInstance,
	}); err != nil {
		return nil, fmt.Errorf("list instance pods of %s: %w", cluster, err)
	}

	return pods.Items, nil
}

// Preflight runs `/plugin-telemetry instance preflight <args>` in the sidecar and decodes the report.
func Preflight(ctx context.Context, cfg *rest.Config, pod types.NamespacedName, args ...string) (preflight.Report, error) {
	command := append([]string{sidecarBinary, "instance", "preflight"}, args...)
	stdout, stderr, err := Exec(ctx, cfg, pod, metadata.SidecarContainerName, command)
	if err != nil {
		return preflight.Report{}, fmt.Errorf("preflight in %s (stderr %q): %w", pod, stderr, err)
	}

	rep, err := DecodeReport(stdout)
	if err != nil {
		return preflight.Report{}, fmt.Errorf("preflight in %s: %w", pod, err)
	}

	return rep, nil
}

// DecodeReport decodes the single JSON report printed by `instance preflight`.
func DecodeReport(out string) (preflight.Report, error) {
	dec := json.NewDecoder(strings.NewReader(out))
	dec.DisallowUnknownFields()

	var rep preflight.Report
	if err := dec.Decode(&rep); err != nil {
		return preflight.Report{}, fmt.Errorf("%w: %w: %q", ErrInvalidReport, err, out)
	}
	if dec.More() {
		return preflight.Report{}, fmt.Errorf("%w: more than one document: %q", ErrInvalidReport, out)
	}

	return rep, nil
}

// RunPod creates pod, waits until it ends, and returns its logs. The Pod must
// have one container. A Pod that ends in phase Failed returns its logs and ErrPodFailed.
func RunPod(ctx context.Context, c client.Client, cfg *rest.Config, pod *corev1.Pod) (string, error) {
	if err := c.Create(ctx, pod); err != nil {
		return "", fmt.Errorf("create pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	key := client.ObjectKeyFromObject(pod)
	var phase corev1.PodPhase
	err := wait.PollUntilContextCancel(ctx, podPollInterval, true, func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		if err := c.Get(ctx, key, &p); err != nil {
			return false, fmt.Errorf("get pod %s: %w", key, err)
		}
		phase = p.Status.Phase

		return phase == corev1.PodSucceeded || phase == corev1.PodFailed, nil
	})
	if err != nil {
		return "", fmt.Errorf("wait for pod %s to end (phase %q): %w", key, phase, err)
	}

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("create clientset: %w", err)
	}
	logs, err := cs.CoreV1().Pods(key.Namespace).GetLogs(key.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		return "", fmt.Errorf("get logs of pod %s: %w", key, err)
	}

	if phase == corev1.PodFailed {
		return string(logs), fmt.Errorf("pod %s: %w", key, ErrPodFailed)
	}

	return string(logs), nil
}
