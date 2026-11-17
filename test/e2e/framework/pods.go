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

// DecodeReportFromLogs finds the one line of combined stdout and stderr that decodes strictly as a Report (ErrInvalidReport for none or two).
func DecodeReportFromLogs(logs string) (preflight.Report, error) {
	var (
		found preflight.Report
		count int
	)
	for line := range strings.Lines(logs) {
		rep, err := DecodeReport(line)
		if err != nil {
			continue
		}
		found = rep
		count++
	}

	if count != 1 {
		return preflight.Report{}, fmt.Errorf("%w: %d reports in the logs: %q", ErrInvalidReport, count, logs)
	}

	return found, nil
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

	logs, err := ContainerLogs(ctx, cfg, key, "", &corev1.PodLogOptions{})
	if err != nil {
		return "", err
	}

	if phase == corev1.PodFailed {
		return logs, fmt.Errorf("pod %s: %w", key, ErrPodFailed)
	}

	return logs, nil
}

// ContainerLogs returns a container's logs; container may be empty for a Pod with one container.
func ContainerLogs(
	ctx context.Context, cfg *rest.Config, pod types.NamespacedName, container string, opts *corev1.PodLogOptions,
) (string, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("create clientset: %w", err)
	}

	o := opts.DeepCopy()
	o.Container = container
	logs, err := cs.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, o).DoRaw(ctx)
	if err != nil {
		return "", fmt.Errorf("get logs of pod %s container %q: %w", pod, container, err)
	}

	return string(logs), nil
}

// SidecarRestarts returns the sidecar's restart count, which CNPG's native sidecar reports in initContainerStatuses.
func SidecarRestarts(pod *corev1.Pod) int32 {
	for _, st := range pod.Status.InitContainerStatuses {
		if st.Name == metadata.SidecarContainerName {
			return st.RestartCount
		}
	}

	return 0
}
