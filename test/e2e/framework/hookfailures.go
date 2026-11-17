package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// failedHookMessages are the CloudNativePG v1.30.1 messages of a plugin error that fails a Cluster reconcile
// (internal/controller/cluster_controller.go:247-286,403, internal/cnpi/plugin/client/lifecycle.go:133,
// internal/controller/plugins.go:118, internal/cnpi/plugin/client/cluster.go:139).
var failedHookMessages = []string{
	"Error loading plugins, retrying",
	"Pre-reconcile hook stopped the reconciliation loop",
	"Error while calling LifecycleHook",
	"Post-reconcile hook returned an error",
	"Error while calling SetStatusInCluster",
}

// FailedHookCalls returns the operator log lines after since that report a failed plugin call for namespace.
func FailedHookCalls(ctx context.Context, cfg *rest.Config, namespace string, since time.Time) ([]string, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	deploy, err := cs.AppsV1().Deployments(operatorNamespace).Get(ctx, operatorDeployment, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get deployment %s/%s: %w", operatorNamespace, operatorDeployment, err)
	}
	selector, err := metav1.LabelSelectorAsSelector(deploy.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("selector of deployment %s/%s: %w", operatorNamespace, operatorDeployment, err)
	}
	pods, err := cs.CoreV1().Pods(operatorNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("list operator pods: %w", err)
	}

	// SinceTime has second precision; ParseFailedHookCalls drops the earlier lines.
	sinceTime := metav1.NewTime(since)
	var calls []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if err := CheckOperatorRestart(pod, since); err != nil {
			return nil, err
		}
		key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		logs, err := ContainerLogs(ctx, cfg, key, operatorContainer, &corev1.PodLogOptions{SinceTime: &sinceTime})
		if err != nil {
			return nil, err
		}
		calls = append(calls, ParseFailedHookCalls(logs, namespace, since)...)
	}

	return calls, nil
}

// ErrOperatorRestarted is returned when the operator container terminated after the window started.
var ErrOperatorRestarted = errors.New("operator container restarted")

// CheckOperatorRestart fails when pod's manager container terminated after since: the log of its earlier
// container is gone, so a failed hook call in the window could be missing.
func CheckOperatorRestart(pod *corev1.Pod, since time.Time) error {
	for _, st := range pod.Status.ContainerStatuses {
		if st.Name != operatorContainer {
			continue
		}
		for _, term := range []*corev1.ContainerStateTerminated{st.LastTerminationState.Terminated, st.State.Terminated} {
			if term != nil && term.FinishedAt.After(since) {
				return fmt.Errorf("%w: the CNPG operator container %s restarted at %s; its earlier log is gone",
					ErrOperatorRestarted, pod.Name, term.FinishedAt.Format(time.RFC3339))
			}
		}
	}

	return nil
}

// ParseFailedHookCalls returns the JSON lines of logs with a ts after since, namespace equal to namespace and
// one of the failed-hook messages. Lines that are not JSON are skipped.
func ParseFailedHookCalls(logs, namespace string, since time.Time) []string {
	var calls []string
	for line := range strings.Lines(logs) {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}

		var entry struct {
			Time      time.Time `json:"ts"`
			Namespace string    `json:"namespace"`
			Msg       string    `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		if entry.Time.After(since) && entry.Namespace == namespace && slices.Contains(failedHookMessages, entry.Msg) {
			calls = append(calls, line)
		}
	}

	return calls
}
