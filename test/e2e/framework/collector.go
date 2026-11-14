package framework

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	collectorNamespace  = "telemetry-e2e"
	collectorName       = "collector"
	collectorReader     = "reader"
	collectorFile       = "/data/metrics.jsonl"
	collectorReadyPoll  = 2 * time.Second
	collectorReadyLimit = 3 * time.Minute
)

// ErrNoCollectorPod is returned when the collector Deployment has no running Pod.
var ErrNoCollectorPod = errors.New("no running collector pod")

// CollectorReady waits until the collector Deployment that make deploy applied is Ready.
func CollectorReady(ctx context.Context, c client.Client) error {
	ctx, cancel := context.WithTimeout(ctx, collectorReadyLimit)
	defer cancel()

	key := client.ObjectKey{Namespace: collectorNamespace, Name: collectorName}
	err := wait.PollUntilContextCancel(ctx, collectorReadyPoll, true, func(ctx context.Context) (bool, error) {
		var d appsv1.Deployment
		if err := c.Get(ctx, key, &d); err != nil {
			return false, fmt.Errorf("get deployment %s: %w", key, err)
		}

		return RolloutComplete(&d)
	})
	if err != nil {
		return fmt.Errorf("wait for deployment %s: %w", key, err)
	}

	return nil
}

// CollectorLines reads the last n lines of the collector's file through the reader container.
func CollectorLines(ctx context.Context, cfg *rest.Config, n int) ([]otlp.Line, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	pods, err := cs.CoreV1().Pods(collectorNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=" + collectorName,
	})
	if err != nil {
		return nil, fmt.Errorf("list collector pods: %w", err)
	}

	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}

		out, _, err := Exec(ctx, cfg, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, collectorReader,
			[]string{"tail", "-n", strconv.Itoa(n), collectorFile})
		if err != nil {
			return nil, fmt.Errorf("read the collector file: %w", err)
		}

		return ParseLines(out)
	}

	return nil, fmt.Errorf("%w in %s", ErrNoCollectorPod, collectorNamespace)
}

// ParseLines decodes the output of tail on the collector's file. A last line without a newline is skipped.
func ParseLines(out string) ([]otlp.Line, error) {
	var l otlp.Lines
	if err := l.Update([]byte(out)); err != nil {
		return nil, fmt.Errorf("decode the collector file: %w", err)
	}

	return l.All(), nil
}
