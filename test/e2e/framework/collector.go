package framework

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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

// CollectorReader reads the file the collector's file exporter writes. Rotation is off and the exporter appends,
// so each Pod has one file that only grows and every line is read once.
type CollectorReader struct {
	cfg *rest.Config

	mu      sync.Mutex // guards cs, offsets and lines
	cs      kubernetes.Interface
	offsets map[types.UID]int
	lines   []otlp.Line

	// pods lists the collector Pods and read returns a Pod's file from a byte offset; tests replace them.
	pods func(ctx context.Context) ([]corev1.Pod, error)
	read func(ctx context.Context, pod *corev1.Pod, offset int) (string, error)
}

// NewCollectorReader returns a reader of the collector Pods' files.
func NewCollectorReader(cfg *rest.Config) *CollectorReader {
	r := &CollectorReader{cfg: cfg, offsets: map[types.UID]int{}}
	r.pods = r.listPods
	r.read = r.tail

	return r
}

// Since reads what each running collector Pod wrote since the last call and returns every line read so far whose
// Time is at or after since, in read order. Safe for concurrent use.
func (r *CollectorReader) Since(ctx context.Context, since time.Time) ([]otlp.Line, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pods, err := r.pods(ctx)
	if err != nil {
		return nil, err
	}

	running := 0
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		running++

		out, err := r.read(ctx, pod, r.offsets[pod.UID])
		if err != nil {
			return nil, err
		}
		lines, consumed, err := otlp.Decode([]byte(out))
		if err != nil {
			return nil, fmt.Errorf("decode the collector file of %s: %w", pod.Name, err)
		}
		r.offsets[pod.UID] += consumed
		r.lines = append(r.lines, lines...)
	}
	if running == 0 {
		return nil, fmt.Errorf("%w in %s", ErrNoCollectorPod, collectorNamespace)
	}

	return otlp.Since(r.lines, since), nil
}

// clientset returns the clientset, built on first use; the caller holds r.mu.
func (r *CollectorReader) clientset() (kubernetes.Interface, error) {
	if r.cs == nil {
		cs, err := kubernetes.NewForConfig(r.cfg)
		if err != nil {
			return nil, fmt.Errorf("create clientset: %w", err)
		}
		r.cs = cs
	}

	return r.cs, nil
}

func (r *CollectorReader) listPods(ctx context.Context) ([]corev1.Pod, error) {
	cs, err := r.clientset()
	if err != nil {
		return nil, err
	}
	pods, err := cs.CoreV1().Pods(collectorNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=" + collectorName,
	})
	if err != nil {
		return nil, fmt.Errorf("list collector pods: %w", err)
	}

	return pods.Items, nil
}

// tail returns the collector file of pod from byte offset.
func (r *CollectorReader) tail(ctx context.Context, pod *corev1.Pod, offset int) (string, error) {
	name := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	out, stderr, err := Exec(ctx, r.cfg, name, collectorReader, []string{"tail", "-c", "+" + strconv.Itoa(offset+1), collectorFile})
	if err != nil {
		return "", fmt.Errorf("read the collector file of %s: %w: %s", name, err, strings.TrimSpace(stderr))
	}

	return out, nil
}
