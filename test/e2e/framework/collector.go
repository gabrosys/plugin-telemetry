package framework

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	collectorContainer  = "collector"
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

// CollectorReader reads the file the collector's file exporter writes. Rotation is off, so each Pod has one file
// and every line is read once.
type CollectorReader struct {
	cfg *rest.Config

	mu      sync.Mutex // guards offsets and lines
	offsets map[collectorRun]int
	lines   []otlp.Line
}

// collectorRun names one run of a collector Pod: the exporter truncates its file when the container restarts.
type collectorRun struct {
	uid      types.UID
	restarts int32
}

// NewCollectorReader returns a reader of the collector Pods' files.
func NewCollectorReader(cfg *rest.Config) *CollectorReader {
	return &CollectorReader{cfg: cfg, offsets: map[collectorRun]int{}}
}

// Since reads what each running collector Pod wrote since the last call and returns every line read so far whose
// Time is at or after since, in read order. Safe for concurrent use.
func (r *CollectorReader) Since(ctx context.Context, since time.Time) ([]otlp.Line, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cs, err := kubernetes.NewForConfig(r.cfg)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}
	pods, err := cs.CoreV1().Pods(collectorNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=" + collectorName,
	})
	if err != nil {
		return nil, fmt.Errorf("list collector pods: %w", err)
	}

	running := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		running++
		if err := r.read(ctx, pod); err != nil {
			return nil, err
		}
	}
	if running == 0 {
		return nil, fmt.Errorf("%w in %s", ErrNoCollectorPod, collectorNamespace)
	}

	return otlp.Since(r.lines, since), nil
}

// read appends the lines the collector in pod wrote past the offset of its run.
func (r *CollectorReader) read(ctx context.Context, pod *corev1.Pod) error {
	run := collectorRun{uid: pod.UID}
	for _, st := range pod.Status.ContainerStatuses {
		if st.Name == collectorContainer {
			run.restarts = st.RestartCount
		}
	}

	name := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	out, _, err := Exec(ctx, r.cfg, name, collectorReader, []string{"tail", "-c", "+" + strconv.Itoa(r.offsets[run]+1), collectorFile})
	if err != nil {
		return fmt.Errorf("read the collector file of %s: %w", name, err)
	}
	lines, consumed, err := otlp.Decode([]byte(out))
	if err != nil {
		return fmt.Errorf("decode the collector file of %s: %w", name, err)
	}
	r.offsets[run] += consumed
	r.lines = append(r.lines, lines...)

	return nil
}
