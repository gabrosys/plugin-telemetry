package framework

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
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

// CollectorLines reads the last n lines of the collector's files, the rotated backups included.
func CollectorLines(ctx context.Context, cfg *rest.Config, n int) ([]otlp.Line, error) {
	return readCollector(ctx, cfg, n, time.Time{})
}

// CollectorLinesSince reads the last n lines of the collector's files, the rotated backups included,
// and keeps those whose Time is at or after since.
func CollectorLinesSince(ctx context.Context, cfg *rest.Config, n int, since time.Time) ([]otlp.Line, error) {
	return readCollector(ctx, cfg, n, since)
}

// rotatedFile matches the backups of the collector's file exporter: <name>-<UTC time>-size<ext>.
// Source: opentelemetry-collector-contrib v0.165.0 exporter/fileexporter/README.md, "File Rotation".
var rotatedFile = regexp.MustCompile(`^metrics-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}-size\.jsonl$`)

// RotatedFiles returns the paths of the backups in an `ls -1` listing of the collector's directory, oldest
// first: the time in their names has a fixed width, so the name order is the time order.
func RotatedFiles(listing string) []string {
	var names []string
	for name := range strings.Lines(listing) {
		if name = strings.TrimSpace(name); rotatedFile.MatchString(name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	files := make([]string, len(names))
	for i, name := range names {
		files[i] = path.Join(path.Dir(collectorFile), name)
	}

	return files
}

// ReadFiles reads at most n complete lines from files, given oldest first with the current file last.
// It asks the newest file for n lines and each older one only for what is left, then parses each file
// on its own, so a last line without a newline is skipped in every file. Lines before since are dropped.
func ReadFiles(files []string, read func(file string, n int) (string, error), n int, since time.Time) ([]otlp.Line, error) {
	outputs := make([]string, len(files))
	left := n
	for i := len(files) - 1; i >= 0 && left > 0; i-- {
		out, err := read(files[i], left)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", files[i], err)
		}
		outputs[i] = out
		left -= strings.Count(out, "\n")
	}

	var all []otlp.Line
	for _, out := range outputs {
		lines, err := ParseLines(out)
		if err != nil {
			return nil, err
		}
		all = append(all, lines...)
	}

	return otlp.Since(all, since), nil
}

func readCollector(ctx context.Context, cfg *rest.Config, n int, since time.Time) ([]otlp.Line, error) {
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

		name := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		listing, _, err := Exec(ctx, cfg, name, collectorReader, []string{"ls", "-1", path.Dir(collectorFile)})
		if err != nil {
			return nil, fmt.Errorf("list the collector directory: %w", err)
		}

		files := append(RotatedFiles(listing), collectorFile)
		lines, err := ReadFiles(files, func(file string, count int) (string, error) {
			out, _, err := Exec(ctx, cfg, name, collectorReader, []string{"tail", "-n", strconv.Itoa(count), file})

			return out, err
		}, n, since)
		if err != nil {
			return nil, fmt.Errorf("read the collector files: %w", err)
		}

		return lines, nil
	}

	return nil, fmt.Errorf("%w in %s", ErrNoCollectorPod, collectorNamespace)
}

// ParseLinesSince is ParseLines keeping the lines whose Time is at or after since.
func ParseLinesSince(out string, since time.Time) ([]otlp.Line, error) {
	lines, err := ParseLines(out)
	if err != nil {
		return nil, err
	}

	return otlp.Since(lines, since), nil
}

// ParseLines decodes the output of tail on the collector's file. A last line without a newline is skipped.
func ParseLines(out string) ([]otlp.Line, error) {
	var l otlp.Lines
	if err := l.Update([]byte(out)); err != nil {
		return nil, fmt.Errorf("decode the collector file: %w", err)
	}

	return l.All(), nil
}
