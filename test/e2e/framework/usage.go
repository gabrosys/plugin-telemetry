package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
)

// ErrNoContainer is returned when the kubelet's stats summary doesn't list the container.
var ErrNoContainer = errors.New("container not in the stats summary")

// Usage is a container's memory and CPU as the kubelet reports it.
type Usage struct {
	WorkingSet   uint64 // workingSetBytes
	RSS          uint64 // rssBytes
	CPUNanoCores uint64 // usageNanoCores
}

// statsSummary is the part of the kubelet's stats summary ParseUsage reads
// (k8s.io/kubelet/pkg/apis/stats/v1alpha1 types.go).
type statsSummary struct {
	Pods []struct {
		PodRef struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"podRef"`
		Containers []struct {
			Name string `json:"name"`
			CPU  struct {
				UsageNanoCores uint64 `json:"usageNanoCores"`
			} `json:"cpu"`
			Memory struct {
				WorkingSetBytes uint64 `json:"workingSetBytes"`
				RSSBytes        uint64 `json:"rssBytes"`
			} `json:"memory"`
		} `json:"containers"`
	} `json:"pods"`
}

// ParseUsage returns the usage of container in pod from a kubelet stats summary.
func ParseUsage(summary []byte, pod types.NamespacedName, container string) (Usage, error) {
	var s statsSummary
	if err := json.Unmarshal(summary, &s); err != nil {
		return Usage{}, fmt.Errorf("decode the stats summary: %w", err)
	}

	for _, p := range s.Pods {
		if p.PodRef.Namespace != pod.Namespace || p.PodRef.Name != pod.Name {
			continue
		}
		for _, c := range p.Containers {
			if c.Name == container {
				return Usage{WorkingSet: c.Memory.WorkingSetBytes, RSS: c.Memory.RSSBytes, CPUNanoCores: c.CPU.UsageNanoCores}, nil
			}
		}
	}

	return Usage{}, fmt.Errorf("%w: %s %s", ErrNoContainer, pod, container)
}

// ContainerUsage reads the container's usage from the stats summary of the Pod's node, through the API server.
func ContainerUsage(ctx context.Context, cfg *rest.Config, pod types.NamespacedName, container string) (Usage, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return Usage{}, fmt.Errorf("create clientset: %w", err)
	}
	p, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return Usage{}, fmt.Errorf("get pod %s: %w", pod, err)
	}

	summary, err := cs.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", p.Spec.NodeName, "proxy/stats/summary").DoRaw(ctx)
	if err != nil {
		return Usage{}, fmt.Errorf("get the stats summary of node %s: %w", p.Spec.NodeName, err)
	}

	return ParseUsage(summary, pod, container)
}

// SampleUsage reads each pod's container usage every `every` over d, the first tick at once and the last at
// floor(d/every)×every (at d when d is a multiple of every), and returns the readings in tick order and, within
// a tick, pod order. When ctx ends it returns the readings so far with its error.
func SampleUsage(
	ctx context.Context, cfg *rest.Config, pods []types.NamespacedName, container string, every, d time.Duration,
) ([]Usage, error) {
	read := func(ctx context.Context, pod types.NamespacedName) (Usage, error) {
		return ContainerUsage(ctx, cfg, pod, container)
	}

	return sampleUsage(ctx, clock.RealClock{}, read, pods, every, d)
}

func sampleUsage(
	ctx context.Context,
	clk clock.WithTicker,
	read func(ctx context.Context, pod types.NamespacedName) (Usage, error),
	pods []types.NamespacedName,
	every, d time.Duration,
) ([]Usage, error) {
	ticker := clk.NewTicker(every)
	defer ticker.Stop()

	var readings []Usage
	for tick := range int(d/every) + 1 {
		if tick > 0 {
			select {
			case <-ctx.Done():
				return readings, fmt.Errorf("sample usage: %w", ctx.Err())
			case <-ticker.C():
			}
		}
		for _, pod := range pods {
			u, err := read(ctx, pod)
			if err != nil {
				return readings, fmt.Errorf("read usage of %s: %w", pod, err)
			}
			readings = append(readings, u)
		}
	}

	return readings, nil
}

// Stat is the min, median and max of a series of readings.
type Stat struct {
	Min    uint64 `json:"min"`
	Median uint64 `json:"median"`
	Max    uint64 `json:"max"`
}

// StatOf returns the Stat of xs, which must not be empty.
func StatOf(xs []uint64) Stat {
	s := slices.Sorted(slices.Values(xs))

	return Stat{Min: s[0], Median: s[len(s)/2], Max: s[len(s)-1]}
}
