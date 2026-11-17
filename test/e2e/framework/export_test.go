package framework

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
)

// NewCollectorReaderWith returns a reader over fake pods and reads.
func NewCollectorReaderWith(
	pods func(context.Context) ([]corev1.Pod, error),
	read func(ctx context.Context, pod *corev1.Pod, offset int) (string, error),
) *CollectorReader {
	r := NewCollectorReader(nil)
	r.pods = pods
	r.read = read

	return r
}

// SampleUsageWith runs the sampling loop over a fake clock and a fake read.
func SampleUsageWith(
	ctx context.Context,
	clk clock.WithTicker,
	read func(ctx context.Context, pod types.NamespacedName) (Usage, error),
	pods []types.NamespacedName,
	every, d time.Duration,
) ([]Usage, error) {
	return sampleUsage(ctx, clk, read, pods, every, d)
}
