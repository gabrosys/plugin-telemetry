package framework

import (
	"context"

	corev1 "k8s.io/api/core/v1"
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
