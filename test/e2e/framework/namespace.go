package framework

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const suiteLabel = "e2e.telemetry.gabrosys.github.io/suite"

// CreateNamespace creates a namespace named <prefix>-<random>.
func CreateNamespace(ctx context.Context, c client.Client, prefix string) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: prefix + "-",
		Labels:       map[string]string{suiteLabel: prefix},
	}}
	if err := c.Create(ctx, ns); err != nil {
		return nil, fmt.Errorf("create namespace %s-*: %w", prefix, err)
	}

	return ns, nil
}
