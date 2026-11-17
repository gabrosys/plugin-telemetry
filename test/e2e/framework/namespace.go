package framework

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
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

// DeleteNamespace deletes every Cluster in the namespace and waits until no Cluster and no Pod with CNPG's cluster
// label is left. Then it deletes the namespace and waits until the namespace is gone. Both waits check once per poll.
func DeleteNamespace(ctx context.Context, c client.Client, name string, poll time.Duration) error {
	if err := c.DeleteAllOf(ctx, &apiv1.Cluster{}, client.InNamespace(name)); err != nil {
		return fmt.Errorf("delete the clusters in namespace %s: %w", name, err)
	}

	err := wait.PollUntilContextCancel(ctx, poll, true, func(ctx context.Context) (bool, error) {
		var clusters apiv1.ClusterList
		if err := c.List(ctx, &clusters, client.InNamespace(name)); err != nil {
			return false, fmt.Errorf("list clusters: %w", err)
		}
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(name), client.HasLabels{clusterLabel}); err != nil {
			return false, fmt.Errorf("list pods: %w", err)
		}

		return len(clusters.Items) == 0 && len(pods.Items) == 0, nil
	})
	if err != nil {
		return fmt.Errorf("wait for the clusters and their pods in namespace %s to go: %w", name, err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := client.IgnoreNotFound(c.Delete(ctx, ns)); err != nil {
		return fmt.Errorf("delete namespace %s: %w", name, err)
	}

	err = wait.PollUntilContextCancel(ctx, poll, true, func(ctx context.Context) (bool, error) {
		err := c.Get(ctx, client.ObjectKeyFromObject(ns), &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("get namespace: %w", err)
		}

		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for namespace %s to go: %w", name, err)
	}

	return nil
}
