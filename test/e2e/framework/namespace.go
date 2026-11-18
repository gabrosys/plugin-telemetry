package framework

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const suiteLabel = "e2e.telemetry.gabrosys.github.io/suite"

// CreateNamespace creates a namespace named <prefix>-<random>.
func CreateNamespace(ctx context.Context, c client.Client, prefix string) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{
		GenerateName: prefix + "-",
		Labels:       map[string]string{suiteLabel: prefix},
	}
	if err := c.Create(ctx, ns); err != nil {
		return nil, fmt.Errorf("create namespace %s-*: %w", prefix, err)
	}

	return ns, nil
}

// DeleteNamespace deletes every Cluster in the namespace and waits until no Cluster and no instance Pod is left. Then
// it deletes the namespace and waits until the namespace is gone. A failed read is retried at the next poll. It returns
// ctx's error, with what is left and the last failed read, if ctx ends before both waits finish.
func DeleteNamespace(ctx context.Context, c client.Client, name string, poll time.Duration) error {
	if err := c.DeleteAllOf(ctx, &apiv1.Cluster{}, client.InNamespace(name)); err != nil {
		return fmt.Errorf("delete the clusters in namespace %s: %w", name, err)
	}

	var (
		left    string
		lastErr error
	)
	err := wait.PollUntilContextCancel(ctx, poll, true, func(ctx context.Context) (bool, error) {
		var clusters apiv1.ClusterList
		if err := c.List(ctx, &clusters, client.InNamespace(name)); err != nil {
			lastErr = fmt.Errorf("list clusters: %w", err)

			return false, nil
		}
		var pods corev1.PodList
		instances := []client.ListOption{
			client.InNamespace(name), client.HasLabels{clusterLabel}, client.MatchingLabels{podRoleLabel: podRoleInstance},
		}
		if err := c.List(ctx, &pods, instances...); err != nil {
			lastErr = fmt.Errorf("list pods: %w", err)

			return false, nil
		}
		left = fmt.Sprintf("%d clusters, %d pods", len(clusters.Items), len(pods.Items))

		return len(clusters.Items) == 0 && len(pods.Items) == 0, nil
	})
	if err != nil {
		return waitError("the clusters and their instance pods in namespace "+name, err, left, lastErr)
	}

	ns := &corev1.Namespace{Name: name}
	if err := client.IgnoreNotFound(c.Delete(ctx, ns)); err != nil {
		return fmt.Errorf("delete namespace %s: %w", name, err)
	}

	left, lastErr = "", nil
	err = wait.PollUntilContextCancel(ctx, poll, true, func(ctx context.Context) (bool, error) {
		var got corev1.Namespace
		err := c.Get(ctx, client.ObjectKeyFromObject(ns), &got)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			lastErr = fmt.Errorf("get namespace: %w", err)

			return false, nil
		}
		left = fmt.Sprintf("namespace in phase %s", got.Status.Phase)
		for _, cond := range got.Status.Conditions {
			if cond.Status == corev1.ConditionTrue &&
				(cond.Type == corev1.NamespaceContentRemaining || cond.Type == corev1.NamespaceFinalizersRemaining) {
				left += "; " + cond.Message
			}
		}

		return false, nil
	})
	if err != nil {
		return waitError("namespace "+name, err, left, lastErr)
	}

	return nil
}

// waitError wraps err with what was left, if a poll recorded it, and the last failed read, if a read failed.
func waitError(what string, err error, left string, lastErr error) error {
	err = fmt.Errorf("wait for %s to go: %w", what, err)
	if left != "" {
		err = fmt.Errorf("%w (left: %s)", err, left)
	}
	if lastErr != nil {
		return fmt.Errorf("%w (last error: %w)", err, lastErr)
	}

	return err
}
