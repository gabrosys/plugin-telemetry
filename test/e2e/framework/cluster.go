package framework

import (
	"context"
	"encoding/json"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	operatorNamespace  = "cnpg-system"
	operatorDeployment = "cnpg-controller-manager"
	operatorContainer  = "manager"
)

// NewCluster returns a one-instance Cluster with 1Gi of storage.
func NewCluster(namespace, name, image string) *apiv1.Cluster {
	return &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: apiv1.ClusterSpec{
			Instances:            1,
			ImageName:            image,
			StorageConfiguration: apiv1.StorageConfiguration{Size: "1Gi"},
		},
	}
}

// TouchAnnotation is the annotation Touch sets; any edit makes CloudNativePG reconcile the Cluster.
const TouchAnnotation = "e2e.telemetry.gabrosys.github.io/touch"

// Touch merge-patches TouchAnnotation to value and returns the write's resourceVersion; API errors are returned for Eventually.
func Touch(ctx context.Context, c client.Client, key client.ObjectKey, value string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]string{TouchAnnotation: value}},
	})
	if err != nil {
		return "", fmt.Errorf("encode the touch patch: %w", err)
	}

	cluster := &apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
	if err := c.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, body)); err != nil {
		return "", fmt.Errorf("touch cluster %s: %w", key, err)
	}

	return cluster.ResourceVersion, nil
}

// ClusterReady reports whether the Cluster's Ready condition is true and all
// its instances are ready.
func ClusterReady(ctx context.Context, c client.Client, key client.ObjectKey) (bool, error) {
	var cluster apiv1.Cluster
	if err := c.Get(ctx, key, &cluster); err != nil {
		return false, fmt.Errorf("get cluster %s: %w", key, err)
	}

	return meta.IsStatusConditionTrue(cluster.Status.Conditions, string(apiv1.ConditionClusterReady)) &&
		cluster.Status.ReadyInstances == cluster.Spec.Instances, nil
}

// OperatorImage returns the image of the CNPG operator's manager container.
func OperatorImage(ctx context.Context, c client.Client) (string, error) {
	var deploy appsv1.Deployment
	key := client.ObjectKey{Namespace: operatorNamespace, Name: operatorDeployment}
	if err := c.Get(ctx, key, &deploy); err != nil {
		return "", fmt.Errorf("get deployment %s: %w", key, err)
	}

	for _, ctr := range deploy.Spec.Template.Spec.Containers {
		if ctr.Name == operatorContainer {
			return ctr.Image, nil
		}
	}

	return "", fmt.Errorf("deployment %s has no %q container", key, operatorContainer)
}
