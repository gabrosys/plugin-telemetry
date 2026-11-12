package framework

import (
	"context"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
