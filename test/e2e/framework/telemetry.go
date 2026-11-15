package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

const (
	collectorEndpoint = "http://" + collectorName + "." + collectorNamespace + ".svc:4318"
	exportInterval    = 10 * time.Second
)

// PluginStatus decodes our entry of status.pluginStatus; ok is false when it's absent or empty.
func PluginStatus(c *apiv1.Cluster) (operator.ClusterStatus, bool, error) {
	for _, ps := range c.Status.PluginStatus {
		if ps.Name != metadata.PluginName || ps.Status == "" {
			continue
		}

		var st operator.ClusterStatus
		if err := json.Unmarshal([]byte(ps.Status), &st); err != nil {
			return operator.ClusterStatus{}, false, fmt.Errorf("decode the status of plugin %s: %w", ps.Name, err)
		}

		return st, true, nil
	}

	return operator.ClusterStatus{}, false, nil
}

// Identity returns the identity objects of a Cluster as they are in the API server.
func Identity(ctx context.Context, c client.Client, namespace, cluster string) (identity.Objects, error) {
	account := metadata.ServiceAccountName(cluster)
	objects := identity.Objects{
		ServiceAccount: &corev1.ServiceAccount{},
		Role:           &rbacv1.Role{},
		RoleBinding:    &rbacv1.RoleBinding{},
		Token:          &corev1.Secret{},
	}

	for _, o := range []struct {
		name string
		obj  client.Object
	}{
		{account, objects.ServiceAccount},
		{account, objects.Role},
		{account, objects.RoleBinding},
		{metadata.TokenSecretName(cluster), objects.Token},
	} {
		key := client.ObjectKey{Namespace: namespace, Name: o.name}
		if err := c.Get(ctx, key, o.obj); err != nil {
			return identity.Objects{}, fmt.Errorf("get %T %s: %w", o.obj, key, err)
		}
	}

	return objects, nil
}

// ExporterFor returns a TelemetryExporter that sends metrics to the collector every 10s.
func ExporterFor(namespace, name string) *v1alpha1.TelemetryExporter {
	return &v1alpha1.TelemetryExporter{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: v1alpha1.TelemetryExporterSpec{
			Exporter: v1alpha1.OTLPExporter{Endpoint: collectorEndpoint},
			Metrics:  v1alpha1.MetricsSpec{Interval: &metav1.Duration{Duration: exportInterval}},
		},
	}
}
