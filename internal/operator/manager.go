package operator

import (
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

// NewManager builds the plugin's manager: no leader election and no metrics, probe or pprof listeners.
// Its cache holds Clusters, TelemetryExporters and MetricQuerySets from every namespace, and only the
// ServiceAccounts, Roles, RoleBindings and Secrets the plugin created.
func NewManager(cfg *rest.Config) (manager.Manager, error) {
	scheme, err := newScheme()
	if err != nil {
		return nil, err
	}

	mgr, err := manager.New(cfg, managerOptions(scheme))
	if err != nil {
		return nil, fmt.Errorf("create manager: %w", err)
	}

	return mgr, nil
}

func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"client-go": clientgoscheme.AddToScheme,
		"rbac":      rbacv1.AddToScheme,
		"cnpg":      apiv1.AddToScheme,
		"telemetry": v1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("add %s types to scheme: %w", name, err)
		}
	}

	return scheme, nil
}

func managerOptions(scheme *runtime.Scheme) manager.Options {
	managed := cache.ByObject{Label: labels.SelectorFromSet(labels.Set{metadata.ManagedByLabel: metadata.ManagedByValue})}

	return manager.Options{
		Scheme:                 scheme,
		LeaderElection:         false,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		PprofBindAddress:       "0",
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.ServiceAccount{}: managed,
				&rbacv1.Role{}:           managed,
				&rbacv1.RoleBinding{}:    managed,
				&corev1.Secret{}:         managed,
			},
		},
	}
}
