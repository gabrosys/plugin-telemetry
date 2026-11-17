package operator

import (
	"context"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/fields"
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
// Its cache holds Clusters and TelemetryExporters from every namespace, and only the ServiceAccounts,
// Roles, RoleBindings and Secrets the plugin created, and the one sidecar image ConfigMap in namespace,
// all without managedFields.
func NewManager(cfg *rest.Config, namespace, imageConfigMap string) (manager.Manager, error) {
	scheme, err := newScheme()
	if err != nil {
		return nil, err
	}

	mgr, err := manager.New(cfg, managerOptions(scheme, namespace, imageConfigMap))
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

// managedObjects are the kinds cached only when they carry the managed-by label.
func managedObjects() []client.Object {
	return []client.Object{&corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}, &corev1.Secret{}}
}

// cachedObjects are all the kinds the plugin reads from the cache.
func cachedObjects() []client.Object {
	return append([]client.Object{&apiv1.Cluster{}, &v1alpha1.TelemetryExporter{}, &corev1.ConfigMap{}}, managedObjects()...)
}

func managerOptions(scheme *runtime.Scheme, namespace, imageConfigMap string) manager.Options {
	managed := cache.ByObject{Label: labels.SelectorFromSet(labels.Set{metadata.ManagedByLabel: metadata.ManagedByValue})}
	byObject := make(map[client.Object]cache.ByObject)
	for _, obj := range managedObjects() {
		byObject[obj] = managed
	}

	byObject[&corev1.ConfigMap{}] = cache.ByObject{
		Namespaces: map[string]cache.Config{namespace: {}},
		Field:      fields.OneTermEqualSelector("metadata.name", imageConfigMap),
	}

	return manager.Options{
		Scheme:                 scheme,
		LeaderElection:         false,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		PprofBindAddress:       "0",
		Cache: cache.Options{
			ByObject:                    byObject,
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
		},
	}
}

// registerInformers starts an informer for every cached kind without waiting for it to sync.
func registerInformers(ctx context.Context, c cache.Cache) error {
	for _, obj := range cachedObjects() {
		if _, err := c.GetInformer(ctx, obj, cache.BlockUntilSynced(false)); err != nil {
			return fmt.Errorf("register informer for %T: %w", obj, err)
		}
	}

	return nil
}
