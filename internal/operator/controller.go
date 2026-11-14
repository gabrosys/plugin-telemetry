package operator

import (
	"context"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

// specChanged passes creates, deletes and updates that change metadata.generation.
var specChanged = predicate.GenerationChangedPredicate{}

// IdentityController keeps identities current between CNPG reconciles: For(Cluster) with
// GenerationChangedPredicate, Watches(TelemetryExporter, GenerationChangedPredicate) mapped to the
// Clusters in its namespace whose exporter parameter names it, Owns(ServiceAccount, Role,
// RoleBinding, Secret).
type IdentityController struct {
	Client   client.Client
	Identity *identity.Ensurer
	Metrics  *PluginMetrics // nil records nothing
}

// Reconcile ensures the identity of a Cluster that enables the plugin and removes it from one that does not.
// It returns Ensure and Remove errors so the workqueue backs off, and never requeues on its own.
func (c *IdentityController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	err := c.reconcile(ctx, req)
	outcome := outcomeOK
	if err != nil {
		outcome = outcomeError
	}
	c.Metrics.IdentityReconcile(outcome)

	return ctrl.Result{}, err
}

func (c *IdentityController) reconcile(ctx context.Context, req ctrl.Request) error {
	var cluster apiv1.Cluster
	if err := c.Client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("get cluster %s: %w", req.NamespacedName, err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return nil
	}

	logger := log.FromContext(ctx)

	refs, ok, err := referencesFor(ctx, c.Client, &cluster)
	if err != nil {
		return err
	}
	if !ok {
		return c.remove(ctx, logger, &cluster)
	}

	res, err := c.Identity.Ensure(ctx, &cluster, refs)
	if err != nil {
		return fmt.Errorf("ensure identity of %s: %w", req.NamespacedName, err)
	}
	if len(res.Applied) > 0 {
		logger.Info("identity controller", "outcome", outcomeApplied, "objects", res.Applied)
	}

	return nil
}

// remove deletes the identity when the cache still holds any of its objects.
func (c *IdentityController) remove(ctx context.Context, logger log.Logger, cluster *apiv1.Cluster) error {
	present, err := c.Identity.Present(ctx, cluster.Namespace, cluster.Name)
	if err != nil {
		return fmt.Errorf("check identity of %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	if !present {
		return nil
	}

	if err := c.Identity.Remove(ctx, cluster.Namespace, cluster.Name); err != nil {
		return fmt.Errorf("remove identity of %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	logger.Info("identity controller", "outcome", "removed")

	return nil
}

// SetupWithManager registers the controller with mgr.
func (c *IdentityController) SetupWithManager(mgr manager.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("identity").
		For(&apiv1.Cluster{}, builder.WithPredicates(specChanged)).
		Watches(&v1alpha1.TelemetryExporter{}, handler.EnqueueRequestsFromMapFunc(exportersToClusters(c.Client)),
			builder.WithPredicates(specChanged)).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&corev1.Secret{}).
		Complete(c)
	if err != nil {
		return fmt.Errorf("set up identity controller: %w", err)
	}

	return nil
}

// exportersToClusters maps a TelemetryExporter to the Clusters in its namespace whose enabled plugin
// entry names it.
func exportersToClusters(r client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		logger := log.FromContext(ctx)

		var clusters apiv1.ClusterList
		if err := r.List(ctx, &clusters, client.InNamespace(obj.GetNamespace())); err != nil {
			logger.Error(err, "list clusters for exporter", "namespace", obj.GetNamespace(), "exporter", obj.GetName())

			return nil
		}

		var requests []reconcile.Request
		for i := range clusters.Items {
			cluster := &clusters.Items[i]
			plugin, ok := clusterconfig.EnabledPlugin(cluster)
			if !ok {
				continue
			}
			parsed, err := podpatch.ParseParams(plugin.Parameters)
			if err != nil || parsed.Params.Exporter != obj.GetName() {
				continue
			}
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		}

		return requests
	}
}

// referencesFor reads the parameters and the cached exporter: ok is false when there's nothing to
// ensure (plugin absent or disabled, invalid auth).
func referencesFor(ctx context.Context, r client.Reader, c *apiv1.Cluster) (clusterconfig.Refs, bool, error) {
	plugin, ok := clusterconfig.EnabledPlugin(c)
	if !ok {
		return clusterconfig.Refs{}, false, nil
	}

	parsed, err := podpatch.ParseParams(plugin.Parameters)
	if err != nil {
		log.FromContext(ctx).Error(err, "invalid plugin parameters, no identity ensured",
			"namespace", c.Namespace, "cluster", c.Name)

		return clusterconfig.Refs{}, false, nil
	}

	var exporter *v1alpha1.TelemetryExporter
	found := &v1alpha1.TelemetryExporter{}
	err = r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: parsed.Params.Exporter}, found)
	switch {
	case err == nil:
		exporter = found
	case !apierrors.IsNotFound(err):
		return clusterconfig.Refs{}, false, fmt.Errorf("get exporter %s/%s: %w", c.Namespace, parsed.Params.Exporter, err)
	}

	return clusterconfig.References(c, parsed.Params, exporter), true, nil
}
