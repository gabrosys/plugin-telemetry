package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

// ownedObjects drops create events: the plugin's own creates would re-trigger the Cluster before its cache holds them.
var ownedObjects = predicate.Funcs{CreateFunc: func(event.CreateEvent) bool { return false }}

// specChanged passes creates, deletes and updates that change metadata.generation.
var specChanged = predicate.GenerationChangedPredicate{}

// IdentityController keeps identities current between CNPG reconciles: For(Cluster) with
// GenerationChangedPredicate, Watches(TelemetryExporter, GenerationChangedPredicate) mapped to the
// Clusters in its namespace whose exporter parameter names it, Owns(ServiceAccount, Role, RoleBinding, Secret).
type IdentityController struct {
	Client   client.Client
	Identity *identity.Ensurer
	Metrics  *PluginMetrics // nil records nothing

	blocked sync.Map // Clusters whose "in the way" line was logged and hasn't cleared; keyed by namespaced name
}

// requeueInTheWay is the wait before Ensure retries after an object in the way; the cache may catch up
// or the object's owner may be gone.
const requeueInTheWay = 5 * time.Second

// Reconcile ensures the identity of a Cluster that enables the plugin and removes it from one that does not.
// An object in the way requeues after 5 s; any other error goes to the workqueue's back-off.
func (c *IdentityController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	err := c.reconcile(ctx, req)
	outcome := outcomeOK
	if err != nil {
		outcome = outcomeError
	}
	c.Metrics.IdentityReconcile(outcome)

	logger := log.FromContext(ctx)
	switch {
	case errors.Is(err, identity.ErrInTheWay):
		if _, seen := c.blocked.LoadOrStore(req.NamespacedName, struct{}{}); !seen {
			logger.Info("identity controller", "outcome", outcomeInTheWay, "reason", err.Error())
		}

		return ctrl.Result{RequeueAfter: requeueInTheWay}, nil
	case err == nil:
		if _, was := c.blocked.LoadAndDelete(req.NamespacedName); was {
			logger.Info("identity controller", "outcome", outcomeInTheWay+" cleared", "cluster", req.NamespacedName)
		}
	}

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

	wrote, err := c.Identity.Ensure(ctx, &cluster, refs)
	if len(wrote) > 0 {
		logger.Info("identity controller", "outcome", outcomeApplied, "objects", wrote)
	}
	if err != nil {
		return fmt.Errorf("ensure identity of %s: %w", req.NamespacedName, err)
	}

	return nil
}

// remove deletes the identity objects the cache holds.
func (c *IdentityController) remove(ctx context.Context, logger log.Logger, cluster *apiv1.Cluster) error {
	removed, err := c.Identity.Remove(ctx, cluster.Namespace, cluster.Name)
	if removed > 0 {
		logger.Info("identity controller", "outcome", "removed")
	}
	if err != nil {
		return fmt.Errorf("remove identity of %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}

	return nil
}

// SetupWithManager registers the controller with mgr.
func (c *IdentityController) SetupWithManager(mgr manager.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("identity").
		For(&apiv1.Cluster{}, builder.WithPredicates(specChanged)).
		Watches(&v1alpha1.TelemetryExporter{}, handler.EnqueueRequestsFromMapFunc(exportersToClusters(c.Client)),
			builder.WithPredicates(specChanged)).
		Owns(&corev1.ServiceAccount{}, builder.WithPredicates(ownedObjects)).
		Owns(&rbacv1.Role{}, builder.WithPredicates(ownedObjects)).
		Owns(&rbacv1.RoleBinding{}, builder.WithPredicates(ownedObjects)).
		Owns(&corev1.Secret{}, builder.WithPredicates(ownedObjects)).
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
