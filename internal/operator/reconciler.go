package operator

import (
	"context"
	"errors"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
)

// DefaultPreTimeout bounds the identity ensure in Pre.
const DefaultPreTimeout = 5 * time.Second

// Outcomes of a reconciler hook call, as logged.
const (
	outcomeApplied = "applied"
	outcomeError   = "error"
)

var errNotCluster = errors.New("object is not a Cluster")

// Reconciler serves ReconcilerHooks for Clusters. Pre ensures the identity within Timeout;
// both hooks answer CONTINUE whatever happens.
type Reconciler struct {
	reconciler.UnimplementedReconcilerHooksServer
	Identity  *identity.Ensurer
	Exporters client.Reader // cached
	// Timeout bounds the ensure in Pre; DefaultPreTimeout when zero.
	Timeout time.Duration
}

// GetCapabilities declares the hooks run for Clusters.
func (*Reconciler) GetCapabilities(
	context.Context, *reconciler.ReconcilerHooksCapabilitiesRequest,
) (*reconciler.ReconcilerHooksCapabilitiesResult, error) {
	return &reconciler.ReconcilerHooksCapabilitiesResult{
		ReconcilerCapabilities: []*reconciler.ReconcilerHooksCapability{{
			Kind: reconciler.ReconcilerHooksCapability_KIND_CLUSTER,
		}},
	}, nil
}

// Pre ensures the Cluster's identity and lets CloudNativePG continue.
func (r *Reconciler) Pre(ctx context.Context, req *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error) {
	logger := log.FromContext(ctx)

	cluster, err := decodeHookCluster(req)
	if err != nil {
		logger.Error(err, "decode reconciler hook request")
		logger.Info("reconciler hook", "operation", "Pre", "outcome", outcomeError)

		return continueResult(), nil
	}

	logger = logger.WithValues("namespace", cluster.Namespace, "cluster", cluster.Name)
	outcome, err := r.ensure(ctx, cluster)
	if err != nil {
		logger.Error(err, "ensure identity")
	}
	logger.Info("reconciler hook", "operation", "Pre", "outcome", outcome)

	return continueResult(), nil
}

// Post lets CloudNativePG continue.
func (*Reconciler) Post(ctx context.Context, req *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error) {
	logger := log.FromContext(ctx)

	cluster, err := decodeHookCluster(req)
	if err != nil {
		logger.Error(err, "decode reconciler hook request")
		logger.Info("reconciler hook", "operation", "Post", "outcome", outcomeError)

		return continueResult(), nil
	}

	logger.Info("reconciler hook",
		"operation", "Post", "namespace", cluster.Namespace, "cluster", cluster.Name, "outcome", outcomeSkipped)

	return continueResult(), nil
}

// ensure applies the Cluster's identity and returns the outcome to log.
func (r *Reconciler) ensure(ctx context.Context, cluster *apiv1.Cluster) (string, error) {
	refs, ok, err := referencesFor(ctx, r.Exporters, cluster)
	if err != nil {
		return outcomeError, err
	}
	if !ok {
		return outcomeSkipped, nil
	}

	timeout := r.Timeout
	if timeout <= 0 {
		timeout = DefaultPreTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	res, err := r.Identity.Ensure(ctx, cluster, refs)
	if err != nil {
		return outcomeError, fmt.Errorf("ensure identity of %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	if len(res.Applied) == 0 {
		return outcomeUnchanged, nil
	}

	return outcomeApplied, nil
}

// decodeHookCluster returns the Cluster of a hook request, and fails for any other kind.
func decodeHookCluster(req *reconciler.ReconcilerHooksRequest) (*apiv1.Cluster, error) {
	kind, err := object.GetKind(req.GetResourceDefinition())
	if err != nil {
		return nil, fmt.Errorf("get resource kind: %w", err)
	}
	if kind != apiv1.ClusterKind {
		return nil, fmt.Errorf("%w: %q", errNotCluster, kind)
	}

	cluster, err := decoder.DecodeClusterLenient(req.GetClusterDefinition())
	if err != nil {
		return nil, fmt.Errorf("decode cluster: %w", err)
	}

	return cluster, nil
}

func continueResult() *reconciler.ReconcilerHooksResult {
	return &reconciler.ReconcilerHooksResult{Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE}
}
