package operator

import (
	"context"

	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
)

// Reconciler answers CloudNativePG's reconciler hooks for Clusters.
type Reconciler struct {
	reconciler.UnimplementedReconcilerHooksServer
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

// Pre lets CloudNativePG continue its reconciliation.
func (*Reconciler) Pre(context.Context, *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error) {
	return &reconciler.ReconcilerHooksResult{Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE}, nil
}

// Post lets CloudNativePG continue its reconciliation.
func (*Reconciler) Post(context.Context, *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error) {
	return &reconciler.ReconcilerHooksResult{Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE}, nil
}
