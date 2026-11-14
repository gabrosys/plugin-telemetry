package operator

import (
	"context"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/clusterstatus"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
)

// Status serves the Operator service, which reports the plugin's status in the Cluster.
type Status struct {
	operator.UnimplementedOperatorServer
}

// GetCapabilities declares only SetStatusInCluster.
func (*Status) GetCapabilities(context.Context, *operator.OperatorCapabilitiesRequest) (*operator.OperatorCapabilitiesResult, error) {
	return &operator.OperatorCapabilitiesResult{
		Capabilities: []*operator.OperatorCapability{{
			Type: &operator.OperatorCapability_Rpc{
				Rpc: &operator.OperatorCapability_RPC{Type: operator.OperatorCapability_RPC_TYPE_SET_STATUS_IN_CLUSTER},
			},
		}},
	}, nil
}

// SetStatusInCluster leaves the Cluster's plugin status unchanged.
func (*Status) SetStatusInCluster(context.Context, *operator.SetStatusInClusterRequest) (*operator.SetStatusInClusterResponse, error) {
	return clusterstatus.NewSetStatusInClusterResponseBuilder().NoOpResponse(), nil
}
