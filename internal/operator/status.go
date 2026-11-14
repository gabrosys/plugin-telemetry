package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/clusterstatus"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/machinery/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

// States in ClusterStatus.
const (
	exporterOK       = "ok"
	exporterMissing  = "missing"
	exporterInvalid  = "invalid"
	identityReady    = "ready"
	identityPending  = "pending"
	statusLogMessage = "set status"
)

// ClusterStatus is what the plugin stores in the Cluster's status.pluginStatus. Its JSON is
// deterministic: fixed field order, sorted problems, no timestamps.
type ClusterStatus struct {
	Exporter      string   `json:"exporter"`
	ExporterState string   `json:"exporterState"` // ok, missing, invalid
	Message       string   `json:"message,omitempty"`
	Identity      string   `json:"identity"` // ready, pending
	Problems      []string `json:"problems,omitempty"`
}

// Healthy reports whether the exporter is ok and the identity ready.
func (s ClusterStatus) Healthy() bool {
	return s.ExporterState == exporterOK && s.Identity == identityReady
}

// Status serves the Operator service, which reports the plugin's status in the Cluster.
type Status struct {
	operator.UnimplementedOperatorServer
	Reader   client.Reader     // cached exporters and query sets
	Identity *identity.Ensurer // Ready gives the identity state
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

// SetStatusInCluster never fails. It returns NoOpResponse when the status is healthy and equal to the
// stored one, so that CloudNativePG stops requeueing the Cluster every 5 s, and JSONStatusResponse otherwise.
func (s *Status) SetStatusInCluster(ctx context.Context, req *operator.SetStatusInClusterRequest) (*operator.SetStatusInClusterResponse, error) {
	builder := clusterstatus.NewSetStatusInClusterResponseBuilder()
	logger := log.FromContext(ctx)

	cluster, err := decoder.DecodeClusterLenient(req.GetCluster())
	if err != nil {
		logger.Error(err, "decode set status request")

		return builder.NoOpResponse(), nil
	}
	logger = logger.WithValues("namespace", cluster.Namespace, "cluster", cluster.Name)

	plugin, ok := clusterconfig.EnabledPlugin(cluster)
	if !ok {
		return builder.NoOpResponse(), nil
	}

	st, err := s.compute(ctx, cluster, plugin)
	if err != nil {
		logger.Error(err, statusLogMessage)

		return builder.NoOpResponse(), nil
	}

	raw, err := json.Marshal(st)
	if err != nil {
		logger.Error(fmt.Errorf("marshal status: %w", err), statusLogMessage)

		return builder.NoOpResponse(), nil
	}
	if st.Healthy() && storedStatus(cluster) == string(raw) {
		return builder.NoOpResponse(), nil
	}

	resp, err := builder.JSONStatusResponse(st)
	if err != nil {
		logger.Error(err, statusLogMessage)

		return builder.NoOpResponse(), nil
	}

	return resp, nil
}

// compute builds the status from the cached exporter and identity objects.
func (s *Status) compute(ctx context.Context, c *apiv1.Cluster, plugin apiv1.PluginConfiguration) (ClusterStatus, error) {
	parsed, authErr := podpatch.ParseParams(plugin.Parameters)
	params := parsed.Params
	var problems []string
	if authErr != nil {
		params = podpatch.Params{Exporter: podpatch.DefaultExporter, Auth: podpatch.AuthPeer}
		if v := plugin.Parameters["exporter"]; v != "" {
			params.Exporter = v
		}
		problems = append(problems, fmt.Sprintf("auth %q is invalid: no sidecar is injected", plugin.Parameters["auth"]))
	} else {
		problems = append(problems, clusterconfig.SpecProblems(c, params)...)
	}
	for _, err := range parsed.Defaulted {
		problems = append(problems, err.Error())
	}
	for _, k := range parsed.Ignored {
		problems = append(problems, fmt.Sprintf("unknown plugin parameter %q is ignored", k))
	}
	slices.Sort(problems)

	st := ClusterStatus{Exporter: params.Exporter, Problems: problems}

	exporter := &v1alpha1.TelemetryExporter{}
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: params.Exporter}, exporter)
	switch {
	case apierrors.IsNotFound(err):
		st.ExporterState = exporterMissing
	case err != nil:
		return ClusterStatus{}, fmt.Errorf("get exporter %s/%s: %w", c.Namespace, params.Exporter, err)
	default:
		if err := clusterconfig.Check(c, params, exporter); err != nil {
			st.ExporterState = exporterInvalid
			st.Message = firstLine(err)
		} else {
			st.ExporterState = exporterOK
		}
	}

	ready, err := s.Identity.Ready(ctx, c.Namespace, c.Name)
	if err != nil {
		return ClusterStatus{}, fmt.Errorf("check identity of %s/%s: %w", c.Namespace, c.Name, err)
	}
	st.Identity = identityPending
	if ready {
		st.Identity = identityReady
	}

	return st, nil
}

// storedStatus returns the status CloudNativePG holds for this plugin.
func storedStatus(c *apiv1.Cluster) string {
	for _, ps := range c.Status.PluginStatus {
		if ps.Name == metadata.PluginName {
			return ps.Status
		}
	}

	return ""
}

func firstLine(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")

	return line
}
