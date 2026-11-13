package operator

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

const podKind = "Pod"

var errNoImage = errors.New("no sidecar image available yet")

// ImageSource provides the current sidecar image, or "" when none is known.
type ImageSource interface {
	Image() string
}

// Lifecycle adds the sidecar to instance Pods.
type Lifecycle struct {
	lifecycle.UnimplementedOperatorLifecycleServer
	// Images supplies the sidecar image.
	Images ImageSource
}

// GetCapabilities hooks Pod creation and evaluation.
func (*Lifecycle) GetCapabilities(
	context.Context, *lifecycle.OperatorLifecycleCapabilitiesRequest,
) (*lifecycle.OperatorLifecycleCapabilitiesResponse, error) {
	return &lifecycle.OperatorLifecycleCapabilitiesResponse{
		LifecycleCapabilities: []*lifecycle.OperatorLifecycleCapabilities{{
			Group: "",
			Kind:  podKind,
			OperationTypes: []*lifecycle.OperatorOperationType{
				{Type: lifecycle.OperatorOperationType_TYPE_CREATE},
				{Type: lifecycle.OperatorOperationType_TYPE_EVALUATE},
			},
		}},
	}, nil
}

// LifecycleHook returns the patch that adds the sidecar to an instance Pod.
// Only undecodable requests fail; every other problem yields an empty patch,
// because a failing hook blocks the Cluster's reconcile.
func (l *Lifecycle) LifecycleHook(
	ctx context.Context, req *lifecycle.OperatorLifecycleRequest,
) (*lifecycle.OperatorLifecycleResponse, error) {
	cluster, err := decoder.DecodeClusterLenient(req.GetClusterDefinition())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode cluster: %v", err)
	}

	kind, err := object.GetKind(req.GetObjectDefinition())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "get object kind: %v", err)
	}
	if kind != podKind {
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}

	pluginConfig, ok := ourPlugin(cluster)
	if !ok {
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}

	original, err := decoder.DecodePodJSON(req.GetObjectDefinition())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode pod: %v", err)
	}

	logger := log.FromContext(ctx).WithValues("namespace", cluster.Namespace, "cluster", cluster.Name)

	patch, err := l.patch(logger, cluster, pluginConfig, original)
	if err != nil {
		logger.Error(err, "sidecar not injected, returning no patch")
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}

	return &lifecycle.OperatorLifecycleResponse{JsonPatch: patch}, nil
}

func (l *Lifecycle) patch(
	logger log.Logger, cluster *apiv1.Cluster, pluginConfig apiv1.PluginConfiguration, original *corev1.Pod,
) ([]byte, error) {
	image := l.Images.Image()
	if image == "" {
		return nil, errNoImage
	}

	params, ignored, err := podpatch.ParseParams(pluginConfig.Parameters)
	if err != nil {
		return nil, fmt.Errorf("parse plugin parameters: %w", err)
	}
	if len(ignored) > 0 {
		logger.Warning("ignoring unknown plugin parameters", "keys", ignored)
	}

	mutated := original.DeepCopy()
	if err := podpatch.Apply(mutated, cluster.Name, params, image); err != nil {
		return nil, fmt.Errorf("apply sidecar patch: %w", err)
	}

	patch, err := object.CreatePatch(mutated, original)
	if err != nil {
		return nil, fmt.Errorf("create json patch: %w", err)
	}

	return patch, nil
}

func ourPlugin(cluster *apiv1.Cluster) (apiv1.PluginConfiguration, bool) {
	for _, p := range cluster.Spec.Plugins {
		if p.Name == metadata.PluginName && (p.Enabled == nil || *p.Enabled) {
			return p, true
		}
	}

	return apiv1.PluginConfiguration{}, false
}
