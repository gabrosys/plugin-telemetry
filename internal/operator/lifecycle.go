package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

const podKind = "Pod"

// Outcomes of a lifecycle hook call, as logged.
const (
	outcomePatched   = "patched"
	outcomeUnchanged = "unchanged"
	outcomeSkipped   = "skipped"
)

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
// because a failing hook blocks the Cluster's reconcile. Every decoded
// request logs one "lifecycle hook" line.
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

	var meta metav1.PartialObjectMetadata
	if err := json.Unmarshal(req.GetObjectDefinition(), &meta); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode object metadata: %v", err)
	}

	logger := log.FromContext(ctx).WithValues("namespace", cluster.Namespace, "cluster", cluster.Name)

	patch, outcome, err := l.hook(logger, cluster, kind, req.GetObjectDefinition())
	if err != nil {
		return nil, err
	}

	logger.Info("lifecycle hook",
		"operation", strings.TrimPrefix(req.GetOperationType().GetType().String(), "TYPE_"),
		"pod", meta.Name,
		"outcome", outcome)

	return &lifecycle.OperatorLifecycleResponse{JsonPatch: patch}, nil
}

// hook returns the patch for one object and its outcome. It fails only for
// an undecodable Pod.
func (l *Lifecycle) hook(
	logger log.Logger, cluster *apiv1.Cluster, kind string, definition []byte,
) ([]byte, string, error) {
	if kind != podKind {
		return nil, outcomeSkipped, nil
	}

	pluginConfig, ok := ourPlugin(cluster)
	if !ok {
		return nil, outcomeSkipped, nil
	}

	original, err := decoder.DecodePodJSON(definition)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "decode pod: %v", err)
	}

	patch, err := l.patch(logger, cluster, pluginConfig, original)
	if err != nil {
		logger.Error(err, "sidecar not injected, returning no patch")
		return nil, outcomeSkipped, nil
	}
	if len(patch) == 0 {
		return nil, outcomeUnchanged, nil
	}

	return patch, outcomePatched, nil
}

func (l *Lifecycle) patch(
	logger log.Logger, cluster *apiv1.Cluster, pluginConfig apiv1.PluginConfiguration, original *corev1.Pod,
) ([]byte, error) {
	image := l.Images.Image()
	if image == "" {
		return nil, errNoImage
	}

	parsed, err := podpatch.ParseParams(pluginConfig.Parameters)
	if err != nil {
		return nil, fmt.Errorf("parse plugin parameters: %w", err)
	}
	for _, defaulted := range parsed.Defaulted {
		logger.Error(defaulted, "invalid sidecar resource, using the default")
	}
	if len(parsed.Ignored) > 0 {
		logger.Warning("ignoring unknown plugin parameters", "keys", parsed.Ignored)
	}

	mutated := original.DeepCopy()
	if err := podpatch.Apply(mutated, cluster.Name, parsed.Params, image); err != nil {
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
