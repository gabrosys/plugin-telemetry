package framework

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
)

// Exec runs command in a pod container over WebSocket, falling back to SPDY,
// and returns its stdout and stderr.
func Exec(
	ctx context.Context, cfg *rest.Config, pod types.NamespacedName, container string, command []string,
) (stdout, stderr string, err error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", "", fmt.Errorf("create clientset: %w", err)
	}

	req := cs.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	websocket, err := remotecommand.NewWebSocketExecutor(cfg, "GET", req.URL().String())
	if err != nil {
		return "", "", fmt.Errorf("websocket executor: %w", err)
	}
	spdy, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("spdy executor: %w", err)
	}
	executor, err := remotecommand.NewFallbackExecutor(websocket, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return "", "", fmt.Errorf("fallback executor: %w", err)
	}

	var out, errOut bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: &errOut}); err != nil {
		return out.String(), errOut.String(), fmt.Errorf("exec %v in %s/%s: %w", command, pod, container, err)
	}

	return out.String(), errOut.String(), nil
}
