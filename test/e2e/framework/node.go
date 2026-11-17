package framework

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ErrNoContainerID is returned when the Pod has no container ID for the named container.
var ErrNoContainerID = errors.New("no container id")

// KillContainer sends SIGKILL to the main process of the Pod's container on its kind node: no preStop, no grace.
func KillContainer(ctx context.Context, pod *corev1.Pod, container string) error {
	var id string
	for _, st := range append(slices.Clone(pod.Status.ContainerStatuses), pod.Status.InitContainerStatuses...) {
		if st.Name == container {
			_, id, _ = strings.Cut(st.ContainerID, "://")
		}
	}
	if id == "" {
		return fmt.Errorf("%w: container %s of pod %s/%s", ErrNoContainerID, container, pod.Namespace, pod.Name)
	}

	const script = `kill -KILL "$(crictl inspect -o go-template --template "{{.info.pid}}" "$1")"`
	out, err := exec.CommandContext(ctx, "docker", "exec", pod.Spec.NodeName, "sh", "-c", script, "sh", id).CombinedOutput() //nolint:gosec // the node name comes from the Pod under test
	if err != nil {
		return fmt.Errorf("kill container %s of pod %s/%s on node %s: %w: %s",
			container, pod.Namespace, pod.Name, pod.Spec.NodeName, err, strings.TrimSpace(string(out)))
	}

	return nil
}
