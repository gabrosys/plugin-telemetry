//go:build e2e

package plugin_test

import (
	"fmt"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

// review is one access review and its outcome.
type review struct {
	attrs   authorizationv1.ResourceAttributes
	allowed bool
}

func (r review) String() string {
	gr := r.attrs.Resource
	if r.attrs.Group != "" {
		gr += "." + r.attrs.Group
	}
	if r.attrs.Name != "" {
		gr += "/" + r.attrs.Name
	}

	return fmt.Sprintf("%-6s %-60s %t", r.attrs.Verb, gr, r.allowed)
}

// decided returns the reviews whose outcome equals allowed.
func decided(reviews []review, allowed bool) []string {
	var out []string
	for _, r := range reviews {
		if r.allowed == allowed {
			out = append(out, r.String())
		}
	}

	return out
}

func table(reviews []review) string {
	lines := make([]string, 0, len(reviews))
	for _, r := range reviews {
		lines = append(lines, r.String())
	}

	return strings.Join(lines, "\n")
}

func formatRules(rules []authorizationv1.ResourceRule) string {
	lines := make([]string, 0, len(rules))
	for _, r := range rules {
		lines = append(lines, fmt.Sprintf("verbs=%v apiGroups=%q resources=%v resourceNames=%v",
			r.Verbs, r.APIGroups, r.Resources, r.ResourceNames))
	}

	return strings.Join(lines, "\n")
}

func sidecarSecurityContext(peer bool) *corev1.SecurityContext {
	sc := &corev1.SecurityContext{
		RunAsNonRoot:             ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if peer {
		sc.RunAsUser = ptr.To(metadata.PeerUID)
	}

	return sc
}

func sidecarMounts(peer bool) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: metadata.TokenVolume, MountPath: metadata.SidecarTokenDir, ReadOnly: true},
		{Name: metadata.TokenMaskVolume, MountPath: metadata.PodTokenDir, ReadOnly: true},
	}
	if peer {
		mounts = append(mounts, corev1.VolumeMount{
			Name: metadata.ScratchVolume, MountPath: metadata.SocketDir, SubPath: "run", ReadOnly: true,
		})
	}

	return mounts
}

// forbidden lists what the sidecar of cluster must not do; other is another Cluster of the namespace and
// readable what it may read, which it must not write either.
func forbidden(namespace, cluster, other string, readable []authorizationv1.ResourceAttributes) []authorizationv1.ResourceAttributes {
	attrs := []authorizationv1.ResourceAttributes{
		{Namespace: namespace, Verb: "get", Resource: "secrets", Name: cluster + apiv1.SuperUserSecretSuffix},
		{Namespace: namespace, Verb: "get", Resource: "secrets", Name: cluster + apiv1.ApplicationUserSecretSuffix},
		{Namespace: namespace, Verb: "list", Resource: "secrets"},
		{Namespace: namespace, Verb: "list", Resource: "configmaps"},
		{Namespace: namespace, Verb: "watch", Resource: "secrets"},
		{Namespace: namespace, Verb: "watch", Resource: "configmaps"},
		{Namespace: namespace, Verb: "get", Group: cnpgGroup, Resource: "clusters", Name: other},
	}

	targets := append([]authorizationv1.ResourceAttributes{
		{Namespace: namespace, Group: cnpgGroup, Resource: "clusters"},
		{Namespace: namespace, Resource: "configmaps"},
		{Namespace: namespace, Resource: "secrets"},
		{Namespace: namespace, Resource: "pods"},
	}, readable...)
	for _, verb := range []string{"create", "update", "patch", "delete"} {
		for _, target := range targets {
			target.Verb = verb
			attrs = append(attrs, target)
		}
	}

	return attrs
}
