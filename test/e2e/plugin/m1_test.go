//go:build e2e

package plugin_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// forbidden lists what the sidecar of cluster must not do; other is another Cluster of the namespace.
func forbidden(namespace, cluster, other string) []authorizationv1.ResourceAttributes {
	attrs := []authorizationv1.ResourceAttributes{
		{Namespace: namespace, Verb: "get", Resource: "secrets", Name: cluster + apiv1.SuperUserSecretSuffix},
		{Namespace: namespace, Verb: "get", Resource: "secrets", Name: cluster + apiv1.ApplicationUserSecretSuffix},
		{Namespace: namespace, Verb: "list", Resource: "secrets"},
		{Namespace: namespace, Verb: "list", Resource: "configmaps"},
		{Namespace: namespace, Verb: "watch", Resource: "secrets"},
		{Namespace: namespace, Verb: "watch", Resource: "configmaps"},
		{Namespace: namespace, Verb: "get", Group: cnpgGroup, Resource: "clusters", Name: other},
	}

	targets := []authorizationv1.ResourceAttributes{
		{Namespace: namespace, Group: cnpgGroup, Resource: "clusters"},
		{Namespace: namespace, Group: cnpgGroup, Resource: "clusters", Name: cluster},
		{Namespace: namespace, Group: metadata.PluginName, Resource: "telemetryexporters", Name: exporterName},
		{Namespace: namespace, Resource: "configmaps"},
		{Namespace: namespace, Resource: "configmaps", Name: baseQueries},
		{Namespace: namespace, Resource: "secrets"},
		{Namespace: namespace, Resource: "secrets", Name: monitorSecret},
		{Namespace: namespace, Resource: "pods"},
	}
	for _, verb := range []string{"create", "update", "patch", "delete"} {
		for _, target := range targets {
			target.Verb = verb
			attrs = append(attrs, target)
		}
	}

	return attrs
}

// bumpDescriptions changes spec.description of each Cluster, which bumps the generation and triggers a reconcile.
func bumpDescriptions(ctx context.Context, c client.Client, keys ...client.ObjectKey) {
	GinkgoHelper()

	for _, key := range keys {
		Eventually(func(g Gomega) {
			var cluster apiv1.Cluster
			g.Expect(c.Get(ctx, key, &cluster)).To(Succeed())
			generation := cluster.Generation
			patch := client.MergeFrom(cluster.DeepCopy())
			cluster.Spec.Description = "m1 reconcile " + time.Now().UTC().Format(time.RFC3339Nano)
			g.Expect(c.Patch(ctx, &cluster, patch)).To(Succeed())
			g.Expect(cluster.Generation).To(BeNumerically(">", generation), key.String())
		}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	}
}
