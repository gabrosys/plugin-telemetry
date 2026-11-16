package framework

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CNPG's instance metrics port and path (pkg/management/url/url.go:34,70 at v1.30.1).
const (
	cnpgMetricsPort = "9187"
	cnpgMetricsPath = "/metrics"
)

// ScrapeCNPG reads pod's :9187/metrics through the API server's pod proxy; at is when the response arrived.
func ScrapeCNPG(ctx context.Context, cfg *rest.Config, pod types.NamespacedName) (text []byte, at time.Time, err error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("create clientset: %w", err)
	}
	text, err = cs.CoreV1().Pods(pod.Namespace).ProxyGet("http", pod.Name, cnpgMetricsPort, cnpgMetricsPath, nil).DoRaw(ctx)
	at = time.Now()
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("scrape %s:%s%s: %w", pod, cnpgMetricsPort, cnpgMetricsPath, err)
	}

	return text, at, nil
}

// Switchover asks CNPG to promote target as `kubectl cnpg promote` does: a status patch locked on cluster's
// resourceVersion setting targetPrimary, targetPrimaryTimestamp (RFC3339Micro, now), phase "Switchover in progress",
// phaseReason "Switching over to <target>" and the Ready condition False.
// Ported from CloudNativePG v1.30.1 internal/cmd/plugin/promote/promote.go and pkg/resources/status/transactions.go.
func Switchover(ctx context.Context, c client.Client, cluster *apiv1.Cluster, target string) error {
	patched := cluster.DeepCopy()
	patched.Status.TargetPrimary = target
	patched.Status.TargetPrimaryTimestamp = time.Now().Format(metav1.RFC3339Micro)
	patched.Status.Phase = apiv1.PhaseSwitchover
	patched.Status.PhaseReason = "Switching over to " + target
	meta.SetStatusCondition(&patched.Status.Conditions, metav1.Condition{
		Type:    string(apiv1.ConditionClusterReady),
		Status:  metav1.ConditionFalse,
		Reason:  string(apiv1.ClusterIsNotReady),
		Message: "Cluster Is Not Ready",
	})

	patch := client.MergeFromWithOptions(cluster, client.MergeFromWithOptimisticLock{})
	if err := c.Status().Patch(ctx, patched, patch); err != nil {
		return fmt.Errorf("switch cluster %s/%s over to %s: %w", cluster.Namespace, cluster.Name, target, err)
	}

	return nil
}
