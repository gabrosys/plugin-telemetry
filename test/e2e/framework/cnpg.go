package framework

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
