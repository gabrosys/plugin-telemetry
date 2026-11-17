//go:build e2e

package resilience_test

import (
	"context"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	shutdownCluster = "smart-shutdown"
	// maxSmartShutdown is far below CNPG's 180 s smartShutdownTimeout, which pooled sessions used to run out, and
	// above the pool's hold: the 10 s shutdown probe plus the queries in flight.
	maxSmartShutdown = time.Minute
	exportWait       = 3 * time.Minute
	shutdownPoll     = time.Second
)

var _ = Describe("smart shutdown", func() {
	It("lets PostgreSQL finish a smart shutdown within 60 seconds of a Pod delete", func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, cfg, err := framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())
		Expect(framework.CollectorReady(ctx, c)).To(Succeed())

		namespace, err := framework.CreateNamespace(ctx, c, "smart-shutdown")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(framework.DeleteNamespace(ctx, c, namespace.Name, statePoll)).To(Succeed())
		}, NodeTimeout(3*time.Minute))
		Expect(c.Create(ctx, framework.ExporterFor(namespace.Name, podpatch.DefaultExporter))).To(Succeed())

		cluster := framework.PeerCluster(namespace.Name, shutdownCluster, env.PostgresImage)
		cluster.Spec.Instances = 1
		Expect(c.Create(ctx, cluster)).To(Succeed())
		key := client.ObjectKeyFromObject(cluster)

		Eventually(func(ctx context.Context) (bool, error) { return framework.ClusterReady(ctx, c, key) }).
			WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(statePoll).Should(BeTrue(), "cluster ready")
		Eventually(framework.PluginRolledOut).WithContext(ctx).WithArguments(c).
			WithTimeout(3*time.Minute).WithPolling(statePoll).Should(BeTrue(), "plugin rolled out")
		// The sidecar's sessions are what held the smart shutdown, so it must be exporting this Cluster first.
		reader := framework.NewCollectorReader(cfg)
		podName := shutdownCluster + "-1"
		Eventually(func(ctx context.Context) (bool, error) {
			lines, err := reader.Since(ctx, time.Time{})

			return len(otlp.ForResource(lines, "k8s.pod.name", podName)) > 0, err
		}).WithContext(ctx).WithTimeout(exportWait).WithPolling(statePoll).Should(BeTrue(), "the sidecar exports")

		podKey := client.ObjectKey{Namespace: namespace.Name, Name: podName}
		var pod corev1.Pod
		Expect(c.Get(ctx, podKey, &pod)).To(Succeed())
		deleted := time.Now()
		Expect(c.Delete(ctx, &pod)).To(Succeed())

		// The postgres container's termination time when the poll sees it, else the moment the Pod is gone.
		var stopped time.Time
		Eventually(func(ctx context.Context) (bool, error) {
			var p corev1.Pod
			err := c.Get(ctx, podKey, &p)
			switch {
			case apierrors.IsNotFound(err) || (err == nil && p.UID != pod.UID):
				if stopped.IsZero() {
					stopped = time.Now()
				}

				return true, nil
			case err != nil:
				return false, fmt.Errorf("get pod %s: %w", podKey, err)
			}
			for _, st := range p.Status.ContainerStatuses {
				if st.Name == "postgres" && st.State.Terminated != nil {
					stopped = st.State.Terminated.FinishedAt.Time
				}
			}

			return false, nil
		}).WithContext(ctx).WithTimeout(5*time.Minute).WithPolling(shutdownPoll).Should(BeTrue(), "the Pod is gone")

		gap := stopped.Sub(deleted)
		AddReportEntry("smart shutdown", gap.String())
		Expect(gap).To(BeNumerically("<", maxSmartShutdown))
	}, SpecTimeout(15*time.Minute))
})
