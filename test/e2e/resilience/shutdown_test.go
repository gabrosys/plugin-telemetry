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
)

const (
	shutdownCluster = "smart-shutdown"
	// maxSmartShutdown is far below CNPG's 180 s smartShutdownTimeout, which pooled sessions used to run out.
	maxSmartShutdown = 30 * time.Second
	shutdownPoll     = time.Second
)

var _ = Describe("smart shutdown", func() {
	It("lets PostgreSQL finish a smart shutdown within 30 seconds of a Pod delete", func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, _, err := framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())
		Expect(framework.CollectorReady(ctx, c)).To(Succeed())

		namespace, err := framework.CreateNamespace(ctx, c, "smart-shutdown")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) { Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed()) })
		Expect(c.Create(ctx, framework.ExporterFor(namespace.Name, podpatch.DefaultExporter))).To(Succeed())

		cluster := framework.PeerCluster(namespace.Name, shutdownCluster, env.PostgresImage)
		cluster.Spec.Instances = 1
		Expect(c.Create(ctx, cluster)).To(Succeed())
		// Runs before the namespace's cleanup: a namespace deleted around a live Cluster stays Terminating.
		key := client.ObjectKeyFromObject(cluster)
		DeferCleanup(func(ctx SpecContext) { Expect(client.IgnoreNotFound(c.Delete(ctx, cluster))).To(Succeed()) }, NodeTimeout(3*time.Minute))

		Eventually(func(ctx context.Context) (bool, error) { return framework.ClusterReady(ctx, c, key) }).
			WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(statePoll).Should(BeTrue(), "cluster ready")
		Eventually(framework.PluginRolledOut).WithContext(ctx).WithArguments(c).
			WithTimeout(3*time.Minute).WithPolling(statePoll).Should(BeTrue(), "plugin rolled out")
		// The sidecar's sessions are what held the smart shutdown, so let it run a few rounds first.
		Expect(framework.WaitUntil(ctx, time.Now().Add(30*time.Second))).To(Succeed())

		podKey := client.ObjectKey{Namespace: namespace.Name, Name: shutdownCluster + "-1"}
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
