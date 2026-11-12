//go:build e2e

package smoke_test

import (
	"context"
	"os"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("smoke", Ordered, func() {
	var (
		env     framework.Env
		c       client.Client
		cfg     *rest.Config
		cluster *apiv1.Cluster
	)

	BeforeAll(func(ctx SpecContext) {
		var err error
		env, err = framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		ns, err := framework.CreateNamespace(ctx, c, "smoke")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, ns))).To(Succeed())
		})
		cluster = framework.NewCluster(ns.Name, "smoke", env.PostgresImage)
	})

	It("runs against the requested CNPG version", func(ctx SpecContext) {
		img, err := framework.OperatorImage(ctx, c)
		Expect(err).NotTo(HaveOccurred())
		AddReportEntry("cnpg-operator-image", img)
		Expect(framework.MatchesCNPGVersion(img, env.CNPGVersion)).To(BeTrue(), img)
	})

	It("creates a one-instance Cluster", func(ctx SpecContext) {
		Eventually(func(ctx context.Context) error {
			return client.IgnoreAlreadyExists(c.Create(ctx, cluster.DeepCopy()))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	})

	It("becomes ready", func(ctx SpecContext) {
		Eventually(framework.ClusterReady).WithContext(ctx).
			WithArguments(c, client.ObjectKeyFromObject(cluster)).
			WithTimeout(10 * time.Minute).WithPolling(5 * time.Second).Should(BeTrue())
	}, SpecTimeout(11*time.Minute))

	It("answers SELECT 1 on the primary", func(ctx SpecContext) {
		Expect(c.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
		pod := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Status.CurrentPrimary}
		stdout, stderr, err := framework.Exec(ctx, cfg, pod, "postgres", []string{"psql", "-XAtq", "-c", "SELECT 1"})
		Expect(err).NotTo(HaveOccurred(), stderr)
		Expect(stdout).To(Equal("1\n"))
	})
})
