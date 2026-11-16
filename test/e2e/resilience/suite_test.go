//go:build e2e

package resilience_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	clusterName    = "resilience"
	queriesName    = "resilience-queries"
	queriesKey     = "queries"
	clusterTimeout = 4 * time.Minute
	statePoll      = 2 * time.Second
	touchEvery     = 2 * time.Second
)

// suite is the state the specs share: one peer Cluster with an exporter and a query ConfigMap.
type suite struct {
	env     framework.Env
	c       client.Client
	cfg     *rest.Config
	ns      string
	cluster client.ObjectKey
}

func (s *suite) setUp(ctx SpecContext) {
	var err error
	s.env, err = framework.LoadEnv(os.Getenv)
	Expect(err).NotTo(HaveOccurred())
	s.c, s.cfg, err = framework.NewClients(s.env.KubeContext)
	Expect(err).NotTo(HaveOccurred())

	Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(s.c).
		WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())
	Expect(framework.CollectorReady(ctx, s.c)).To(Succeed())

	namespace, err := framework.CreateNamespace(ctx, s.c, "resilience")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) {
		Expect(client.IgnoreNotFound(s.c.Delete(ctx, namespace))).To(Succeed())
	})
	s.ns = namespace.Name
	s.cluster = client.ObjectKey{Namespace: s.ns, Name: clusterName}

	var queries corev1.ConfigMap
	Expect(yaml.UnmarshalStrict(readTestdata("queries.yaml"), &queries)).To(Succeed())
	queries.Namespace = s.ns
	Expect(s.c.Create(ctx, &queries)).To(Succeed())

	exporter := framework.ExporterFor(s.ns, podpatch.DefaultExporter)
	exporter.Spec.Metrics.PlanInterval = &metav1.Duration{Duration: 30 * time.Second}
	Expect(s.c.Create(ctx, exporter)).To(Succeed())

	cluster := framework.PeerCluster(s.ns, clusterName, s.env.PostgresImage)
	cluster.Spec.Instances = 2
	cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{
		CustomQueriesConfigMap: []machineryapi.ConfigMapKeySelector{{
			LocalObjectReference: machineryapi.LocalObjectReference{Name: queriesName},
			Key:                  queriesKey,
		}},
	}
	Expect(s.c.Create(ctx, cluster)).To(Succeed())
	// Runs before the namespace's cleanup: a namespace deleted around a live Cluster stays Terminating.
	DeferCleanup(func(ctx SpecContext) {
		Expect(client.IgnoreNotFound(s.c.Delete(ctx, cluster))).To(Succeed())
		Eventually(func(ctx SpecContext) bool {
			return apierrors.IsNotFound(s.c.Get(ctx, s.cluster, &apiv1.Cluster{}))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(statePoll).Should(BeTrue())
	}, NodeTimeout(3*time.Minute))

	Eventually(func(ctx SpecContext) (bool, error) {
		return framework.ClusterReady(ctx, s.c, s.cluster)
	}).WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(statePoll).Should(BeTrue(), "cluster ready")
}

// healthy waits until the Cluster is ready, the plugin is rolled out and the collector is ready.
func (s *suite) healthy(ctx context.Context) {
	GinkgoHelper()

	Eventually(func(ctx context.Context) (bool, error) {
		return framework.ClusterReady(ctx, s.c, s.cluster)
	}).WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(statePoll).Should(BeTrue(), "cluster ready")
	Eventually(framework.PluginRolledOut).WithContext(ctx).WithArguments(s.c).
		WithTimeout(3*time.Minute).WithPolling(statePoll).Should(BeTrue(), "plugin rolled out")
	Eventually(func(ctx context.Context) error {
		return framework.CollectorReady(ctx, s.c)
	}).WithContext(ctx).WithTimeout(3*time.Minute).WithPolling(statePoll).Should(Succeed(), "collector ready")
}

// traffic touches the Cluster every 2 s so CNPG calls the plugin's hooks; stop ends it and waits for it.
func (s *suite) traffic(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer GinkgoRecover()
		defer wg.Done()

		ticker := time.NewTicker(touchEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// A failed touch is retried by the next tick.
				if _, err := framework.Touch(ctx, s.c, s.cluster, time.Now().Format(time.RFC3339Nano)); err != nil && ctx.Err() == nil {
					AddReportEntry("touch error", err.Error())
				}
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
	}
}

// expectNoFailedHookCall fails when the operator logged a failed plugin call for the suite's namespace after since.
func (s *suite) expectNoFailedHookCall(ctx context.Context, name string, since time.Time) {
	GinkgoHelper()

	calls, err := framework.FailedHookCalls(ctx, s.cfg, s.ns, since)
	Expect(err).NotTo(HaveOccurred())
	AddReportEntry("failed hook calls: "+name, strings.Join(calls, "\n"))
	Expect(calls).To(BeEmpty())
}

func readTestdata(name string) []byte {
	GinkgoHelper()

	root, err := os.OpenRoot("testdata")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)

	data, err := root.ReadFile(name)
	Expect(err).NotTo(HaveOccurred())

	return data
}
