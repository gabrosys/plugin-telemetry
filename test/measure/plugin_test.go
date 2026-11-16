//go:build e2e

package measure_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	pluginNamespaces   = 100
	pluginClustersPer  = 10
	pluginWorkingSet   = 67108864 // 64Mi
	pluginNamespace    = "cnpg-system"
	pluginContainer    = "plugin-telemetry"
	pluginSettle       = 5 * time.Minute
	pluginIdleWindow   = 10 * time.Minute
	pluginReplaceDelay = 2 * time.Minute
	pluginReplaceSpan  = 5 * time.Minute
	pluginCreateLimit  = 16
	pluginRefName      = "measure-ref"
	reconciliationLoop = "cnpg.io/reconciliationLoop"
)

// replicaUsage is one plugin replica's usage over a window.
type replicaUsage struct {
	Pod             string         `json:"pod"`
	WorkingSetBytes framework.Stat `json:"workingSetBytes"`
	RSSBytes        framework.Stat `json:"rssBytes"`
}

// pluginMeasurement is plugin-<cnpg>.json.
type pluginMeasurement struct {
	CNPGVersion           string         `json:"cnpgVersion"`
	Namespaces            int            `json:"namespaces"`
	Clusters              int            `json:"clusters"`
	ClusterBytes          int            `json:"clusterBytes"`
	CreateSeconds         float64        `json:"createSeconds"`
	IdentitySeconds       float64        `json:"identitySeconds"`
	Idle                  []replicaUsage `json:"idle"`
	ReplacementReadySecs  float64        `json:"replacementReadySeconds"`
	ReplacementRestarts   int32          `json:"replacementRestarts"`
	LastTerminationReason string         `json:"lastTerminationReason"`
	Replacement           replicaUsage   `json:"replacement"`
}

// perReplica splits readings taken over pods in tick order into one replicaUsage per pod.
func perReplica(pods []string, readings []framework.Usage) []replicaUsage {
	out := make([]replicaUsage, len(pods))
	for i, pod := range pods {
		var ws, rss []uint64
		for j := i; j < len(readings); j += len(pods) {
			ws, rss = append(ws, readings[j].WorkingSet), append(rss, readings[j].RSS)
		}
		out[i] = replicaUsage{Pod: pod}
		if len(ws) > 0 {
			out[i].WorkingSetBytes, out[i].RSSBytes = framework.StatOf(ws), framework.StatOf(rss)
		}
	}

	return out
}

// renamed returns in with every occurrence of old replaced by name, through its JSON.
func renamed[T any](in *T, old, name string) (*T, error) {
	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	var out T
	if err := json.Unmarshal([]byte(strings.ReplaceAll(string(data), old, name)), &out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return &out, nil
}

var _ = Describe("the plugin replica with 1000 Clusters", Ordered, func() {
	var (
		c          client.Client
		cfg        *rest.Config
		out        string
		m          pluginMeasurement
		namespaces []string
		mu         sync.Mutex // namespaces, appended by the create workers

		identityStart time.Time
	)

	inNamespaces := func() map[string]bool {
		set := map[string]bool{}
		for _, n := range namespaces {
			set[n] = true
		}

		return set
	}

	createClusters := func(ctx context.Context, ref *apiv1.Cluster) error {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(pluginCreateLimit)
		for range pluginNamespaces {
			g.Go(func() error {
				ns, err := framework.CreateNamespace(gctx, c, "measure-plugin")
				if err != nil {
					return fmt.Errorf("create namespace: %w", err)
				}
				mu.Lock()
				namespaces = append(namespaces, ns.Name)
				mu.Unlock()

				if err := c.Create(gctx, measuredExporter(ns.Name)); err != nil {
					return fmt.Errorf("create exporter in %s: %w", ns.Name, err)
				}
				for i := range pluginClustersPer {
					name := fmt.Sprintf("pg-%02d", i)
					cluster := &apiv1.Cluster{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: ns.Name, Name: name,
							Annotations: map[string]string{reconciliationLoop: "disabled"},
						},
						Spec: *ref.Spec.DeepCopy(),
					}
					if err := c.Create(gctx, cluster); err != nil {
						return fmt.Errorf("create cluster %s/%s: %w", ns.Name, name, err)
					}
					status, err := renamed(&ref.Status, pluginRefName, name)
					if err != nil {
						return fmt.Errorf("rename the status for %s: %w", name, err)
					}
					cluster.Status = *status
					if err := c.Status().Update(gctx, cluster); err != nil {
						return fmt.Errorf("update status of %s/%s: %w", ns.Name, name, err)
					}
				}

				return nil
			})
		}

		return g.Wait()
	}

	BeforeAll(func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		out = os.Getenv("MEASURE_OUT")
		Expect(out).NotTo(BeEmpty(), "MEASURE_OUT")
		m.CNPGVersion = env.CNPGVersion

		var base client.Client
		base, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		fast := rest.CopyConfig(cfg)
		fast.QPS, fast.Burst = 50, 100
		c, err = client.New(fast, client.Options{Scheme: base.Scheme()})
		Expect(err).NotTo(HaveOccurred())

		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())

		refNS, err := framework.CreateNamespace(ctx, c, "measure-plugin-ref")
		Expect(err).NotTo(HaveOccurred())
		mu.Lock()
		namespaces = append(namespaces, refNS.Name)
		mu.Unlock()
		Expect(c.Create(ctx, measuredExporter(refNS.Name))).To(Succeed())
		refKey := client.ObjectKey{Namespace: refNS.Name, Name: pluginRefName}
		refCluster := framework.PeerCluster(refNS.Name, pluginRefName, env.PostgresImage)
		refCluster.Spec.Instances = 2
		Expect(c.Create(ctx, refCluster)).To(Succeed())
		Eventually(func(ctx SpecContext) (bool, error) {
			return framework.ClusterReady(ctx, c, refKey)
		}).WithContext(ctx).WithTimeout(8*time.Minute).WithPolling(statePoll).Should(BeTrue(), "reference cluster ready")
		Expect(c.Get(ctx, refKey, refCluster)).To(Succeed())

		start := time.Now()
		Expect(createClusters(ctx, refCluster)).To(Succeed())
		m.CreateSeconds = time.Since(start).Seconds()
		m.Namespaces, m.Clusters = pluginNamespaces, pluginNamespaces*pluginClustersPer

		var clusters apiv1.ClusterList
		Expect(c.List(ctx, &clusters)).To(Succeed())
		set, total, n := inNamespaces(), 0, 0
		for i := range clusters.Items {
			if !set[clusters.Items[i].Namespace] || clusters.Items[i].Name == pluginRefName {
				continue
			}
			data, err := json.Marshal(&clusters.Items[i])
			Expect(err).NotTo(HaveOccurred())
			total += len(data)
			n++
		}
		Expect(n).To(Equal(m.Clusters))
		m.ClusterBytes = total / n
		identityStart = start
	}, NodeTimeout(30*time.Minute))

	AfterAll(func(ctx SpecContext) {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(pluginCreateLimit)
		for _, name := range namespaces {
			g.Go(func() error {
				return client.IgnoreNotFound(c.Delete(gctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
			})
		}
		Expect(g.Wait()).To(Succeed())

		Eventually(func(ctx SpecContext) (int, error) {
			var all corev1.NamespaceList
			if err := c.List(ctx, &all); err != nil {
				return 0, fmt.Errorf("list namespaces: %w", err)
			}
			left := 0
			for _, ns := range all.Items {
				if slices.Contains(namespaces, ns.Name) {
					left++
				}
			}

			return left, nil
		}).WithContext(ctx).WithTimeout(10 * time.Minute).WithPolling(5 * time.Second).Should(BeZero())
	}, NodeTimeout(15*time.Minute))

	It("gives each of 1000 unreconciled Clusters its identity", func(ctx SpecContext) {
		set, refToken := inNamespaces(), metadata.TokenSecretName(pluginRefName)
		tokens := func(ctx context.Context) (int, error) {
			var secrets corev1.SecretList
			if err := c.List(ctx, &secrets, client.MatchingLabels{metadata.ManagedByLabel: metadata.ManagedByValue}); err != nil {
				return 0, fmt.Errorf("list secrets: %w", err)
			}
			n := 0
			for i := range secrets.Items {
				if set[secrets.Items[i].Namespace] && secrets.Items[i].Name != refToken && len(secrets.Items[i].Data["token"]) > 0 {
					n++
				}
			}

			return n, nil
		}
		Eventually(tokens).WithContext(ctx).WithTimeout(30*time.Minute).WithPolling(10*time.Second).
			Should(Equal(m.Clusters), "labelled token Secrets with a token")
		m.IdentitySeconds = time.Since(identityStart).Seconds()
		writeMeasurement(out, "plugin", m.CNPGVersion, m)

		var pods corev1.PodList
		Expect(c.List(ctx, &pods)).To(Succeed())
		for i := range pods.Items {
			if set[pods.Items[i].Namespace] && !strings.HasPrefix(pods.Items[i].Name, pluginRefName) {
				Fail("a Pod exists in " + pods.Items[i].Namespace + ": " + pods.Items[i].Name)
			}
		}
	}, SpecTimeout(35*time.Minute))

	It("keeps each replica's working set at most 64 MiB with 1000 Clusters cached", func(ctx SpecContext) {
		names, err := framework.PluginPods(ctx, c)
		Expect(err).NotTo(HaveOccurred())
		Expect(names).To(HaveLen(2))
		keys := make([]types.NamespacedName, len(names))
		for i, n := range names {
			keys[i] = types.NamespacedName{Namespace: pluginNamespace, Name: n}
		}

		Expect(framework.WaitUntil(ctx, time.Now().Add(pluginSettle))).To(Succeed())
		readings, sampleErr := framework.SampleUsage(ctx, cfg, keys, pluginContainer, usageEvery, pluginIdleWindow)
		m.Idle = perReplica(names, readings)
		writeMeasurement(out, "plugin", m.CNPGVersion, m)

		Expect(sampleErr).NotTo(HaveOccurred(), "sampling usage")
		for _, r := range m.Idle {
			Expect(r.WorkingSetBytes.Max).To(BeNumerically("<=", pluginWorkingSet), "max working set of "+r.Pod)
		}
	}, SpecTimeout(25*time.Minute))

	It("starts a replacement replica with 1000 Clusters without an OOM kill and settles at most 64 MiB", func(ctx SpecContext) {
		old, err := framework.PluginPods(ctx, c)
		Expect(err).NotTo(HaveOccurred())
		Expect(old).NotTo(BeEmpty())
		deleted := time.Now()
		Expect(c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pluginNamespace, Name: old[0]}})).To(Succeed())

		var replacement corev1.Pod
		Eventually(func(ctx SpecContext) (bool, error) {
			names, err := framework.PluginPods(ctx, c)
			if err != nil {
				return false, err
			}
			for _, n := range names {
				if slices.Contains(old, n) {
					continue
				}
				if err := c.Get(ctx, client.ObjectKey{Namespace: pluginNamespace, Name: n}, &replacement); err != nil {
					return false, fmt.Errorf("get pod %s: %w", n, err)
				}
				for _, cond := range replacement.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						return true, nil
					}
				}
			}

			return false, nil
		}).WithContext(ctx).WithTimeout(10*time.Minute).WithPolling(statePoll).Should(BeTrue(), "replacement ready")
		ready := time.Now()
		m.ReplacementReadySecs = ready.Sub(deleted).Seconds()

		key := client.ObjectKeyFromObject(&replacement)
		Expect(framework.WaitUntil(ctx, ready.Add(pluginReplaceDelay))).To(Succeed())
		readings, sampleErr := framework.SampleUsage(ctx, cfg, []types.NamespacedName{key}, pluginContainer, usageEvery, pluginReplaceSpan)
		m.Replacement = perReplica([]string{key.Name}, readings)[0]

		Expect(c.Get(ctx, key, &replacement)).To(Succeed())
		for _, st := range replacement.Status.ContainerStatuses {
			if st.Name == pluginContainer {
				m.ReplacementRestarts = st.RestartCount
				if t := st.LastTerminationState.Terminated; t != nil {
					m.LastTerminationReason = t.Reason
				}
			}
		}
		writeMeasurement(out, "plugin", m.CNPGVersion, m)

		Expect(sampleErr).NotTo(HaveOccurred(), "sampling usage")
		Expect(m.ReplacementRestarts).To(BeZero(), "restarts")
		Expect(m.LastTerminationReason).NotTo(Equal("OOMKilled"), "last termination reason")
		Expect(m.Replacement.WorkingSetBytes.Max).To(BeNumerically("<=", pluginWorkingSet), "max working set")
	}, SpecTimeout(25*time.Minute))
})
