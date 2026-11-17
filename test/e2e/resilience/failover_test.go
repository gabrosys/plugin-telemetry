//go:build e2e

package resilience_test

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	failoverTimeout = 3 * time.Minute
	failoverPoll    = 250 * time.Millisecond
	failoverSlack   = 30 * time.Second
	pluginContainer = "plugin-telemetry"
)

// newestPluginPod returns the plugin Pod that logged the newest hook call for the suite's namespace after since.
func (s *suite) newestPluginPod(ctx context.Context, since time.Time) (string, error) {
	plugins, err := framework.PluginPods(ctx, s.c)
	if err != nil {
		return "", fmt.Errorf("list plugin pods: %w", err)
	}

	var (
		newest time.Time
		pod    string
	)
	for _, plugin := range plugins {
		calls, err := framework.HookCalls(ctx, s.cfg, plugin, since)
		if err != nil {
			return "", fmt.Errorf("hook calls of %s: %w", plugin, err)
		}
		for _, call := range calls {
			if call.Namespace == s.ns && call.Time.After(newest) {
				newest, pod = call.Time, plugin
			}
		}
	}

	return pod, nil
}

// failover applies fault to the plugin replica with the newest hook call, deletes the primary and returns
// the old primary, the new one and the time from the delete's return until the new one is a ready endpoint of -rw.
func (s *suite) failover(
	ctx context.Context, fault func(ctx context.Context, plugin *corev1.Pod),
) (oldPrimary, newPrimary string, took time.Duration) {
	GinkgoHelper()

	s.healthy(ctx)

	start := time.Now()
	stop := s.traffic(ctx)
	defer stop()

	var hit string
	Eventually(func(ctx context.Context) (string, error) {
		var err error
		hit, err = s.newestPluginPod(ctx, start)

		return hit, err
	}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(statePoll).ShouldNot(BeEmpty(), "no hook call for %s", s.ns)

	if fault != nil {
		var plugin corev1.Pod
		Expect(s.c.Get(ctx, client.ObjectKey{Namespace: pluginNamespace, Name: hit}, &plugin)).To(Succeed())
		fault(ctx, &plugin)
	}

	var cluster apiv1.Cluster
	Expect(s.c.Get(ctx, s.cluster, &cluster)).To(Succeed())
	oldPrimary = cluster.Status.CurrentPrimary
	primary := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: oldPrimary}}
	Expect(s.c.Delete(ctx, &primary, client.GracePeriodSeconds(1))).To(Succeed())
	deleted := time.Now()

	Eventually(func(ctx context.Context) (string, error) {
		ready, err := framework.ReadyEndpoints(ctx, s.c, s.ns, clusterName+"-rw")
		if err != nil {
			return "", err
		}
		for _, name := range ready {
			if name != oldPrimary {
				newPrimary = name
			}
		}

		return newPrimary, nil
	}).WithContext(ctx).WithTimeout(failoverTimeout).WithPolling(failoverPoll).ShouldNot(BeEmpty(), "no new primary in -rw")
	took = time.Since(deleted)

	stop()
	AddReportEntry("failover", fmt.Sprintf("plugin replica hit: %s, old: %s, new: %s, took: %s", hit, oldPrimary, newPrimary, took))
	calls, err := framework.FailedHookCalls(ctx, s.cfg, s.ns, start)
	Expect(err).NotTo(HaveOccurred())
	AddReportEntry("failover failed hook calls", fmt.Sprint(calls))

	return oldPrimary, newPrimary, took
}

// failoverSpecs adds a failover with both plugin replicas up, then failovers with one replica deleted or hard-killed.
func failoverSpecs(s *suite) {
	var baseline time.Duration

	check := func(fault func(ctx context.Context, plugin *corev1.Pod)) func(ctx SpecContext) {
		return func(ctx SpecContext) {
			old, newer, took := s.failover(ctx, fault)
			Expect(newer).NotTo(Equal(old))
			Expect(took).To(BeNumerically("<=", baseline+failoverSlack))
		}
	}

	It("measures a failover with both plugin replicas up", func(ctx SpecContext) {
		old, newer, took := s.failover(ctx, nil)
		Expect(newer).NotTo(Equal(old))
		baseline = took
	}, SpecTimeout(6*time.Minute))

	It("a failover with one plugin replica deleted takes at most 30 s more",
		check(func(ctx context.Context, plugin *corev1.Pod) {
			Expect(s.c.Delete(ctx, plugin)).To(Succeed())
		}), SpecTimeout(6*time.Minute))

	It("a failover with one plugin replica hard-killed takes at most 30 s more",
		check(func(ctx context.Context, plugin *corev1.Pod) {
			before := restartCount(plugin, pluginContainer)
			Expect(framework.KillContainer(ctx, plugin, pluginContainer)).To(Succeed())
			Eventually(func(ctx context.Context) (int32, error) {
				var p corev1.Pod
				if err := s.c.Get(ctx, client.ObjectKeyFromObject(plugin), &p); err != nil {
					return 0, err
				}

				return restartCount(&p, pluginContainer), nil
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(statePoll).Should(Equal(before + 1))
		}), SpecTimeout(6*time.Minute))
}

func restartCount(pod *corev1.Pod, container string) int32 {
	for _, st := range pod.Status.ContainerStatuses {
		if st.Name == container {
			return st.RestartCount
		}
	}

	return 0
}
