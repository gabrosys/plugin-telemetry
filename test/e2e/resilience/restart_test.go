//go:build e2e

package resilience_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	hookEvaluate   = "EVALUATE"
	restarts       = 3
	rolloutTimeout = 3 * time.Minute
	afterRestarts  = 30 * time.Second
)

// restartSpecs adds PLG-01: the plugin's rolling restarts under traffic.
func restartSpecs(s *suite) {
	It("PLG-01: three rolling restarts of the plugin cause no failed hook call", func(ctx SpecContext) {
		s.healthy(ctx)

		start := time.Now()
		stop := s.traffic(ctx)
		defer stop()

		for i := range restarts {
			old, err := framework.PluginPods(ctx, s.c)
			Expect(err).NotTo(HaveOccurred())
			Expect(framework.RestartPlugin(ctx, s.c)).To(Succeed())
			Eventually(framework.PluginRolledOut).WithContext(ctx).WithArguments(s.c).
				WithTimeout(rolloutTimeout).WithPolling(statePoll).Should(BeTrue(), "rollout %d", i+1)
			Eventually(framework.PluginPods).WithContext(ctx).WithArguments(s.c).
				WithTimeout(rolloutTimeout).WithPolling(statePoll).
				ShouldNot(ContainElement(BeElementOf(old)), "an old plugin Pod is left after rollout %d", i+1)
		}
		restarted := time.Now()

		Expect(framework.WaitUntil(ctx, restarted.Add(afterRestarts))).To(Succeed())
		stop()

		s.expectNoFailedHookCall(ctx, "PLG-01", start)
		s.expectEvaluatedSince(ctx, restarted)
	}, SpecTimeout(15*time.Minute))
}

// expectEvaluatedSince fails unless every instance Pod has an EVALUATE line after since from a current plugin Pod.
func (s *suite) expectEvaluatedSince(ctx context.Context, since time.Time) {
	GinkgoHelper()

	plugins, err := framework.PluginPods(ctx, s.c)
	Expect(err).NotTo(HaveOccurred())
	Expect(plugins).NotTo(BeEmpty())
	instances, err := framework.InstancePods(ctx, s.c, s.cluster)
	Expect(err).NotTo(HaveOccurred())
	Expect(instances).NotTo(BeEmpty())

	evaluated := map[string]bool{}
	for _, plugin := range plugins {
		calls, err := framework.HookCalls(ctx, s.cfg, plugin, since)
		Expect(err).NotTo(HaveOccurred())
		for _, call := range calls {
			if call.Namespace == s.ns && call.Cluster == clusterName && call.Operation == hookEvaluate {
				evaluated[call.Pod] = true
			}
		}
	}
	for _, pod := range instances {
		Expect(evaluated).To(HaveKey(pod.Name), "no %s of %s after %s by %v", hookEvaluate, pod.Name, since, plugins)
	}
}
