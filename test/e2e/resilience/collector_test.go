//go:build e2e

package resilience_test

import (
	"context"
	"fmt"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	collectorNamespace = "telemetry-e2e"
	collectorService   = "collector"
	exportsWarmup      = 3 * time.Minute
	usageEvery         = 15 * time.Second
	steadyWindow       = 2 * time.Minute
	outageWindow       = 10 * time.Minute
	growthLimit        = 1.10
	resumeLimit        = 20 * time.Second
)

// perPod splits SampleUsage readings, taken in pod order, into one series of working sets and one of RSS per pod.
func perPod(readings []framework.Usage, pods int) (workingSets, rss [][]uint64) {
	workingSets, rss = make([][]uint64, pods), make([][]uint64, pods)
	for i, u := range readings {
		workingSets[i%pods] = append(workingSets[i%pods], u.WorkingSet)
		rss[i%pods] = append(rss[i%pods], u.RSS)
	}

	return workingSets, rss
}

// median returns the middle value of values, the upper one when their number is even.
func median(values []uint64) uint64 {
	sorted := slices.Sorted(slices.Values(values))

	return sorted[len(sorted)/2]
}

// readyCollector reports whether the collector Service has a ready endpoint.
func (s *suite) readyCollector(ctx context.Context) (bool, error) {
	ready, err := framework.ReadyEndpoints(ctx, s.c, collectorNamespace, collectorService)

	return len(ready) > 0, err
}

// collectorSpecs adds ENG-05: the collector down for ten minutes.
func collectorSpecs(s *suite) {
	It("ENG-05: with the collector down for 10 minutes each sidecar's working set stays within 10 % of its steady value "+
		"and exports resume within two export intervals", func(ctx SpecContext) {
		s.healthy(ctx)

		instances, err := framework.InstancePods(ctx, s.c, s.cluster)
		Expect(err).NotTo(HaveOccurred())
		Expect(instances).To(HaveLen(2))
		pods := make([]types.NamespacedName, len(instances))
		restartsBefore := make([]int32, len(instances))
		for i := range instances {
			pods[i] = types.NamespacedName{Namespace: instances[i].Namespace, Name: instances[i].Name}
			restartsBefore[i] = sidecarRestarts(&instances[i])
		}

		reader := framework.NewCollectorReader(s.cfg)
		Expect(framework.WaitUntil(ctx, time.Now().Add(exportsWarmup))).To(Succeed())

		readings, err := framework.SampleUsage(ctx, s.cfg, pods, metadata.SidecarContainerName, usageEvery, steadyWindow)
		Expect(err).NotTo(HaveOccurred())
		steadyWS, steadyRSS := perPod(readings, len(pods))

		DeferCleanup(func(ctx SpecContext) {
			Expect(framework.ScaleCollector(ctx, s.c, 1)).To(Succeed())
			Expect(framework.CollectorReady(ctx, s.c)).To(Succeed())
		}, NodeTimeout(4*time.Minute))
		Expect(framework.ScaleCollector(ctx, s.c, 0)).To(Succeed())
		Eventually(s.readyCollector).WithContext(ctx).WithTimeout(2*time.Minute).WithPolling(time.Second).
			Should(BeFalse(), "collector endpoints gone")

		readings, err = framework.SampleUsage(ctx, s.cfg, pods, metadata.SidecarContainerName, usageEvery, outageWindow)
		Expect(err).NotTo(HaveOccurred())
		outageWS, outageRSS := perPod(readings, len(pods))

		Expect(framework.ScaleCollector(ctx, s.c, 1)).To(Succeed())
		var tReady time.Time
		Eventually(func(ctx SpecContext) (bool, error) {
			ready, err := s.readyCollector(ctx)
			if ready {
				tReady = time.Now()
			}

			return ready, err
		}).WithContext(ctx).WithTimeout(2*time.Minute).WithPolling(failoverPoll).Should(BeTrue(), "collector endpoint back")

		// Record every instance's diagnostics first; the checks run together at the end, so one breach hides none.
		var breaches []string
		soft := NewGomega(func(message string, _ ...int) { breaches = append(breaches, message) })
		for i, pod := range pods {
			steady := median(steadyWS[i])
			highest := slices.Max(outageWS[i])
			AddReportEntry("working set "+pod.Name, fmt.Sprintf("steady %d, min %d, max %d bytes",
				steady, slices.Min(outageWS[i]), highest))
			AddReportEntry("rssBytes "+pod.Name, fmt.Sprintf("steady %d, min %d, max %d bytes",
				median(steadyRSS[i]), slices.Min(outageRSS[i]), slices.Max(outageRSS[i])))
			soft.Expect(float64(highest)).To(BeNumerically("<=", growthLimit*float64(steady)), "working set of %s", pod.Name)
		}

		var delays []time.Duration
		Eventually(func(ctx SpecContext) (bool, error) {
			lines, err := reader.Since(ctx, tReady)
			if err != nil {
				return false, err
			}
			delays = delays[:0]
			for _, pod := range pods {
				if first := otlp.ForResource(lines, "k8s.pod.name", pod.Name); len(first) > 0 {
					delays = append(delays, first[0].Time.Sub(tReady))
				}
			}

			return len(delays) == len(pods), nil
		}).WithContext(ctx).WithTimeout(2*time.Minute).WithPolling(statePoll).Should(BeTrue(), "every sidecar exports again")
		for i, pod := range pods {
			AddReportEntry("resume delay "+pod.Name, delays[i].String())
			soft.Expect(delays[i]).To(BeNumerically("<=", resumeLimit), "first point of %s after the collector is back", pod.Name)
		}

		for i, pod := range pods {
			var p corev1.Pod
			Expect(s.c.Get(ctx, pod, &p)).To(Succeed())
			AddReportEntry("sidecar restarts "+pod.Name, fmt.Sprintf("before %d, after %d", restartsBefore[i], sidecarRestarts(&p)))
			soft.Expect(sidecarRestarts(&p)).To(Equal(restartsBefore[i]), "restarts of the sidecar of %s", pod.Name)
		}
		Expect(breaches).To(BeEmpty())
	}, SpecTimeout(20*time.Minute))
}
