//go:build e2e

package resilience_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
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
	plateauWindow      = time.Minute
	plateauTolerance   = 0.02
	plateauMax         = 10 * time.Minute
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

// series formats SampleUsage readings, taken in pod order, as one line per tick: the offset from the first tick,
// then each pod's working set and RSS in bytes.
func series(readings []framework.Usage, pods int) string {
	var b strings.Builder
	for i, u := range readings {
		if i%pods == 0 {
			fmt.Fprintf(&b, "\nt+%s", time.Duration(i/pods)*usageEvery)
		}
		fmt.Fprintf(&b, " ws=%d rss=%d", u.WorkingSet, u.RSS)
	}

	return b.String()
}

// readyCollector reports whether the collector Service has a ready endpoint.
func (s *suite) readyCollector(ctx context.Context) (bool, error) {
	ready, err := framework.ReadyEndpoints(ctx, s.c, collectorNamespace, collectorService)

	return len(ready) > 0, err
}

// awaitPlateau samples the sidecars in up to plateauMax/plateauWindow windows until, for every pod, the median
// working set of a window is within plateauTolerance of the previous window's. A sidecar still climbing after the
// last window fails the spec. The windows are recorded once, however the wait ends.
func (s *suite) awaitPlateau(ctx context.Context, pods []types.NamespacedName) {
	GinkgoHelper()

	var recorded strings.Builder
	defer func() { AddReportEntry("plateau usage series", recorded.String()) }()

	var prev [][]uint64
	var moving []string
	for range int(plateauMax / plateauWindow) {
		readings, err := framework.SampleUsage(ctx, s.cfg, pods, metadata.SidecarContainerName, usageEvery, plateauWindow)
		fmt.Fprintf(&recorded, "\nwindow ending %s:%s", time.Now().Format(time.RFC3339), series(readings, len(pods)))
		Expect(err).NotTo(HaveOccurred())
		cur, _ := perPod(readings, len(pods))

		if prev != nil {
			moving = moving[:0]
			for i, pod := range pods {
				if !framework.Settled(prev[i], cur[i], plateauTolerance) {
					moving = append(moving, fmt.Sprintf("%s: median %d then %d bytes", pod.Name,
						framework.StatOf(prev[i]).Median, framework.StatOf(cur[i]).Median))
				}
			}
			if len(moving) == 0 {
				return
			}
		}
		prev = cur
	}
	Expect(moving).To(BeEmpty(), "sidecar memory still climbing with the collector up after %s", plateauMax)
}

// collectorSpecs adds ENG-05: the collector down for ten minutes.
func collectorSpecs(s *suite) {
	It("ENG-05: with the collector down for 10 minutes each sidecar's median working set stays within 10 % of its steady median "+
		"and exports resume within two export intervals", func(ctx SpecContext) {
		s.healthy(ctx)

		instances, err := framework.InstancePods(ctx, s.c, s.cluster)
		Expect(err).NotTo(HaveOccurred())
		Expect(instances).To(HaveLen(2))
		pods := make([]types.NamespacedName, len(instances))
		restartsBefore := make([]int32, len(instances))
		for i := range instances {
			pods[i] = types.NamespacedName{Namespace: instances[i].Namespace, Name: instances[i].Name}
			restartsBefore[i] = framework.SidecarRestarts(&instances[i])
		}

		reader := framework.NewCollectorReader(s.cfg)
		Expect(framework.WaitUntil(ctx, time.Now().Add(exportsWarmup))).To(Succeed())

		s.awaitPlateau(ctx, pods)

		readings, err := framework.SampleUsage(ctx, s.cfg, pods, metadata.SidecarContainerName, usageEvery, steadyWindow)
		Expect(err).NotTo(HaveOccurred())
		steadyWS, steadyRSS := perPod(readings, len(pods))
		AddReportEntry("steady usage series", series(readings, len(pods)))

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
		AddReportEntry("outage usage series", series(readings, len(pods)))

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
			steady, outage := framework.StatOf(steadyWS[i]).Median, framework.StatOf(outageWS[i]).Median
			AddReportEntry("working set "+pod.Name, fmt.Sprintf("steady median %d, outage median %d, min %d, max %d bytes",
				steady, outage, slices.Min(outageWS[i]), slices.Max(outageWS[i])))
			AddReportEntry("rssBytes "+pod.Name, fmt.Sprintf("steady %d, min %d, max %d bytes",
				framework.StatOf(steadyRSS[i]).Median, slices.Min(outageRSS[i]), slices.Max(outageRSS[i])))
			soft.Expect(float64(outage)).To(BeNumerically("<=", growthLimit*float64(steady)), "median working set of %s", pod.Name)
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
			AddReportEntry("sidecar restarts "+pod.Name, fmt.Sprintf("before %d, after %d", restartsBefore[i], framework.SidecarRestarts(&p)))
			soft.Expect(framework.SidecarRestarts(&p)).To(Equal(restartsBefore[i]), "restarts of the sidecar of %s", pod.Name)
		}
		Expect(breaches).To(BeEmpty())
	}, SpecTimeout(30*time.Minute))
}
