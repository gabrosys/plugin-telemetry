//go:build e2e

package resilience_test

import (
	"fmt"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	primaryOnlyMetric = "cnpg_resilience_primary_one"
	switchoverTimeout = 3 * time.Minute
	switchoverPoll    = time.Second
	// planCycle is the exporter's planInterval, one planning cycle.
	planCycle = 30 * time.Second
	// firstRunJitter and exportEvery are observation lag on top of the planning cycle.
	firstRunJitter = 10 * time.Second
	exportEvery    = 10 * time.Second
	boundMargin    = 2 * time.Second
)

// switchoverSpecs adds the spec that checks the primary-only metric moves with the primary.
func switchoverSpecs(s *suite) {
	It("after a switchover the primary-only metric comes only from the new primary within one planning cycle plus observation lag",
		func(ctx SpecContext) {
			s.healthy(ctx)

			var cluster apiv1.Cluster
			Expect(s.c.Get(ctx, s.cluster, &cluster)).To(Succeed())
			old := cluster.Status.CurrentPrimary
			instances, err := framework.InstancePods(ctx, s.c, s.cluster)
			Expect(err).NotTo(HaveOccurred())
			var target string
			for i := range instances {
				if instances[i].Name != old {
					target = instances[i].Name
				}
			}
			Expect(target).NotTo(BeEmpty(), "no instance besides %s", old)

			reader := framework.NewCollectorReader(s.cfg)
			start := time.Now()
			Expect(framework.Switchover(ctx, s.c, &cluster, target)).To(Succeed())

			var promoted time.Time
			Eventually(func(ctx SpecContext) (string, error) {
				var c apiv1.Cluster
				if err := s.c.Get(ctx, s.cluster, &c); err != nil {
					return "", err
				}
				if c.Status.CurrentPrimary == target && promoted.IsZero() {
					promoted = time.Now()
				}

				return c.Status.CurrentPrimary, nil
			}).WithContext(ctx).WithTimeout(switchoverTimeout).WithPolling(switchoverPoll).Should(Equal(target))

			bound := promoted.Add(planCycle + firstRunJitter + exportEvery + boundMargin)
			Expect(framework.WaitUntil(ctx, bound.Add(3*exportEvery))).To(Succeed())

			lines, err := reader.Since(ctx, start)
			Expect(err).NotTo(HaveOccurred())

			var oldLast, targetFirst time.Time
			after := map[string][]time.Time{}
			for _, e := range otlp.Exports(lines) {
				if e.Resource["service.namespace"] != s.ns || e.Resource["service.name"] != clusterName {
					continue
				}
				if !slices.ContainsFunc(e.Series, func(series string) bool { return strings.HasPrefix(series, primaryOnlyMetric+"{") }) {
					continue
				}
				pod := e.Resource["k8s.pod.name"]
				if pod == old && e.Time.After(oldLast) {
					oldLast = e.Time
				}
				if pod == target && e.Time.After(promoted) && (targetFirst.IsZero() || e.Time.Before(targetFirst)) {
					targetFirst = e.Time
				}
				if !e.Time.Before(bound) {
					after[pod] = append(after[pod], e.Time)
				}
			}

			AddReportEntry("switchover", fmt.Sprintf("old: %s, target: %s, T (currentPrimary is the target): %s after the patch",
				old, target, promoted.Sub(start)))
			AddReportEntry("bound", fmt.Sprintf("T + %s = one planning cycle (%s) + first-run jitter (%s) + export interval (%s) + %s; "+
				"the jitter and the export interval are observation lag", bound.Sub(promoted), planCycle, firstRunJitter, exportEvery, boundMargin))
			AddReportEntry("last point from old", delay(oldLast, promoted))
			AddReportEntry("first point from target after T", delay(targetFirst, promoted))

			Expect(after).To(HaveLen(1), "pods with a %s point since the bound: %v", primaryOnlyMetric, after)
			Expect(after).To(HaveKey(target))
			Expect(len(after[target])).To(BeNumerically(">=", 2), "points of %s since the bound", target)
		}, SpecTimeout(6*time.Minute))
}

// delay formats at relative to promoted, or "none" for a zero at.
func delay(at, promoted time.Time) string {
	if at.IsZero() {
		return "none"
	}

	return fmt.Sprintf("T%+s", at.Sub(promoted).Round(time.Millisecond))
}
