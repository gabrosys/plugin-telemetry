//go:build e2e

package resilience_test

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	pgbenchSeconds = 90
	pgbenchWarmup  = 15 * time.Second
	maxOutage      = 5 * time.Second
)

var pgbenchSummary = regexp.MustCompile(`(?m)^(tps = .*|number of (?:transactions actually processed|failed transactions).*)$`)

// killSidecar kills the primary's sidecar and polls until it restarted and the primary is back in -rw.
// It returns the longest time the primary was missing from -rw.
func (s *suite) killSidecar(ctx context.Context, primary string) time.Duration {
	GinkgoHelper()

	key := client.ObjectKey{Namespace: s.ns, Name: primary}
	var pod corev1.Pod
	Expect(s.c.Get(ctx, key, &pod)).To(Succeed())
	before := framework.SidecarRestarts(&pod)
	Expect(framework.KillContainer(ctx, &pod, metadata.SidecarContainerName)).To(Succeed())

	var (
		longest time.Duration
		outFrom time.Time
		sawOut  bool
	)
	Eventually(func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		if err := s.c.Get(ctx, key, &p); err != nil {
			return false, fmt.Errorf("get pod %s: %w", key, err)
		}
		ready, err := framework.ReadyEndpoints(ctx, s.c, s.ns, clusterName+"-rw")
		if err != nil {
			return false, err
		}

		now := time.Now()
		if slices.Contains(ready, primary) {
			if !outFrom.IsZero() {
				longest = max(longest, now.Sub(outFrom))
				outFrom = time.Time{}
			}

			return sawOut && framework.SidecarRestarts(&p) == before+1, nil
		}
		if outFrom.IsZero() {
			outFrom, sawOut = now, true
		}

		return false, nil
	}).WithContext(ctx).WithTimeout(2*time.Minute).WithPolling(failoverPoll).Should(BeTrue(), "sidecar restart and primary back in -rw")

	return longest
}

// sidecarSpecs adds ENG-04: killing the primary's sidecar under load.
func sidecarSpecs(s *suite) {
	It("ENG-04: killing the primary's sidecar under pgbench keeps the primary out of -rw for at most 5 s", func(ctx SpecContext) {
		s.healthy(ctx)

		var cluster apiv1.Cluster
		Expect(s.c.Get(ctx, s.cluster, &cluster)).To(Succeed())
		primary := cluster.Status.CurrentPrimary

		type result struct {
			logs string
			err  error
		}
		done := make(chan result, 1)
		go func() {
			defer GinkgoRecover()
			pod := framework.PgbenchPod(s.ns, "pgbench", clusterName, s.env.PostgresImage, pgbenchSeconds)
			logs, err := framework.RunPod(ctx, s.c, s.cfg, pod)
			done <- result{logs, err}
		}()

		time.Sleep(pgbenchWarmup)
		first := s.killSidecar(ctx, primary)
		second := s.killSidecar(ctx, primary)
		AddReportEntry("sidecar outage 1", first.String())
		AddReportEntry("sidecar outage 2", second.String())

		res := <-done
		var summary string
		if res.logs != "" {
			summary = strings.Join(pgbenchSummary.FindAllString(res.logs, -1), "\n")
		}
		AddReportEntry("pgbench summary", summary)
		Expect(res.err).NotTo(HaveOccurred(), "pgbench logs: %s", res.logs)

		Expect(s.c.Get(ctx, s.cluster, &cluster)).To(Succeed())
		Expect(cluster.Status.CurrentPrimary).To(Equal(primary))
		Expect(first).To(BeNumerically("<=", maxOutage))
	}, SpecTimeout(6*time.Minute))
}
