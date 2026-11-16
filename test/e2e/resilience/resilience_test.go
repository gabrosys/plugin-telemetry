//go:build e2e

package resilience_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("resilience", Ordered, ContinueOnFailure, func() {
	s := &suite{}

	BeforeAll(s.setUp, NodeTimeout(12*time.Minute))

	It("runs against the requested CNPG version", func(ctx SpecContext) {
		s.healthy(ctx)
		img, err := framework.OperatorImage(ctx, s.c)
		Expect(err).NotTo(HaveOccurred())
		AddReportEntry("cnpg-operator-image", img)
		Expect(framework.MatchesCNPGVersion(img, s.env.CNPGVersion)).To(BeTrue(), img)
	}, SpecTimeout(time.Minute))

	It("steady traffic produces no failed hook call", func(ctx SpecContext) {
		s.healthy(ctx)

		start := time.Now()
		stop := s.traffic(ctx)
		defer stop()
		Expect(framework.WaitUntil(ctx, start.Add(20*time.Second))).To(Succeed())
		stop()

		s.expectNoFailedHookCall(ctx, "steady traffic", start)
	}, SpecTimeout(5*time.Minute))
})
