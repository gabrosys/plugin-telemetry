package framework_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("WaitUntil", func() {
	It("returns at once for a time already past", func(ctx SpecContext) {
		start := time.Now()
		Expect(framework.WaitUntil(ctx, start.Add(-time.Second))).To(Succeed())
		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
	})

	It("returns at the time", func(ctx SpecContext) {
		until := time.Now().Add(50 * time.Millisecond)
		Expect(framework.WaitUntil(ctx, until)).To(Succeed())
		Expect(time.Now()).NotTo(BeTemporally("<", until))
	})

	It("returns the context's error when it ends first", func(ctx SpecContext) {
		short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		Expect(framework.WaitUntil(short, time.Now().Add(time.Hour))).To(MatchError(context.DeadlineExceeded))
	})
})
