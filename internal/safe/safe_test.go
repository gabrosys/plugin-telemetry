package safe_test

import (
	"sync"

	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/safe"
)

var _ = Describe("safe", func() {
	var (
		mu    sync.Mutex // guards lines
		lines []string
	)
	logger := funcr.New(func(_, args string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, args)
	}, funcr.Options{})
	logged := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}

	BeforeEach(func() {
		mu.Lock()
		defer mu.Unlock()
		lines = nil
	})

	It("Call returns true after a panic and logs what, the value and the stack", func() {
		Expect(safe.Call(logger, "the thing", func() { panic("boom") })).To(BeTrue())

		got := logged()
		Expect(got).To(HaveLen(1))
		Expect(got[0]).To(ContainSubstring(`"msg"="recovered from a panic"`))
		Expect(got[0]).To(ContainSubstring(`"in"="the thing"`))
		Expect(got[0]).To(ContainSubstring("panic: boom"))
		Expect(got[0]).To(ContainSubstring(`"stack"=`))
		Expect(got[0]).To(ContainSubstring("safe_test.go"))
	})

	It("Call returns false and logs nothing when fn returns", func() {
		ran := false
		Expect(safe.Call(logger, "the thing", func() { ran = true })).To(BeFalse())
		Expect(ran).To(BeTrue())
		Expect(logged()).To(BeEmpty())
	})

	It("Go recovers on its goroutine", func() {
		safe.Go(logger, "the goroutine", func() { panic("boom") })

		Eventually(logged).Should(HaveLen(1))
		Expect(logged()[0]).To(ContainSubstring(`"in"="the goroutine"`))
	})
})
