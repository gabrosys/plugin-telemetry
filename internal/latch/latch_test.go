package latch_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/latch"
)

var _ = Describe("Latch", func() {
	var l *latch.Latch[string]

	BeforeEach(func() { l = &latch.Latch[string]{} })

	It("reports a key as new only the first time it is set", func() {
		Expect(l.Set("a")).To(BeTrue())
		Expect(l.Set("a")).To(BeFalse())
		Expect(l.Set("b")).To(BeTrue())
	})

	It("reports whether Clear and ClearAll cleared a key", func() {
		Expect(l.Clear("a")).To(BeFalse())
		l.Set("a")
		l.Set("b")

		Expect(l.Clear("a")).To(BeTrue())
		Expect(l.Clear("a")).To(BeFalse())
		Expect(l.ClearAll()).To(BeTrue())
		Expect(l.ClearAll()).To(BeFalse())
		Expect(l.Set("b")).To(BeTrue(), "ClearAll forgot b")
	})

	It("keeps the keys Retain accepts and reports whether it dropped one", func() {
		l.Set("a")
		l.Set("b")

		Expect(l.Retain(func(k string) bool { return k == "a" })).To(BeTrue())
		Expect(l.Retain(func(k string) bool { return k == "a" })).To(BeFalse())
		Expect(l.Set("a")).To(BeFalse(), "a was kept")
		Expect(l.Set("b")).To(BeTrue(), "b was dropped")
	})
})
