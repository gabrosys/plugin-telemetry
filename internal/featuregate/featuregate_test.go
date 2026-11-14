package featuregate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/featuregate"
)

var _ = Describe("Registry", func() {
	var r *featuregate.Registry

	BeforeEach(func() {
		r = featuregate.NewRegistry()
	})

	It("enables gates by stage default", func() {
		alpha, err := r.Register("a", "alpha gate", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())
		beta, err := r.Register("b", "beta gate", featuregate.StageBeta)
		Expect(err).ToNot(HaveOccurred())
		stable, err := r.Register("s", "stable gate", featuregate.StageStable)
		Expect(err).ToNot(HaveOccurred())

		Expect(r.Enabled(alpha)).To(BeFalse())
		Expect(r.Enabled(beta)).To(BeTrue())
		Expect(r.Enabled(stable)).To(BeTrue())
	})

	It("applies a +a,-b spec", func() {
		a, err := r.Register("a", "", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())
		b, err := r.Register("b", "", featuregate.StageBeta)
		Expect(err).ToNot(HaveOccurred())
		c, err := r.Register("c", "", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())

		Expect(r.Set("+a,-b,c")).To(Succeed())

		Expect(r.Enabled(a)).To(BeTrue())
		Expect(r.Enabled(b)).To(BeFalse())
		Expect(r.Enabled(c)).To(BeTrue())
	})

	It("treats an empty spec as a no-op", func() {
		b, err := r.Register("b", "", featuregate.StageBeta)
		Expect(err).ToNot(HaveOccurred())
		Expect(r.Set("")).To(Succeed())
		Expect(r.Enabled(b)).To(BeTrue())
	})

	It("rejects an unknown id and applies nothing", func() {
		a, err := r.Register("a", "", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())

		err = r.Set("+a,+nope")

		Expect(err).To(MatchError(ContainSubstring(`unknown feature gate "nope"`)))
		Expect(r.Enabled(a)).To(BeFalse())
	})

	It("refuses to disable a stable gate", func() {
		s, err := r.Register("s", "", featuregate.StageStable)
		Expect(err).ToNot(HaveOccurred())

		Expect(r.Set("-s")).To(MatchError(ContainSubstring(`stable feature gate "s"`)))
		Expect(r.Set("+s")).To(Succeed())
		Expect(r.Enabled(s)).To(BeTrue())
	})

	It("rejects registering an id twice", func() {
		_, err := r.Register("a", "", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())

		_, err = r.Register("a", "", featuregate.StageBeta)

		Expect(err).To(MatchError(ContainSubstring(`feature gate "a" already registered`)))
	})

	DescribeTable("rejects invalid ids and stages",
		func(id string, stage featuregate.Stage) {
			_, err := r.Register(id, "", stage)
			Expect(err).To(HaveOccurred())
		},
		Entry("empty id", "", featuregate.StageAlpha),
		Entry("space", "a b", featuregate.StageAlpha),
		Entry("underscore", "a_b", featuregate.StageAlpha),
		Entry("plus prefix", "+a", featuregate.StageAlpha),
		Entry("unknown stage", "a", featuregate.Stage("gamma")),
	)

	It("accepts dots, hyphens and digits in ids", func() {
		_, err := r.Register("metrics.v2-beta1", "", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())
	})

	It("lists gates sorted by id with their state", func() {
		_, err := r.Register("z", "last", featuregate.StageBeta)
		Expect(err).ToNot(HaveOccurred())
		_, err = r.Register("a", "first", featuregate.StageAlpha)
		Expect(err).ToNot(HaveOccurred())
		Expect(r.Set("+a,-z")).To(Succeed())

		Expect(r.List()).To(Equal([]featuregate.GateState{
			{ID: "a", Description: "first", Stage: featuregate.StageAlpha, Enabled: true},
			{ID: "z", Description: "last", Stage: featuregate.StageBeta, Enabled: false},
		}))
	})

	It("reports a nil gate as disabled", func() {
		Expect(r.Enabled(nil)).To(BeFalse())
	})
})
