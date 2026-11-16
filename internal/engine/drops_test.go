package engine_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
)

var _ = Describe("Dropped series", func() {
	It("counts what the producer drops by reason", func(ctx SpecContext) {
		got, err := engine.DroppedSeries(ctx)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[string]int64{"duplicate": 1, "conversion": 1}))
	})
})
