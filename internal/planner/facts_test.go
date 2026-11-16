package planner_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
)

var _ = DescribeTable("The wal_segment_size of the facts",
	func(setting string, want float64, ok bool) {
		got, err := planner.ParseWALSegmentSize(setting)
		if !ok {
			Expect(err).To(MatchError(ContainSubstring("wal_segment_size")))
			return
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(want))
	},
	Entry("16 MiB", "16777216", 16777216.0, true),
	Entry("1 GiB", "1073741824", 1073741824.0, true),
	Entry("zero fails the round", "0", 0.0, false),
	Entry("negative fails the round", "-1", 0.0, false),
	Entry("unparsable fails the round", "16MB", 0.0, false),
	Entry("empty fails the round", "", 0.0, false),
	Entry("infinite fails the round", "Inf", 0.0, false),
	Entry("NaN fails the round", "NaN", 0.0, false),
)
