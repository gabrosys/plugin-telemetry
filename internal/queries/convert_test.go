package queries_test

import (
	"math"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

var _ = Describe("conversions", func() {
	DescribeTable("toFloat",
		func(in any, want float64, wantOK bool) {
			got, ok := queries.ToFloat(in)
			Expect(ok).To(Equal(wantOK))
			if math.IsNaN(want) {
				Expect(math.IsNaN(got)).To(BeTrue())
			} else {
				Expect(got).To(Equal(want))
			}
		},
		Entry("int64", int64(3), 3.0, true),
		Entry("float64", 2.5, 2.5, true),
		Entry("true", true, 1.0, true),
		Entry("false", false, 0.0, true),
		Entry("time", time.Unix(1700000000, 0), 1700000000.0, true),
		Entry("text", "12.5", 12.5, true),
		Entry("bytes", []byte("12.5"), 12.5, true),
		Entry("nil", nil, math.NaN(), true),
		Entry("bad text", "abc", math.NaN(), false),
		Entry("bad bytes", []byte("abc"), math.NaN(), false),
		Entry("other type", struct{}{}, math.NaN(), false),
	)

	DescribeTable("toUint",
		func(in any, want uint64, wantOK bool) {
			got, ok := queries.ToUint(in)
			Expect(ok).To(Equal(wantOK))
			Expect(got).To(Equal(want))
		},
		Entry("uint64", uint64(7), uint64(7), true),
		Entry("int64", int64(7), uint64(7), true),
		Entry("float64", 7.9, uint64(7), true),
		Entry("time", time.Unix(1700000000, 0), uint64(1700000000), true),
		Entry("text", "7", uint64(7), true),
		Entry("bytes", []byte("7"), uint64(7), true),
		Entry("true", true, uint64(1), true),
		Entry("false", false, uint64(0), true),
		Entry("nil", nil, uint64(0), true),
		Entry("bad text", "7.5", uint64(0), false),
		Entry("other type", struct{}{}, uint64(0), false),
	)

	DescribeTable("toString",
		func(in any, want string, wantOK bool) {
			got, ok := queries.ToString(in)
			Expect(ok).To(Equal(wantOK))
			Expect(got).To(Equal(want))
		},
		Entry("nil", nil, "", true),
		Entry("time", time.Unix(1700000000, 0), "1700000000", true),
		Entry("true", true, "true", true),
		Entry("false", false, "false", true),
		Entry("float64", 0.5, "0.5", true),
		Entry("int64", int64(42), "42", true),
		Entry("text", "x", "x", true),
		Entry("bytes", []byte("x"), "x", true),
		Entry("other type", struct{}{}, "", false),
	)
})
