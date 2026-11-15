package queries_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

var _ = DescribeTable("SeriesID keeps series apart",
	func(aMetric string, aLabels []queries.Label, bMetric string, bLabels []queries.Label, same bool) {
		a := queries.SeriesID(aMetric, aLabels)
		b := queries.SeriesID(bMetric, bLabels)
		if same {
			Expect(a).To(Equal(b))
		} else {
			Expect(a).NotTo(Equal(b))
		}
	},
	Entry("a value holding '=' against a key holding '='", "m",
		[]queries.Label{{Key: "a", Value: "b=c"}}, "m",
		[]queries.Label{{Key: "a=b", Value: "c"}}, false),
	Entry("a metric holding NUL and a key against a label with an empty value", "m\x00a", nil, "m",
		[]queries.Label{{Key: "a", Value: ""}}, false),
	Entry("no labels against one empty label", "m", nil, "m",
		[]queries.Label{{Key: "", Value: ""}}, false),
	Entry("equal series", "m",
		[]queries.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}, "m",
		[]queries.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}, true),
)
