package selfmetrics_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSelfMetrics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Self Metrics Suite")
}
