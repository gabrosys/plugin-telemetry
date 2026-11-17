//go:build e2e

package collectormetrics_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCollectorMetrics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Collector Metrics E2E Suite")
}
