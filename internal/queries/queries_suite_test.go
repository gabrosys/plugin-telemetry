package queries_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// testdata/default-monitoring-*.yaml are CNPG config/manager/default-monitoring.yaml at v1.30.1 and at main bc0889a.

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Queries Suite")
}
