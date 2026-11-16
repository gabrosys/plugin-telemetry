//go:build e2e

package resilience_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestResilience(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Resilience E2E Suite")
}
