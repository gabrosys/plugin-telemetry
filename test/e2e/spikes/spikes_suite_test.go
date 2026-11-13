//go:build e2e

package spikes_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSpikes(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Spikes E2E Suite")
}
