package preflight_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPreflight(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Preflight Suite")
}
