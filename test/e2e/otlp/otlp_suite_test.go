package otlp_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOTLP(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E OTLP Suite")
}
