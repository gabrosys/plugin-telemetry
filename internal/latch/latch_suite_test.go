package latch_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLatch(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Latch Suite")
}
