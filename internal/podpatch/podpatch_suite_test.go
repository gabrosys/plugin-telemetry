package podpatch_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPodPatch(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pod Patch Suite")
}
