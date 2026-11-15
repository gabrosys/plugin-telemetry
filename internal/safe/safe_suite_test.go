package safe_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/safe"
)

func TestSafe(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Safe Suite")
}

var _ = BeforeEach(safe.Reset)
