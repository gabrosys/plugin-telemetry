package metadata_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

var _ = Describe("identity names", func() {
	It("names the ServiceAccount after the Cluster", func() {
		Expect(metadata.ServiceAccountName("c")).To(Equal("c.telemetry"))
	})

	It("names the token Secret after the ServiceAccount", func() {
		Expect(metadata.TokenSecretName("c")).To(Equal("c.telemetry-token"))
	})
})
