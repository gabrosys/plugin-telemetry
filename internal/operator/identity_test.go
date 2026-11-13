package operator_test

import (
	"context"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

var _ = Describe("Identity", func() {
	var srv operator.Identity

	It("describes the plugin", func(ctx context.Context) {
		resp, err := srv.GetPluginMetadata(ctx, &identity.GetPluginMetadataRequest{})
		Expect(err).NotTo(HaveOccurred())

		Expect(resp.GetName()).To(Equal(metadata.PluginName))
		Expect(resp.GetVersion()).To(Equal(version.Version))
		Expect(resp.GetLicense()).To(Equal("Apache-2.0"))
		Expect(resp.GetLicenseUrl()).To(Equal("https://github.com/gabrosys/plugin-telemetry/blob/main/LICENSE"))
		Expect(resp.GetProjectUrl()).To(Equal("https://github.com/gabrosys/plugin-telemetry"))
		Expect(resp.GetRepositoryUrl()).To(Equal("https://github.com/gabrosys/plugin-telemetry"))
		Expect(resp.GetMaturity()).To(Equal("alpha"))
		Expect(resp.GetVendor()).To(Equal("gabrosys"))
		Expect(resp.GetDisplayName()).NotTo(BeEmpty())
		Expect(resp.GetDescription()).NotTo(BeEmpty())
	})

	It("offers only the lifecycle service", func(ctx context.Context) {
		resp, err := srv.GetPluginCapabilities(ctx, &identity.GetPluginCapabilitiesRequest{})
		Expect(err).NotTo(HaveOccurred())

		Expect(resp.GetCapabilities()).To(HaveLen(1))
		Expect(resp.GetCapabilities()[0].GetService().GetType()).
			To(Equal(identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE))
	})

	It("reports ready", func(ctx context.Context) {
		resp, err := srv.Probe(ctx, &identity.ProbeRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetReady()).To(BeTrue())
	})
})
