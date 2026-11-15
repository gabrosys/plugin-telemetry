package clusterconfig_test

import (
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

var _ = Describe("EnabledPlugin", func() {
	cluster := func(plugins ...apiv1.PluginConfiguration) *apiv1.Cluster {
		c := newCluster()
		c.Spec.Plugins = plugins
		return c
	}

	It("returns the entry named after the plugin, enabled by default", func() {
		p, ok := clusterconfig.EnabledPlugin(cluster(
			apiv1.PluginConfiguration{Name: "other"},
			apiv1.PluginConfiguration{Name: metadata.PluginName, Parameters: map[string]string{"exporter": "x"}},
		))
		Expect(ok).To(BeTrue())
		Expect(p.Parameters).To(HaveKeyWithValue("exporter", "x"))
	})

	It("returns the entry when enabled is true", func() {
		_, ok := clusterconfig.EnabledPlugin(cluster(apiv1.PluginConfiguration{Name: metadata.PluginName, Enabled: ptr.To(true)}))
		Expect(ok).To(BeTrue())
	})

	It("reports false when the entry is disabled or absent", func() {
		_, ok := clusterconfig.EnabledPlugin(cluster(apiv1.PluginConfiguration{Name: metadata.PluginName, Enabled: ptr.To(false)}))
		Expect(ok).To(BeFalse())
		_, ok = clusterconfig.EnabledPlugin(cluster(apiv1.PluginConfiguration{Name: "other"}))
		Expect(ok).To(BeFalse())
		_, ok = clusterconfig.EnabledPlugin(cluster())
		Expect(ok).To(BeFalse())
	})
})
