package clusterconfig

import (
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

// EnabledPlugin returns the Cluster's spec.plugins entry for this plugin; false when it is absent or
// its enabled field is false.
func EnabledPlugin(c *apiv1.Cluster) (apiv1.PluginConfiguration, bool) {
	for _, p := range c.Spec.Plugins {
		if p.Name == metadata.PluginName && (p.Enabled == nil || *p.Enabled) {
			return p, true
		}
	}

	return apiv1.PluginConfiguration{}, false
}
