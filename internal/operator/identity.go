package operator

import (
	"context"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

const repositoryURL = "https://github.com/gabrosys/plugin-telemetry"

// Identity tells CloudNativePG what the plugin is and offers.
type Identity struct {
	identity.UnimplementedIdentityServer
}

// GetPluginMetadata returns the plugin's descriptive metadata.
func (Identity) GetPluginMetadata(context.Context, *identity.GetPluginMetadataRequest) (*identity.GetPluginMetadataResponse, error) {
	return &identity.GetPluginMetadataResponse{
		Name:          metadata.PluginName,
		Version:       version.Version,
		DisplayName:   "Telemetry",
		Description:   "Exports PostgreSQL metrics from CloudNativePG instances over OTLP",
		ProjectUrl:    repositoryURL,
		RepositoryUrl: repositoryURL,
		License:       "Apache-2.0",
		LicenseUrl:    repositoryURL + "/blob/main/LICENSE",
		Maturity:      "alpha",
		Vendor:        "gabrosys",
	}, nil
}

// GetPluginCapabilities declares the lifecycle service.
func (Identity) GetPluginCapabilities(context.Context, *identity.GetPluginCapabilitiesRequest) (*identity.GetPluginCapabilitiesResponse, error) {
	return &identity.GetPluginCapabilitiesResponse{
		Capabilities: []*identity.PluginCapability{{
			Type: &identity.PluginCapability_Service_{
				Service: &identity.PluginCapability_Service{
					Type: identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE,
				},
			},
		}},
	}, nil
}

// Probe reports the plugin as ready.
func (Identity) Probe(context.Context, *identity.ProbeRequest) (*identity.ProbeResponse, error) {
	return &identity.ProbeResponse{Ready: true}, nil
}
