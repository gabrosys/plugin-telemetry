// Package metadata holds the names the plugin shares across packages.
package metadata

const (
	// PluginName is the plugin's name and the domain of its labels and paths.
	PluginName = "telemetry.gabrosys.github.io"
	// SidecarContainerName is the name of the injected sidecar container.
	SidecarContainerName = "otel-metrics"
	// SidecarTokenDir is where the sidecar finds its own ServiceAccount token.
	SidecarTokenDir = "/var/run/secrets/telemetry.gabrosys.github.io/serviceaccount" //nolint:gosec // a mount path, not a credential
	// PodTokenDir is the default ServiceAccount token path in a Pod.
	PodTokenDir = "/var/run/secrets/kubernetes.io/serviceaccount" //nolint:gosec // a mount path, not a credential
	// SocketDir is the PostgreSQL socket directory in the instance Pod.
	SocketDir = "/controller/run"
	// MetricsExporterRole is the PostgreSQL role the sidecar uses in peer mode.
	MetricsExporterRole = "cnpg_metrics_exporter"
	// ManagedByLabel is the label the plugin sets on the objects it creates.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// ManagedByValue is the value of ManagedByLabel.
	ManagedByValue = "plugin-telemetry"
	// PeerUID is the OS user the sidecar runs as in peer mode.
	PeerUID = int64(65534)
	// ScratchVolume is the CloudNativePG instance Pod volume that holds the socket directory.
	ScratchVolume = "scratch-data"
	// TokenVolume is the sidecar's own ServiceAccount token volume.
	TokenVolume = "telemetry-token"
	// TokenMaskVolume is the empty volume that hides the Pod's default token from the sidecar.
	TokenMaskVolume = "telemetry-sa-mask"
)

// identitySuffix ends the names of a Cluster's ServiceAccount, Role and RoleBinding.
const identitySuffix = ".telemetry"

// ServiceAccountName returns the name of the cluster's sidecar ServiceAccount.
func ServiceAccountName(cluster string) string {
	return cluster + identitySuffix
}

// TokenSecretName returns the name of the Secret holding the sidecar's token.
func TokenSecretName(cluster string) string {
	return ServiceAccountName(cluster) + "-token"
}
