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
	// PeerUID is the OS user the sidecar runs as in peer mode.
	PeerUID = int64(65534)
)

// ServiceAccountName returns the name of the cluster's sidecar ServiceAccount.
func ServiceAccountName(cluster string) string {
	return cluster + "-telemetry"
}

// TokenSecretName returns the name of the Secret holding the sidecar's token.
func TokenSecretName(cluster string) string {
	return cluster + "-telemetry-token"
}
