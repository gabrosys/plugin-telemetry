package operator

// ImageEventHandler exposes imageEventHandler to the external test package.
var ImageEventHandler = imageEventHandler

// Exposed for manager_test.go.
var (
	NewScheme      = newScheme
	ManagerOptions = managerOptions
)

// Exposed for controller_test.go.
var (
	SpecChanged           = specChanged
	ExportersToClusters   = exportersToClusters
	IdentityNameToCluster = identityNameToCluster
)

// Exposed for metrics_test.go.
var CollectMetrics = (*PluginMetrics).collect
