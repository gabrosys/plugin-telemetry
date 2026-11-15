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
	SpecChanged         = specChanged
	OwnedObjects        = ownedObjects
	ExportersToClusters = exportersToClusters
)

// Exposed for metrics_test.go.
var CollectMetrics = (*PluginMetrics).collect
