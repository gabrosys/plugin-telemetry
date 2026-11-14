package operator

// ImageEventHandler exposes imageEventHandler to the external test package.
var ImageEventHandler = imageEventHandler

// Exposed for manager_test.go.
var (
	NewScheme      = newScheme
	ManagerOptions = managerOptions
)
