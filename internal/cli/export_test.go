package cli

import (
	"context"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/kubesource"
)

// FormatSamples is formatSamples.
var FormatSamples = formatSamples

// SnapshotTimeout is snapshotTimeout.
const SnapshotTimeout = snapshotTimeout

// SetMemlimit replaces the function that applies the memory limit and returns the function that restores it.
func SetMemlimit(f func(func(string) ([]byte, error), func(string) string) (int64, error)) (restore func()) {
	prev := setMemoryLimit
	setMemoryLimit = f
	return func() { setMemoryLimit = prev }
}

// KubeSource is kubeSource.
type KubeSource = kubeSource

// SetKubeSources replaces the constructors of the Kubernetes source and of its snapshot, and returns the function that restores them.
func SetKubeSources(
	run func(kubesource.Options) KubeSource,
	snap func(context.Context, kubesource.Options) (engine.Config, error),
) (restore func()) {
	prevRun, prevSnap := newKubeSource, snapshotKubeSource
	newKubeSource, snapshotKubeSource = run, snap
	return func() { newKubeSource, snapshotKubeSource = prevRun, prevSnap }
}
