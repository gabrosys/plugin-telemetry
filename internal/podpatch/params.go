// Package podpatch computes the sidecar patch for PostgreSQL instance Pods.
package podpatch

import (
	"errors"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/resource"
)

// AuthMode selects how the sidecar connects to PostgreSQL.
type AuthMode string

const (
	// AuthPeer connects over the Unix socket with peer authentication.
	AuthPeer AuthMode = "peer"
	// AuthPassword connects over TCP with a password.
	AuthPassword AuthMode = "password"
)

const (
	defaultCPU    = "50m"
	defaultMemory = "64Mi"
	minCPU        = "10m"
	minMemory     = "32Mi"
)

const (
	keyExporter       = "exporter"
	keyAuth           = "auth"
	keyPasswordSecret = "passwordSecret"
	keyCPU            = "sidecarCPU"
	keyMemory         = "sidecarMemory"
)

var (
	// ErrInvalidParameters is returned when a plugin parameter has an invalid value.
	ErrInvalidParameters = errors.New("invalid plugin parameters")
	// ErrNoScratchVolume is returned when a peer-mode Pod has no scratch-data volume.
	ErrNoScratchVolume = errors.New("pod has no scratch-data volume")
)

// Params are the plugin parameters that shape the Pod.
type Params struct {
	Auth   AuthMode
	CPU    resource.Quantity
	Memory resource.Quantity
}

// ParseParams reads the Cluster's plugin parameters. Known keys are exporter,
// auth, passwordSecret, sidecarCPU and sidecarMemory; any other key is returned
// in ignored, sorted.
func ParseParams(raw map[string]string) (p Params, ignored []string, err error) {
	p = Params{
		Auth:   AuthPeer,
		CPU:    resource.MustParse(defaultCPU),
		Memory: resource.MustParse(defaultMemory),
	}

	for key := range raw {
		switch key {
		case keyExporter, keyAuth, keyPasswordSecret, keyCPU, keyMemory:
		default:
			ignored = append(ignored, key)
		}
	}
	slices.Sort(ignored)

	if v, ok := raw[keyAuth]; ok {
		switch mode := AuthMode(v); mode {
		case AuthPeer, AuthPassword:
			p.Auth = mode
		default:
			return Params{}, nil, fmt.Errorf("%w: %s %q must be %q or %q", ErrInvalidParameters, keyAuth, v, AuthPeer, AuthPassword)
		}
	}

	if v, ok := raw[keyCPU]; ok {
		if p.CPU, err = parseQuantity(keyCPU, v, minCPU); err != nil {
			return Params{}, nil, err
		}
	}

	if v, ok := raw[keyMemory]; ok {
		if p.Memory, err = parseQuantity(keyMemory, v, minMemory); err != nil {
			return Params{}, nil, err
		}
	}

	return p, ignored, nil
}

func parseQuantity(key, value, floor string) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("%w: %s %q: %w", ErrInvalidParameters, key, value, err)
	}

	if q.Sign() < 0 || q.Cmp(resource.MustParse(floor)) < 0 {
		return resource.Quantity{}, fmt.Errorf("%w: %s %q is below %s", ErrInvalidParameters, key, value, floor)
	}

	return q, nil
}
