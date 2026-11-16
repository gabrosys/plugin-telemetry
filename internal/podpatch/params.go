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

// DefaultExporter is the TelemetryExporter a Cluster uses when its plugin parameters name none.
const DefaultExporter = "default"

const (
	defaultCPU    = "50m"
	defaultMemory = "96Mi"
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
	Exporter       string // the TelemetryExporter name; Apply does not read it
	Auth           AuthMode
	PasswordSecret string // password mode only; Apply does not read it
	CPU            resource.Quantity
	Memory         resource.Quantity
}

// Result is the outcome of ParseParams.
type Result struct {
	Params    Params
	Ignored   []string // unknown keys, sorted
	Defaulted []error  // one per invalid sidecarCPU/sidecarMemory value; each wraps ErrInvalidParameters
}

// ParseParams reads the Cluster's plugin parameters. Known keys are exporter,
// auth, passwordSecret, sidecarCPU and sidecarMemory; any other key is returned
// in Ignored, sorted. An invalid sidecarCPU or sidecarMemory falls back to its
// default and is reported in Defaulted. An invalid auth leaves Auth at AuthPeer
// and is returned, wrapping ErrInvalidParameters, together with every other
// parsed field.
func ParseParams(raw map[string]string) (Result, error) {
	r := Result{Params: Params{
		Exporter: DefaultExporter,
		Auth:     AuthPeer,
		CPU:      resource.MustParse(defaultCPU),
		Memory:   resource.MustParse(defaultMemory),
	}}

	for key := range raw {
		switch key {
		case keyExporter, keyAuth, keyPasswordSecret, keyCPU, keyMemory:
		default:
			r.Ignored = append(r.Ignored, key)
		}
	}
	slices.Sort(r.Ignored)

	if v := raw[keyExporter]; v != "" {
		r.Params.Exporter = v
	}
	r.Params.PasswordSecret = raw[keyPasswordSecret]

	var authErr error
	if v, ok := raw[keyAuth]; ok {
		switch mode := AuthMode(v); mode {
		case AuthPeer, AuthPassword:
			r.Params.Auth = mode
		default:
			authErr = fmt.Errorf("%w: %s %q must be %q or %q", ErrInvalidParameters, keyAuth, v, AuthPeer, AuthPassword)
		}
	}

	if v, ok := raw[keyCPU]; ok {
		q, err := parseQuantity(keyCPU, v, minCPU)
		if err != nil {
			r.Defaulted = append(r.Defaulted, err)
		} else {
			r.Params.CPU = q
		}
	}

	if v, ok := raw[keyMemory]; ok {
		q, err := parseQuantity(keyMemory, v, minMemory)
		if err != nil {
			r.Defaulted = append(r.Defaulted, err)
		} else {
			r.Params.Memory = q
		}
	}

	return r, authErr
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
