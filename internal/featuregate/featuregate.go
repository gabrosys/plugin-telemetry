// Package featuregate is a small registry of named feature gates set from a flag.
package featuregate

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// Stage is the maturity of a gate and decides its default.
type Stage string

const (
	// StageAlpha gates are off by default.
	StageAlpha Stage = "alpha"
	// StageBeta gates are on by default.
	StageBeta Stage = "beta"
	// StageStable gates are always on and cannot be disabled.
	StageStable Stage = "stable"
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9.-]+$`)

// Gate is a registered feature gate.
type Gate struct {
	ID          string
	Description string
	Stage       Stage
}

// GateState is a gate with its current value.
type GateState struct {
	ID          string
	Description string
	Stage       Stage
	Enabled     bool
}

// Registry holds gates and their overrides.
type Registry struct {
	mu        sync.RWMutex     // guards gates and overrides
	gates     map[string]*Gate // by ID
	overrides map[string]bool  // explicit +id / -id from Set
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{gates: map[string]*Gate{}, overrides: map[string]bool{}}
}

// Register adds a gate. The id may contain letters, digits, dots and hyphens.
func (r *Registry) Register(id, description string, stage Stage) (*Gate, error) {
	if !idPattern.MatchString(id) {
		return nil, fmt.Errorf("invalid feature gate id %q: use letters, digits, dots and hyphens", id)
	}
	switch stage {
	case StageAlpha, StageBeta, StageStable:
	default:
		return nil, fmt.Errorf("feature gate %q: unknown stage %q", id, stage)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.gates[id]; ok {
		return nil, fmt.Errorf("feature gate %q already registered", id)
	}
	g := &Gate{ID: id, Description: description, Stage: stage}
	r.gates[id] = g
	return g, nil
}

// Set applies a comma-separated spec such as "+a,-b,c"; a bare id enables the gate.
// Nothing is applied if any item is invalid.
func (r *Registry) Set(spec string) error {
	pending := map[string]bool{}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		enable := true
		switch item[0] {
		case '+':
			item = item[1:]
		case '-':
			enable = false
			item = item[1:]
		}
		g, ok := r.gates[item]
		if !ok {
			return fmt.Errorf("unknown feature gate %q", item)
		}
		if !enable && g.Stage == StageStable {
			return fmt.Errorf("stable feature gate %q cannot be disabled", item)
		}
		pending[item] = enable
	}
	for id, v := range pending {
		r.overrides[id] = v
	}
	return nil
}

// Enabled reports whether g is on. A nil gate is off.
func (r *Registry) Enabled(g *Gate) bool {
	if g == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.enabledLocked(g)
}

func (r *Registry) enabledLocked(g *Gate) bool {
	if g.Stage == StageStable {
		return true
	}
	if v, ok := r.overrides[g.ID]; ok {
		return v
	}
	return g.Stage == StageBeta
}

// List returns every gate with its state, sorted by ID.
func (r *Registry) List() []GateState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]GateState, 0, len(r.gates))
	for _, g := range r.gates {
		out = append(out, GateState{ID: g.ID, Description: g.Description, Stage: g.Stage, Enabled: r.enabledLocked(g)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
