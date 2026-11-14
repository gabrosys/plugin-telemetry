package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
)

// Changed reports whether the Config's settings or the files they name differ from the last load.
func (r *Replanner) Changed() bool { return r.r.changed() }

// State is what a Steps engine has running.
type State struct {
	Loop, Runner, Exporter, Current bool
	Jobs                            int
}

// Steps drives an engine over a Config on the calling goroutine, without a watcher or a scheduler loop.
type Steps struct {
	e *engine

	mu   sync.Mutex // guards jobs
	jobs int
}

// NewSteps returns Steps over a new engine reading from cfg.
func NewSteps(ctx context.Context, cfg Config) (*Steps, error) {
	e, err := newEngine(ctx, Options{Config: cfg, Getenv: func(string) string { return "" }})
	if err != nil {
		return nil, err
	}
	s := &Steps{e: e}
	update := e.plan.update
	e.plan.update = func(p planner.Plan) {
		s.mu.Lock()
		s.jobs = len(p.Jobs)
		s.mu.Unlock()
		update(p)
	}
	return s, nil
}

// Reload is the engine's reload: it applies the configuration and logs why it did not.
func (s *Steps) Reload(ctx context.Context) { s.e.reload(ctx) }

// Apply loads and applies the configuration and returns the error without logging it.
func (s *Steps) Apply(ctx context.Context) error { return s.e.plan.reload(ctx) }

// SetFacts hands the planner the facts of a round.
func (s *Steps) SetFacts(ctx context.Context, f planner.Facts) { s.e.plan.setFacts(ctx, f) }

// FactsRound runs a facts round against the configured PostgreSQL.
func (s *Steps) FactsRound(ctx context.Context) { s.e.factsRound(ctx, true) }

// SetUp records the outcome of a facts round for cnpg_collector_up.
func (s *Steps) SetUp(ok bool) { s.e.setUp(ok) }

// UpReported reports whether cnpg_collector_up is reported.
func (s *Steps) UpReported(ctx context.Context) (bool, error) {
	_, found, err := s.e.collect(ctx, upMetric)
	return found, err
}

// State returns what the engine has running.
func (s *Steps) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return State{
		Loop: s.e.loop != nil, Runner: s.e.runner != nil, Exporter: s.e.exp != nil,
		Current: s.e.plan.current != nil, Jobs: s.jobs,
	}
}

// Pool returns the pool of the current runner, nil without one.
func (s *Steps) Pool() *pg.Pool {
	if s.e.runner == nil {
		return nil
	}
	return s.e.runner.pool.Pool
}

// Settle waits for the pools a reload replaced or tore down to close.
func (s *Steps) Settle() { s.e.retired.Wait() }

// Close stops what the engine started and shuts the meter provider down.
func (s *Steps) Close(ctx context.Context) error {
	stopTimer(&s.e.factsTimer)
	s.e.stopLoop()
	s.e.retired.Wait()
	if s.e.runner != nil {
		s.e.runner.pool.Close()
	}
	var errs []error
	if s.e.exp != nil {
		errs = append(errs, s.e.exp.Shutdown(ctx))
	}
	errs = append(errs, s.e.mp.Shutdown(ctx))
	return errors.Join(errs...)
}
