// Package scheduler runs planned jobs at their intervals, one worker per database.
package scheduler

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/safe"
)

const (
	// MaxInitialDelay caps the random delay before a job's first run.
	MaxInitialDelay = 10 * time.Second
	// MaxJitter caps the random delay added to each interval.
	MaxJitter = time.Second
)

// ErrAlreadyRunning is returned by Run when it has been called before.
var ErrAlreadyRunning = errors.New("scheduler already running")

// RunFunc runs one job. ctx is derived from the context passed to Run.
type RunFunc func(ctx context.Context, job planner.Job)

// SkipFunc reports that missed runs of job fell due while a run was in progress.
type SkipFunc func(job planner.Job, missed int)

type jobKey struct{ query, database string }

type entry struct {
	job planner.Job
	due time.Time
}

type worker struct {
	wake  chan struct{}
	timer clock.Timer // the timer the worker is waiting on, nil while it is not waiting
}

// Scheduler runs jobs per database. Update and Run may be called from different goroutines.
type Scheduler struct {
	clk  clock.Clock
	run  RunFunc
	skip SkipFunc

	mu      sync.Mutex // guards every field below, including rnd
	rnd     *rand.Rand
	jobs    map[jobKey]*entry
	workers map[string]*worker
	ctx     context.Context //nolint:containedctx // set by Run and used to start workers from Update
	started bool
	stopped bool
	wg      sync.WaitGroup
}

// New returns a Scheduler. rnd is only used while holding the Scheduler's lock.
func New(clk clock.Clock, rnd *rand.Rand, run RunFunc, skip SkipFunc) *Scheduler {
	return &Scheduler{
		clk:     clk,
		rnd:     rnd,
		run:     run,
		skip:    skip,
		jobs:    map[jobKey]*entry{},
		workers: map[string]*worker{},
	}
}

// Update swaps the plan. A job with the same Query, Database, Interval and Fingerprint keeps its next due time.
// Jobs with a non-positive Interval are ignored.
func (s *Scheduler) Update(p planner.Plan) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clk.Now()
	next := make(map[jobKey]*entry, len(p.Jobs))
	touched := map[string]struct{}{}
	for _, j := range p.Jobs {
		if j.Interval <= 0 {
			continue
		}
		k := jobKey{j.Query, j.Database}
		if old, ok := s.jobs[k]; ok && old.job.Interval == j.Interval && old.job.Fingerprint == j.Fingerprint {
			old.job = j
			next[k] = old
			continue
		}
		next[k] = &entry{job: j, due: now.Add(s.randDuration(min(j.Interval, MaxInitialDelay)))}
		touched[j.Database] = struct{}{}
	}
	for k, old := range s.jobs {
		if next[k] != old {
			touched[k.database] = struct{}{}
		}
	}
	s.jobs = next

	if !s.started || s.stopped {
		return
	}
	for db := range touched {
		if w, ok := s.workers[db]; ok {
			w.poke()
		} else if s.hasJobs(db) {
			s.spawn(db)
		}
	}
}

// Run starts one worker per database in the plan and blocks until ctx is done.
// It returns after every running job has returned.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyRunning
	}
	s.started = true
	s.ctx = ctx
	dbs := map[string]struct{}{}
	for k := range s.jobs {
		dbs[k.database] = struct{}{}
	}
	for db := range dbs {
		s.spawn(db)
	}
	s.mu.Unlock()

	<-ctx.Done()

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

// poke interrupts the worker's wait so that it re-reads the plan. Call with Scheduler.mu held.
func (w *worker) poke() {
	if w.timer != nil {
		w.timer.Stop()
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// spawn starts the worker of db. Call with mu held and s.ctx set.
func (s *Scheduler) spawn(db string) {
	w := &worker{wake: make(chan struct{}, 1)}
	s.workers[db] = w
	s.wg.Add(1)
	// Not safe.Go: work wraps each run and skip, and a dead worker would stall its database.
	go s.work(s.ctx, db, w)
}

func (s *Scheduler) hasJobs(db string) bool {
	for k := range s.jobs {
		if k.database == db {
			return true
		}
	}
	return false
}

// earliest returns the entry of db that is due first, ties broken by query name. Call with mu held.
func (s *Scheduler) earliest(db string) *entry {
	var best *entry
	for k, e := range s.jobs {
		if k.database != db {
			continue
		}
		if best == nil || e.due.Before(best.due) || (e.due.Equal(best.due) && e.job.Query < best.job.Query) {
			best = e
		}
	}
	return best
}

func (s *Scheduler) work(ctx context.Context, db string, w *worker) {
	defer s.wg.Done()
	logger := log.FromContext(ctx).GetLogger()
	for {
		s.mu.Lock()
		select {
		case <-w.wake:
		default:
		}
		e := s.earliest(db)
		if e == nil {
			delete(s.workers, db)
			s.mu.Unlock()
			return
		}
		if now := s.clk.Now(); e.due.After(now) {
			t := s.clk.NewTimer(e.due.Sub(now))
			w.timer = t
			s.mu.Unlock()

			select {
			case <-t.C():
			case <-w.wake:
			case <-ctx.Done():
			}
			t.Stop()

			s.mu.Lock()
			w.timer = nil
			s.mu.Unlock()
			if ctx.Err() != nil {
				s.exit(db)
				return
			}
			continue
		}
		j, due := e.job, e.due
		s.mu.Unlock()

		start := s.clk.Now()
		safe.Call(logger, "run of "+j.Query+" on "+j.Database, func() { s.run(ctx, j) })
		end := s.clk.Now()
		if ctx.Err() != nil {
			s.exit(db)
			return
		}
		if missed := s.reschedule(e, due, start, end); missed > 0 {
			safe.Call(logger, "skip of "+j.Query+" on "+j.Database, func() { s.skip(j, missed) })
		}
	}
}

func (s *Scheduler) exit(db string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, db)
}

// reschedule sets the next due time of e after a run that was due at due, started at start and ended at end:
// the later of the next grid slot and one interval after start, moved past end.
// It returns the number of runs that were missed, and does nothing if e left the plan.
func (s *Scheduler) reschedule(e *entry, due, start, end time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[jobKey{e.job.Query, e.job.Database}] != e {
		return 0
	}
	interval := e.job.Interval
	next := due.Add(interval + s.randDuration(min(interval/10, MaxJitter)))
	if floor := start.Add(interval); floor.After(next) {
		next = floor
	}
	missed := 0
	for !next.After(end) {
		missed++
		next = next.Add(interval)
	}
	e.due = next
	return missed
}

// randDuration returns a uniform duration in [0, n). Call with mu held.
func (s *Scheduler) randDuration(n time.Duration) time.Duration {
	if n <= 0 {
		return 0
	}
	return time.Duration(s.rnd.Int64N(int64(n)))
}
