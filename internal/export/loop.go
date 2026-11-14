package export

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

// target is an exporter with the cadence and request size it runs at.
type target struct {
	exp       *Exporter
	interval  time.Duration
	maxPoints int
}

// Loop collects from a ManualReader and exports on its own tick, one export at a time.
type Loop struct {
	reader *sdkmetric.ManualReader
	res    *resource.Resource
	clk    clock.Clock
	rnd    *rand.Rand // used by Run only
	stats  *selfmetrics.Stats

	mu      sync.Mutex // guards pending and unused
	pending *target
	unused  []*Exporter   // replaced before their first tick
	wake    chan struct{} // signals the first SetExporter to a waiting Run
}

// NewLoop returns a Loop that exports what reader collects with res as the resource.
func NewLoop(reader *sdkmetric.ManualReader, res *resource.Resource, clk clock.Clock, rnd *rand.Rand, stats *selfmetrics.Stats) *Loop {
	return &Loop{reader: reader, res: res, clk: clk, rnd: rnd, stats: stats, wake: make(chan struct{}, 1)}
}

// SetExporter takes effect at the next tick; the previous exporter is shut down after its export ends.
// A nil exporter or a non-positive interval is ignored.
func (l *Loop) SetExporter(e *Exporter, interval time.Duration, maxPoints int) {
	if e == nil || interval <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pending != nil && l.pending.exp != e {
		l.unused = append(l.unused, l.pending.exp)
	}
	l.pending = &target{exp: e, interval: interval, maxPoints: maxPoints}
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// take returns the pending target, or nil, and the exporters replaced before they were used.
func (l *Loop) take() (*target, []*Exporter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p, unused := l.pending, l.unused
	l.pending, l.unused = nil, nil
	if p != nil {
		unused = slices.DeleteFunc(unused, func(e *Exporter) bool { return e == p.exp })
	}
	return p, unused
}

// Run exports until ctx ends, then shuts the exporters down. It waits for the first
// SetExporter, exports after a random offset below one interval and then once per interval.
func (l *Loop) Run(ctx context.Context) error {
	var cur *target
	defer func() { l.shutdownAll(ctx, cur) }()

	for cur == nil {
		var unused []*Exporter
		cur, unused = l.take()
		l.shutdown(ctx, unused...)
		if cur != nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-l.wake:
		}
	}

	next := l.clk.Now().Add(time.Duration(l.rnd.Int64N(int64(cur.interval))))
	for {
		if !l.sleepUntil(ctx, next) {
			return nil
		}
		at := next
		if p, unused := l.take(); p != nil || len(unused) > 0 {
			l.shutdown(ctx, unused...)
			if p != nil {
				if p.exp != cur.exp {
					l.shutdown(ctx, cur.exp)
				}
				cur = p
			}
		}
		l.tick(ctx, cur, at)

		next = at.Add(cur.interval)
		for end := l.clk.Now(); !next.After(end); next = next.Add(cur.interval) {
			l.stats.Export(l.record(cur, selfmetrics.OutcomeSkipped)) //nolint:contextcheck // Stats.Export takes no context
		}
	}
}

func (l *Loop) sleepUntil(ctx context.Context, t time.Time) bool {
	d := t.Sub(l.clk.Now())
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := l.clk.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *Loop) record(t *target, o selfmetrics.Outcome) selfmetrics.ExportRecord {
	return selfmetrics.ExportRecord{
		Outcome:       o,
		At:            l.clk.Now(),
		ComponentType: t.exp.ComponentType(),
		ServerAddress: t.exp.address,
		ServerPort:    t.exp.port,
	}
}

// tick collects and exports the batches in order under one deadline of at + 0.9 × interval.
func (l *Loop) tick(ctx context.Context, t *target, at time.Time) {
	ctx, cancel := context.WithTimeout(ctx, at.Add(t.interval*9/10).Sub(l.clk.Now()))
	defer cancel()
	logger := log.FromContext(ctx)

	start := l.clk.Now()
	rm := &metricdata.ResourceMetrics{}
	if err := l.reader.Collect(ctx, rm); err != nil {
		logger.Error(err, "collect metrics")
		rec := l.record(t, selfmetrics.OutcomeError)
		rec.Duration = l.clk.Since(start)
		rec.ErrorType = errorTypeOther
		l.stats.Export(rec) //nolint:contextcheck // Stats.Export takes no context
		return
	}
	rm.Resource = l.res

	for _, b := range Split(rm, t.maxPoints) {
		start := l.clk.Now()
		status, err := t.exp.export(ctx, b)
		rec := l.record(t, selfmetrics.OutcomeOK)
		rec.Duration = l.clk.Since(start)
		rec.Points = points(b)
		rec.HTTPStatusCode = status
		if err != nil {
			errorType, rejected := Classify(err)
			if errorType == errorTypeRejected {
				rec.Rejected = min(rejected, rec.Points)
				logger.Info("export partially rejected", "rejected", rejected, "error", err.Error())
			} else {
				rec.Outcome = selfmetrics.OutcomeError
				rec.ErrorType = errorType
				logger.Error(err, "export metrics", "error.type", errorType)
			}
		}
		l.stats.Export(rec) //nolint:contextcheck // Stats.Export takes no context
	}
}

func points(rm *metricdata.ResourceMetrics) int {
	n := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			n += pointCount(m.Data)
		}
	}
	return n
}

func (l *Loop) shutdown(ctx context.Context, exps ...*Exporter) {
	for _, e := range exps {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		if err := e.Shutdown(sctx); err != nil {
			log.FromContext(ctx).Error(err, "shut down exporter", "otel.component.type", e.ComponentType())
		}
		cancel()
	}
}

// shutdownAll shuts down the current exporter and any set but not used yet.
func (l *Loop) shutdownAll(ctx context.Context, cur *target) {
	p, unused := l.take()
	if p != nil && (cur == nil || p.exp != cur.exp) {
		unused = append(unused, p.exp)
	}
	if cur != nil {
		unused = append(unused, cur.exp)
	}
	l.shutdown(ctx, unused...)
}
