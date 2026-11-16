package export

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/safe"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

// Loop collects from a ManualReader and exports through one exporter on its own tick, one export at a time.
// A new exporter, interval or request size takes a new Loop.
type Loop struct {
	reader    *sdkmetric.ManualReader
	res       *resource.Resource
	clk       clock.Clock
	rnd       *rand.Rand // used by Run only
	stats     *selfmetrics.Stats
	exp       *Exporter
	interval  time.Duration
	maxPoints int

	lastFailure string // the kind of the last failed export, "" while exports succeed; Run only
	tickFailed  bool   // an export of the running tick failed; Run only
}

// failureCollect is the lastFailure of a collection that failed.
const failureCollect = "collect"

// NewLoop returns a Loop that exports what reader collects through exp with res as the resource,
// once per interval and at most maxPoints points per request.
func NewLoop(reader *sdkmetric.ManualReader, res *resource.Resource, clk clock.Clock, rnd *rand.Rand, stats *selfmetrics.Stats, exp *Exporter, interval time.Duration, maxPoints int) *Loop {
	return &Loop{reader: reader, res: res, clk: clk, rnd: rnd, stats: stats, exp: exp, interval: interval, maxPoints: maxPoints}
}

// Run exports after a random offset below one interval and then once per interval, until ctx ends.
// It then shuts the exporter down.
func (l *Loop) Run(ctx context.Context) error {
	defer l.shutdown(ctx)

	next := l.clk.Now().Add(time.Duration(l.rnd.Int64N(int64(l.interval))))
	for {
		if !l.sleepUntil(ctx, next) {
			return nil
		}
		at := next
		started := l.clk.Now()
		if safe.Call(log.FromContext(ctx).GetLogger(), "export tick", func() { l.tick(ctx, at) }) {
			rec := l.record(selfmetrics.OutcomeError)
			rec.Duration = l.clk.Since(started)
			rec.ErrorType = errorTypeOther
			l.stats.Export(ctx, rec)
		}

		next = at.Add(l.interval)
		for end := l.clk.Now(); !next.After(end); next = next.Add(l.interval) {
			l.stats.Export(ctx, l.record(selfmetrics.OutcomeSkipped))
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

func (l *Loop) record(o selfmetrics.Outcome) selfmetrics.ExportRecord {
	return selfmetrics.ExportRecord{
		Outcome:       o,
		At:            l.clk.Now(),
		ComponentType: l.exp.ComponentType(),
		ServerAddress: l.exp.address,
		ServerPort:    l.exp.port,
	}
}

// tick collects and exports the batches in order under one deadline of at + 0.9 × interval.
func (l *Loop) tick(ctx context.Context, at time.Time) {
	ctx, cancel := context.WithTimeout(ctx, at.Add(l.interval*9/10).Sub(l.clk.Now()))
	defer cancel()
	logger := log.FromContext(ctx)

	l.tickFailed = false // tick's state is touched only by Run's goroutine
	start := l.clk.Now()
	rm := &metricdata.ResourceMetrics{}
	if err := l.reader.Collect(ctx, rm); err != nil {
		l.failed(failureCollect, func() { logger.Error(err, "collect metrics") })
		rec := l.record(selfmetrics.OutcomeError)
		rec.Duration = l.clk.Since(start)
		rec.ErrorType = errorTypeOther
		l.stats.Export(ctx, rec)
		return
	}
	rm.Resource = l.res

	for _, b := range Split(rm, l.maxPoints) {
		l.send(ctx, b)
	}
	if !l.tickFailed && l.lastFailure != "" {
		l.lastFailure = ""
		logger.Info("export recovered")
	}
}

// send exports b and records the outcome. A request the SDK refuses as too large, with more than
// one point, is sent in two halves under the same tick deadline, so the parts never outlast 0.9 × interval.
func (l *Loop) send(ctx context.Context, b *metricdata.ResourceMetrics) {
	logger := log.FromContext(ctx)
	start := l.clk.Now()
	status, err := l.exp.export(ctx, b)
	rec := l.record(selfmetrics.OutcomeOK)
	rec.Duration = l.clk.Since(start)
	rec.Points = points(b)
	rec.HTTPStatusCode = status
	if err != nil {
		errorType, rejected := Classify(err)
		switch {
		case errorType == errorTypeTooLarge && rec.Points > 1 && ctx.Err() == nil:
			for _, part := range Split(b, (rec.Points+1)/2) {
				l.send(ctx, part)
			}
			return
		case errorType == errorTypeRejected:
			rec.Rejected = min(rejected, rec.Points)
			l.failed(errorTypeRejected, func() {
				logger.Info("export partially rejected", "rejected", rejected, "error", err.Error())
			})
		default:
			rec.Outcome = selfmetrics.OutcomeError
			rec.ErrorType = errorType
			l.failed(errorType, func() { logger.Error(err, "export metrics", "error.type", errorType) })
		}
	}
	l.stats.Export(ctx, rec)
}

// failed marks the tick failed and calls logFn unless the last failed export was of the same kind.
func (l *Loop) failed(kind string, logFn func()) {
	l.tickFailed = true
	if kind == l.lastFailure {
		return
	}
	l.lastFailure = kind
	logFn()
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

func (l *Loop) shutdown(ctx context.Context) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := l.exp.Shutdown(sctx); err != nil {
		log.FromContext(ctx).Error(err, "shut down exporter", "otel.component.type", l.exp.ComponentType())
	}
}
