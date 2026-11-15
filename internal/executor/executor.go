// Package executor runs one query on one database in a read-only transaction and turns the rows into samples.
package executor

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"k8s.io/utils/clock"

	"github.com/cloudnative-pg/machinery/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const (
	// deadlineGrace lets the server's statement_timeout fire before the context deadline.
	deadlineGrace = 2 * time.Second
	// rollbackTimeout bounds the rollback of an unfinished transaction.
	rollbackTimeout = 2 * time.Second
	// cancelTimeout bounds the cancel request sent at the row cap.
	cancelTimeout = 2 * time.Second

	sqlstateQueryCanceled = "57014"
	sqlstateLockNotAvail  = "55P03"
	sqlstateInsufficient  = "42501"
)

// Result is the outcome of one Run.
type Result struct {
	Job     planner.Job
	Outcome selfmetrics.Outcome // ok, error, timeout, capped, denied, skipped
	Samples []queries.Sample    // nil unless ok
	WouldBe int                 // capped: series counted before giving up
	Dropped map[queries.DropReason]int
	Err     error
	// ConnectionLost is set when the connection broke during the run or none could be acquired.
	ConnectionLost bool
	Started        time.Time
	Duration       time.Duration
}

// Executor runs jobs on the connections of a pool.
type Executor struct {
	pool   *pg.Pool
	limits settings.Limits
	server func() *semver.Version
	clk    clock.PassiveClock
	// cancel asks the server to stop the statement running on conn.
	cancel func(context.Context, *pgx.Conn) error
}

// New returns an Executor; server returns the version of the connected server, nil when unknown.
func New(pool *pg.Pool, limits settings.Limits, server func() *semver.Version, clk clock.PassiveClock) *Executor {
	return &Executor{pool: pool, limits: limits, server: server, clk: clk, cancel: cancelStatement}
}

// Classify maps an error to an outcome: SQLSTATE 57014 or 55P03, or a context deadline, is timeout;
// SQLSTATE 42501 is denied; a closed pool, which a pool swap leaves behind, is skipped.
func Classify(err error) selfmetrics.Outcome {
	if errors.Is(err, pg.ErrPoolClosed) {
		return selfmetrics.OutcomeSkipped
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case sqlstateQueryCanceled, sqlstateLockNotAvail:
			return selfmetrics.OutcomeTimeout
		case sqlstateInsufficient:
			return selfmetrics.OutcomeDenied
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return selfmetrics.OutcomeTimeout
	}
	return selfmetrics.OutcomeError
}

// Run executes job and never panics; a panic becomes an error result.
func (e *Executor) Run(ctx context.Context, job planner.Job) (res Result) {
	res = Result{Job: job, Started: e.clk.Now(), Dropped: map[queries.DropReason]int{}}
	defer func() {
		if r := recover(); r != nil {
			res.Outcome = selfmetrics.OutcomeError
			res.Err = fmt.Errorf("panic in query %s on %s: %v", job.Query, job.Database, r)
			res.Samples = nil
		}
		res.Duration = e.clk.Since(res.Started)
	}()

	ctx, cancel := context.WithTimeout(ctx, e.limits.QueryTimeout+deadlineGrace)
	defer cancel()

	if err := e.run(ctx, job, &res); err != nil {
		res.Outcome = Classify(err)
		if res.Outcome != selfmetrics.OutcomeTimeout && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			res.Outcome = selfmetrics.OutcomeTimeout
		}
		res.Err = err
		res.Samples = nil
		res.WouldBe = 0
	}
	return res
}

func (e *Executor) run(ctx context.Context, job planner.Job, res *Result) error {
	conn, release, err := e.pool.Acquire(ctx, job.Database)
	if err != nil {
		// Waiting for a slot past the run's deadline, or a closed pool, loses no connection.
		res.ConnectionLost = ctx.Err() == nil && !errors.Is(err, pg.ErrPoolClosed)
		return fmt.Errorf("acquire connection to %s: %w", job.Database, err)
	}
	// A panic leaves broken true and ConnectionLost false.
	broken := true
	defer func() { release(broken) }()

	discard, err := e.inTransaction(ctx, conn, job, res)
	res.ConnectionLost = conn.IsClosed() || (err != nil && connectionFailed(ctx, conn, err))
	broken = discard || res.ConnectionLost
	return err
}

// inTransaction runs the predicate and the query in one read-only transaction and commits it.
func (e *Executor) inTransaction(ctx context.Context, conn *pgx.Conn, job planner.Job, res *Result) (discard bool, err error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		rbErr := tx.Rollback(rbCtx)
		if rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			// A capped run's statement was cancelled on purpose, so its rollback error is only logged.
			if res.Outcome == selfmetrics.OutcomeCapped {
				logCleanup(ctx, job, "roll back after the row cap", rbErr)
				return
			}
			err = errors.Join(err, fmt.Errorf("roll back: %w", rbErr))
		}
	}()

	if job.Def.Predicate != "" {
		collect, err := predicate(ctx, tx, job.Def.Predicate)
		if err != nil {
			return false, err
		}
		if !collect {
			res.Outcome = selfmetrics.OutcomeOK
			return false, commit(ctx, tx)
		}
	}

	capped, err := e.collect(ctx, tx, job, res, &discard)
	if err != nil {
		return false, err
	}
	if capped {
		res.Outcome = selfmetrics.OutcomeCapped
		res.Samples = nil
		return discard, nil
	}
	res.Outcome = selfmetrics.OutcomeOK
	return false, commit(ctx, tx)
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// predicate reads the first column of the first row as database/sql scans a sql.NullBool:
// NULL or no row is false, and a value driver.Bool rejects is an error.
func predicate(ctx context.Context, tx pgx.Tx, sql string) (bool, error) {
	rows, err := tx.Query(ctx, sql, pg.ResultFormats)
	if err != nil {
		return false, fmt.Errorf("run predicate query: %w", err)
	}
	defer rows.Close()

	var first any
	if rows.Next() {
		values, err := pg.Values(tx.Conn().TypeMap(), rows.FieldDescriptions(), rows.RawValues())
		if err != nil {
			return false, fmt.Errorf("read predicate result: %w", err)
		}
		if len(values) == 0 {
			return false, errors.New("predicate query returned no column")
		}
		first = values[0]
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("run predicate query: %w", err)
	}
	if first == nil {
		return false, nil
	}

	v, err := driver.Bool.ConvertValue(first)
	if err != nil {
		return false, fmt.Errorf("convert predicate result: %w", err)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("convert predicate result: got %T", v)
	}
	return b, nil
}

// collect reads the rows into res.Samples and res.Dropped, and reports whether a cap was hit.
func (e *Executor) collect(ctx context.Context, tx pgx.Tx, job planner.Job, res *Result, discard *bool) (capped bool, err error) {
	rows, err := tx.Query(ctx, job.Def.SQL, pg.ResultFormats)
	if err != nil {
		return false, fmt.Errorf("run query: %w", err)
	}
	defer rows.Close()

	var (
		fields    []pgconn.FieldDescription
		columns   []string
		typeMap   = tx.Conn().TypeMap()
		server    = e.server()
		seen      = map[string]struct{}{}
		series    int
		rowCount  int
		maxRows   = e.limits.MaxRowsPerQuery
		maxSeries = e.limits.MaxSeriesPerQuery
		overCap   bool
	)
	// The result description can be incomplete until the first row or the end, where a server error shows.
	checkColumns := func() error {
		fields = rows.FieldDescriptions()
		columns = make([]string, len(fields))
		for i, f := range fields {
			columns[i] = f.Name
		}
		if err := job.Def.CheckColumns(columns); err != nil {
			return fmt.Errorf("query %s: %w", job.Query, err)
		}
		return nil
	}
	for rows.Next() {
		if rowCount == 0 {
			if err := checkColumns(); err != nil {
				return false, err
			}
		}
		if maxRows > 0 && rowCount >= maxRows {
			res.WouldBe = series
			if err := e.cancel(ctx, tx.Conn()); err != nil {
				// An undelivered or late cancel request could stop the next statement on this connection.
				*discard = true
				logCleanup(ctx, job, "send the cancel request at the row cap", err)
			}
			return true, nil
		}
		rowCount++

		values, err := pg.Values(typeMap, fields, rows.RawValues())
		if err != nil {
			return false, fmt.Errorf("read row %d: %w", rowCount, err)
		}
		samples, dropped := job.Def.RowSamples(columns, values, server, e.limits.MaxLabelValueLength)
		for reason, n := range dropped {
			res.Dropped[reason] += n
		}
		for _, s := range samples {
			key := seriesKey(s)
			if _, dup := seen[key]; dup {
				res.Dropped[queries.DropDuplicate]++
				continue
			}
			seen[key] = struct{}{}
			series++
			if maxSeries > 0 && series > maxSeries {
				overCap = true
			}
			if !overCap {
				res.Samples = append(res.Samples, s)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read rows: %w", err)
	}
	if rowCount == 0 {
		if err := checkColumns(); err != nil {
			return false, err
		}
	}
	if overCap {
		res.WouldBe = series
		return true, nil
	}
	return false, nil
}

// cancelStatement asks the server to stop the running statement, so closing the rows does not
// read the rest of the result. Delivery is not guaranteed (pgconn.PgConn.CancelRequest).
func cancelStatement(ctx context.Context, conn *pgx.Conn) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
	defer cancel()
	if err := conn.PgConn().CancelRequest(cctx); err != nil {
		return fmt.Errorf("cancel request: %w", err)
	}
	return nil
}

// logCleanup logs an error that does not change the outcome of the run.
func logCleanup(ctx context.Context, job planner.Job, msg string, err error) {
	log.FromContext(ctx).GetLogger().V(1).Info(msg, "query", job.Query, "database", job.Database, "error", err.Error())
}

// seriesKey is the metric name and the labels, which are sorted by key.
func seriesKey(s queries.Sample) string {
	var b strings.Builder
	b.WriteString(s.Metric)
	for _, l := range s.Labels {
		b.WriteByte(0)
		b.WriteString(l.Key)
		b.WriteByte('=')
		b.WriteString(l.Value)
	}
	return b.String()
}

// connectionFailed reports whether the connection must be discarded: the pool does not ping idle
// connections, and pgx closes the connection on a network error or a cancelled or expired context.
func connectionFailed(ctx context.Context, conn *pgx.Conn, err error) bool {
	if conn.IsClosed() || ctx.Err() != nil || pgconn.SafeToRetry(err) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
