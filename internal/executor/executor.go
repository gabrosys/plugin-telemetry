// Package executor runs one query on one database in a read-only transaction and turns the rows into samples.
package executor

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const (
	// deadlineGrace lets the guard's statement_timeout fire before the context deadline.
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
	// ConnectionLost is set when the connection broke during the run or a connect attempt failed.
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
// SQLSTATE 42501 is denied; a closed pool, which a pool swap leaves behind, a wait that got no slot in time
// and a superuser session role are skipped.
func Classify(err error) selfmetrics.Outcome {
	if errors.Is(err, pg.ErrPoolClosed) || errors.Is(err, pg.ErrNoSlot) || errors.Is(err, planner.ErrSuperuser) {
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
			logCleanup(ctx, job, "recover a panic in a run", res.Err, "stack", string(debug.Stack()))
		}
		res.Duration = e.clk.Since(res.Started)
	}()

	if pastDeadline, err := e.run(ctx, job, &res); err != nil {
		res.Outcome = Classify(err)
		if res.Outcome != selfmetrics.OutcomeTimeout && pastDeadline {
			res.Outcome = selfmetrics.OutcomeTimeout
		}
		res.Err = err
		res.Samples = nil
		res.WouldBe = 0
	}
	return res
}

// RunTimeout is the deadline of one run, which starts when the run has a pool slot.
func RunTimeout(l settings.Limits) time.Duration { return l.QueryTimeout + deadlineGrace }

// run waits for a pool slot for at most the job's interval (the query timeout without one), then runs the job under a deadline that starts at the slot.
// pastDeadline reports that this deadline had passed when run returned.
func (e *Executor) run(ctx context.Context, job planner.Job, res *Result) (pastDeadline bool, err error) {
	maxWait := job.Interval
	if maxWait <= 0 {
		maxWait = e.limits.QueryTimeout
	}
	conn, release, err := e.pool.AcquireWithin(ctx, job.Database, maxWait)
	if err != nil {
		if errors.Is(err, pg.ErrNoSlot) {
			return false, fmt.Errorf("query %s on database %s could not get a connection within %s: %w",
				job.Query, job.Database, maxWait, pg.ErrNoSlot)
		}
		// A cancelled wait, the backoff after a failed connect and a closed pool lose no connection.
		res.ConnectionLost = ctx.Err() == nil && !errors.Is(err, pg.ErrBackoff) && !errors.Is(err, pg.ErrPoolClosed)
		return false, fmt.Errorf("acquire connection to %s: %w", job.Database, err)
	}
	rctx, cancel := context.WithTimeout(ctx, RunTimeout(e.limits))
	defer cancel()
	defer release()

	err = e.inTransaction(rctx, conn, job, res)
	res.ConnectionLost = conn.IsClosed() || (err != nil && connectionFailed(conn, err))
	return errors.Is(rctx.Err(), context.DeadlineExceeded), err
}

// inTransaction runs the predicate and the query in one read-only transaction and commits it. The
// transaction runs under the pool's guard unless the query reads the settings; such a query is bounded
// by the cancel request the connection sends at the run deadline.
func (e *Executor) inTransaction(ctx context.Context, conn *pgx.Conn, job planner.Job, res *Result) (err error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
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

	var superuser bool
	if job.Def.ReadsSettings {
		err = tx.QueryRow(ctx, pg.SuperuserSQL).Scan(&superuser)
		if err != nil {
			return fmt.Errorf("read rolsuper of the session role: %w", err)
		}
	} else if superuser, err = pg.ApplyGuard(ctx, tx, e.pool.Guard()); err != nil {
		return fmt.Errorf("query transaction: %w", err)
	}
	if superuser {
		return planner.ErrSuperuser
	}

	if job.Def.Predicate != "" {
		collect, err := predicate(ctx, tx, job.Def.Predicate)
		if err != nil {
			return err
		}
		if !collect {
			res.Outcome = selfmetrics.OutcomeOK
			return commit(ctx, tx)
		}
	}

	capped, err := e.collect(ctx, tx, job, res)
	if err != nil {
		return err
	}
	if capped {
		res.Outcome = selfmetrics.OutcomeCapped
		res.Samples = nil
		return nil
	}
	res.Outcome = selfmetrics.OutcomeOK
	return commit(ctx, tx)
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
func (e *Executor) collect(ctx context.Context, tx pgx.Tx, job planner.Job, res *Result) (capped bool, err error) {
	rows, err := tx.Query(ctx, job.Def.SQL, pg.ResultFormats)
	if err != nil {
		return false, fmt.Errorf("run query: %w", err)
	}
	defer rows.Close()

	// seen holds at most maxSeries + 1 series IDs; a series past that is counted but not remembered, so WouldBe
	// can overcount one that repeats. Limits are validated >= 1, so the bound holds whenever a cap is set.
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
				// The rest of the result is not read.
				e.stopStatement(ctx, tx, job, "send the cancel request at a column mismatch")
				return false, err
			}
		}
		if maxRows > 0 && rowCount >= maxRows {
			res.WouldBe = series
			e.stopStatement(ctx, tx, job, "send the cancel request at the row cap")
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
			id := queries.SeriesID(s.Metric, s.Labels)
			if _, dup := seen[id]; dup {
				res.Dropped[queries.DropDuplicate]++
				continue
			}
			if maxSeries <= 0 || len(seen) <= maxSeries {
				seen[id] = struct{}{}
			}
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

// stopStatement cancels the statement so that closing the rows does not read the rest of the result.
func (e *Executor) stopStatement(ctx context.Context, tx pgx.Tx, job planner.Job, msg string) {
	if err := e.cancel(ctx, tx.Conn()); err != nil {
		logCleanup(ctx, job, msg, err)
	}
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
func logCleanup(ctx context.Context, job planner.Job, msg string, err error, kv ...any) {
	kv = append([]any{"query", job.Query, "database", job.Database, "error", err.Error()}, kv...)
	log.FromContext(ctx).GetLogger().V(1).Info(msg, kv...)
}

// connectionFailed reports whether the connection broke: pgx closes the connection on a network error.
// At a context's end the server cancels the statement and the connection stays usable, unless the cancel
// did not end it in time.
func connectionFailed(conn *pgx.Conn, err error) bool {
	if conn.IsClosed() || pgconn.SafeToRetry(err) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
