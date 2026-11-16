package preflight

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const (
	checkTimeout = 15 * time.Second
	passwordFile = "password"
)

// query is the check the PostgreSQL report shows; a var so tests can see inside its transaction.
var query = `SELECT current_user,
       (SELECT count(*) FROM pg_catalog.pg_ls_waldir()),
       (SELECT count(*) FROM pg_catalog.pg_ls_archive_statusdir())`

// SQLState returns the SQLSTATE and "SEVERITY: message" of a PostgreSQL
// error, or an empty code and the error text for any other error.
func SQLState(err error) (code, message string) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code, pgErr.Severity + ": " + pgErr.Message
	}

	return "", err.Error()
}

func checkPostgres(ctx context.Context, cfg PostgresConfig, precondition error) *PostgresReport {
	rep := &PostgresReport{Host: cfg.Host, User: cfg.User}
	if precondition != nil {
		rep.Error = precondition.Error()

		return rep
	}

	fail := func(err error) *PostgresReport {
		rep.SQLState, rep.Error = SQLState(err)

		return rep
	}

	limits := settings.Defaults().Limits
	pgCfg := pg.Config{
		Host:         cfg.Host,
		Port:         cfg.Port,
		User:         cfg.User,
		QueryTimeout: limits.QueryTimeout,
		LockTimeout:  limits.LockTimeout,
	}
	if cfg.Password != "" {
		pgCfg.PasswordFile = passwordFile
		pgCfg.ReadFile = func(string) ([]byte, error) { return []byte(cfg.Password), nil }
	}

	cc, err := pg.ConnConfig(pgCfg, cfg.Database)
	if err != nil {
		return fail(fmt.Errorf("connection settings: %w", err))
	}

	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return fail(fmt.Errorf("connect: %w", err))
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer closeCancel()
		if err := conn.Close(closeCtx); err != nil && rep.Error == "" {
			rep.Error = fmt.Sprintf("close: %v", err)
		}
	}()
	rep.Connected = true

	if err := readOnly(ctx, conn, pg.GuardSQL(pgCfg), rep); err != nil {
		return fail(err)
	}

	return rep
}

// readOnly runs query in a read-only transaction under guard, the limits of the engine's transactions.
func readOnly(ctx context.Context, conn *pgx.Conn, guard string, rep *PostgresReport) (err error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("roll back: %w", rbErr))
		}
	}()
	if _, err := tx.Exec(ctx, guard); err != nil {
		return fmt.Errorf("set the transaction limits: %w", err)
	}
	if err := tx.QueryRow(ctx, query).Scan(&rep.CurrentUser, &rep.WALFiles, &rep.ArchiveStatusFiles); err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
