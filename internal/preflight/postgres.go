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
	checkTimeout   = 15 * time.Second
	passwordFile   = "password"
	preflightQuery = `SELECT current_user,
       (SELECT count(*) FROM pg_catalog.pg_ls_waldir()),
       (SELECT count(*) FROM pg_catalog.pg_ls_archive_statusdir())`
)

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

	if err := conn.QueryRow(ctx, preflightQuery).Scan(&rep.CurrentUser, &rep.WALFiles, &rep.ArchiveStatusFiles); err != nil {
		return fail(fmt.Errorf("query: %w", err))
	}

	return rep
}
