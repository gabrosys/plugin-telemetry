package preflight

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	connectTimeout  = 5 * time.Second
	queryTimeout    = 10 * time.Second
	applicationName = "cnpg_otel_preflight"

	preflightQuery = `SELECT current_user,
       (SELECT count(*) FROM pg_catalog.pg_ls_waldir()),
       (SELECT count(*) FROM pg_catalog.pg_ls_archive_statusdir())`
)

// ConnConfig builds the connection configuration. A host starting with / is
// a socket directory and gets no TLS.
func ConnConfig(c PostgresConfig) (*pgx.ConnConfig, error) {
	dsn := strings.Join([]string{
		"host=" + quoteValue(c.Host),
		"port=" + strconv.Itoa(int(c.Port)),
		"user=" + quoteValue(c.User),
		"dbname=" + quoteValue(c.Database),
		"password=" + quoteValue(c.Password),
	}, " ")

	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse connection config: %w", err)
	}

	cc.RuntimeParams["default_transaction_read_only"] = "on"
	cc.RuntimeParams["application_name"] = applicationName
	cc.ConnectTimeout = connectTimeout

	return cc, nil
}

// quoteValue quotes a keyword/value connection string value.
func quoteValue(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

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

	cc, err := ConnConfig(cfg)
	if err != nil {
		return fail(err)
	}

	ctx, cancel := context.WithTimeout(ctx, connectTimeout+queryTimeout)
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
