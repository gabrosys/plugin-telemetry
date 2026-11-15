// Package planner turns query definitions, settings and live PostgreSQL facts into jobs.
package planner

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
)

// Facts are what the planner needs to know about the server.
type Facts struct {
	Version    semver.Version
	InRecovery bool
	Databases  []string // allow connections, not templates, CONNECT granted; sorted
	// Checks are pg_monitor, read_only, track_io_timing, track_wal_io_timing and track_functions.
	Checks map[string]bool
	// ChecksErr joins the errors of the checks that could not be read; they are missing from Checks.
	ChecksErr error
}

// ParseVersionNum converts server_version_num to a version as CloudNativePG does:
// 180007 is 18.0.7 (pkg/management/postgres/utils/version.go at v1.30.1).
func ParseVersionNum(s string) (semver.Version, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return semver.Version{}, fmt.Errorf("parse server_version_num %q: %w", s, err)
	}
	return *semver.New(n/10000, (n/100)%100, n%100, "", ""), nil
}

// databasesQuery is CloudNativePG's (pkg/management/postgres/metrics/collector.go:286 at v1.30.1).
const databasesQuery = `SELECT datname, pg_catalog.has_database_privilege(datname, 'CONNECT')
		 FROM pg_catalog.pg_database
		 WHERE datallowconn AND NOT datistemplate`

// checkSettings maps a check to the setting it reads; a setting the server does not have omits the check.
// The settings are documented in PostgreSQL 18, "Run-time Statistics", track_io_timing, track_wal_io_timing and
// track_functions (https://www.postgresql.org/docs/18/runtime-config-statistics.html), and default_transaction_read_only
// (https://www.postgresql.org/docs/18/runtime-config-client.html).

var checkSettings = map[string]string{
	"read_only":           "default_transaction_read_only",
	"track_io_timing":     "track_io_timing",
	"track_wal_io_timing": "track_wal_io_timing",
	"track_functions":     "track_functions",
}

// checkHolds says whether a check holds for the value of its setting.
func checkHolds(check, value string) bool {
	if check == "track_functions" {
		return value != "none"
	}
	return value == "on"
}

// readChecks reads the live checks in tx, each in a savepoint so that a failing one leaves the transaction
// usable. A check that cannot be read is omitted and its error joined into the returned one.
func readChecks(ctx context.Context, tx pgx.Tx, settings map[string]string) (map[string]bool, error) {
	checks := make(map[string]bool, len(settings)+1)
	var errs error
	read := func(name, sql string, args ...any) *string {
		sp, err := tx.Begin(ctx)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("check %s: begin savepoint: %w", name, err))
			return nil
		}
		var value *string
		if err := sp.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
			errs = errors.Join(errs, fmt.Errorf("check %s: %w", name, err), rollbackSavepoint(ctx, name, sp))
			return nil
		}
		if err := sp.Commit(ctx); err != nil {
			errs = errors.Join(errs, fmt.Errorf("check %s: release savepoint: %w", name, err))
			return nil
		}
		return value
	}
	if v := read("pg_monitor", "SELECT pg_catalog.pg_has_role(current_user, oid, 'MEMBER')::text FROM pg_catalog.pg_roles WHERE rolname = 'pg_monitor'"); v != nil {
		checks["pg_monitor"] = *v == "true"
	}
	for check, setting := range settings {
		if v := read(check, "SELECT pg_catalog.current_setting($1::text, true)", setting); v != nil {
			checks[check] = checkHolds(check, *v)
		}
	}
	return checks, errs
}

func rollbackSavepoint(ctx context.Context, check string, sp pgx.Tx) error {
	if err := sp.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("check %s: roll back savepoint: %w", check, err)
	}
	return nil
}

// GatherFacts reads the server version, the recovery state, the live checks and the connectable
// databases in one read-only transaction on conn, which must be connected to the default database.
func GatherFacts(ctx context.Context, conn *pgx.Conn) (Facts, error) {
	return gatherFacts(ctx, conn, checkSettings)
}

func gatherFacts(ctx context.Context, conn *pgx.Conn, settings map[string]string) (facts Facts, err error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Facts{}, fmt.Errorf("begin facts transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("roll back facts transaction: %w", rbErr))
		}
	}()

	var versionNum string
	if err := tx.QueryRow(ctx, "SHOW server_version_num").Scan(&versionNum); err != nil {
		return Facts{}, fmt.Errorf("show server_version_num: %w", err)
	}
	version, err := ParseVersionNum(versionNum)
	if err != nil {
		return Facts{}, err
	}

	var inRecovery bool
	if err := tx.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return Facts{}, fmt.Errorf("check recovery state: %w", err)
	}

	checks, checksErr := readChecks(ctx, tx, settings)

	rows, err := tx.Query(ctx, databasesQuery)
	if err != nil {
		return Facts{}, fmt.Errorf("discover databases: %w", err)
	}
	var databases []string
	for rows.Next() {
		var name string
		var canConnect bool
		if err := rows.Scan(&name, &canConnect); err != nil {
			rows.Close()
			return Facts{}, fmt.Errorf("scan pg_database row: %w", err)
		}
		if canConnect {
			databases = append(databases, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Facts{}, fmt.Errorf("read pg_database rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Facts{}, fmt.Errorf("commit facts transaction: %w", err)
	}

	sort.Strings(databases)
	return Facts{Version: version, InRecovery: inRecovery, Databases: databases, Checks: checks, ChecksErr: checksErr}, nil
}
