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

// GatherFacts reads the server version, the recovery state and the connectable databases in one
// read-only transaction on conn, which must be connected to the default database.
func GatherFacts(ctx context.Context, conn *pgx.Conn) (facts Facts, err error) {
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
	return Facts{Version: version, InRecovery: inRecovery, Databases: databases}, nil
}
