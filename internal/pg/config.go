// Package pg opens read-only monitoring connections to PostgreSQL.
package pg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

const (
	applicationName = "cnpg_otel_engine"
	// cancelDeadlineDelay is how long a cancelled statement may take to end before the socket is closed.
	cancelDeadlineDelay = 2 * time.Second
)

// DefaultConnectTimeout bounds a connection attempt when Config.ConnectTimeout is zero.
const DefaultConnectTimeout = 5 * time.Second

// Config holds the settings shared by every connection, whatever the database.
type Config struct {
	Host string
	Port uint16
	User string
	// PasswordFile is read at every connection; empty means no password.
	PasswordFile string
	// QueryTimeout is the statement_timeout GuardSQL sets; idle_in_transaction_session_timeout is twice this.
	QueryTimeout   time.Duration
	LockTimeout    time.Duration
	ConnectTimeout time.Duration
	// ReadFile reads PasswordFile; nil means os.ReadFile.
	ReadFile func(string) ([]byte, error)
}

var (
	// ErrPGEnv is returned while a PG* environment variable could change the connection settings.
	ErrPGEnv = errors.New("a PG* environment variable is set")
	// ErrInvalidConfig never wraps pgx's error: it holds the connection string, password included.
	ErrInvalidConfig = errors.New("invalid PostgreSQL connection settings")
	// ErrBackoff is returned by Acquire while a database waits before the next connect attempt.
	ErrBackoff = errors.New("waiting before reconnecting")
)

// EnvVars lists every variable pgx v5.13.0 reads (pgconn/config.go:739-779).
var EnvVars = []string{
	"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "PGPASSFILE", "PGAPPNAME",
	"PGCONNECT_TIMEOUT", "PGSSLMODE", "PGSSLKEY", "PGSSLCERT", "PGSSLCERTMODE", "PGSSLSNI", "PGSSLROOTCERT",
	"PGSSLPASSWORD", "PGSSLNEGOTIATION", "PGTARGETSESSIONATTRS", "PGSERVICE", "PGSERVICEFILE", "PGTZ",
	"PGOPTIONS", "PGMINPROTOCOLVERSION", "PGMAXPROTOCOLVERSION", "PGCHANNELBINDING", "PGREQUIREAUTH",
}

// ClearEnv unsets EnvVars in this process and returns the names it removed.
func ClearEnv() []string {
	var removed []string
	for _, name := range EnvVars {
		if _, set := os.LookupEnv(name); !set {
			continue
		}
		// A failed unset leaves the variable set, and ConnConfig then reports it as ErrPGEnv.
		if err := os.Unsetenv(name); err == nil {
			removed = append(removed, name)
		}
	}
	return removed
}

// ConnConfig fails with ErrPGEnv while any of EnvVars is set, then parses a keyword DSN that
// sets every key pgx would otherwise take from $HOME, and adds the session settings. The limits are not
// session settings: GuardSQL sets them per transaction, so pg_settings shows the server's values.
func ConnConfig(c Config, database string) (*pgx.ConnConfig, error) {
	for _, name := range EnvVars {
		if _, set := os.LookupEnv(name); set {
			return nil, fmt.Errorf("%w: %s", ErrPGEnv, name)
		}
	}
	// Below a millisecond the value is written as 0, which PostgreSQL reads as "no timeout".
	if c.Host == "" || c.User == "" || c.Port == 0 || c.QueryTimeout < time.Millisecond || c.LockTimeout < time.Millisecond {
		return nil, ErrInvalidConfig
	}

	var password string
	if c.PasswordFile != "" {
		read := c.ReadFile
		if read == nil {
			read = os.ReadFile
		}
		raw, err := read(c.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read password file: %w", err)
		}
		password = strings.TrimSuffix(string(raw), "\n")
	}

	cc, err := pgx.ParseConfig(dsn(c, database, password))
	if err != nil {
		return nil, ErrInvalidConfig
	}

	cc.RuntimeParams["search_path"] = "pg_catalog, public, pg_temp"
	cc.RuntimeParams["standard_conforming_strings"] = "on"
	// At a context's end the server is asked to cancel the statement; the socket deadline is the fallback.
	cc.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, DeadlineDelay: cancelDeadlineDelay}
	}
	return cc, nil
}

// SuperuserSQL reads rolsuper of the session role (PostgreSQL 18, pg_roles and session_user).
const SuperuserSQL = "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = session_user"

// GuardSQL sets the limits of c for the current transaction only: statement_timeout, lock_timeout,
// idle_in_transaction_session_timeout (twice the query timeout), jit and max_parallel_workers_per_gather.
// Its last column is rolsuper of the session role (SuperuserSQL).
func GuardSQL(c Config) string {
	set := func(name, value string) string {
		return "pg_catalog.set_config('" + name + "', '" + value + "', true)"
	}
	return "SELECT " + strings.Join([]string{
		set("statement_timeout", milliseconds(c.QueryTimeout)),
		set("lock_timeout", milliseconds(c.LockTimeout)),
		set("idle_in_transaction_session_timeout", milliseconds(2*c.QueryTimeout)),
		set("jit", "off"),
		set("max_parallel_workers_per_gather", "0"),
		"(" + SuperuserSQL + ")",
	}, ", ")
}

// rollbackTimeout bounds the rollback of a transaction ReadOnly's fn left open.
const rollbackTimeout = 2 * time.Second

// ReadOnly runs fn in a read-only transaction on conn under guard (Pool.Guard), and rolls the transaction
// back if fn leaves it open. A session role that is a superuser gives superuser true and fn does not run.
func ReadOnly(ctx context.Context, conn *pgx.Conn, guard string, fn func(pgx.Tx) error) (superuser bool, err error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("roll back transaction: %w", rbErr))
		}
	}()
	if superuser, err = ApplyGuard(ctx, tx, guard); err != nil || superuser {
		return superuser, err
	}
	return false, fn(tx)
}

// ApplyGuard runs guard (GuardSQL, Pool.Guard) in tx and reports whether the session role is a superuser.
func ApplyGuard(ctx context.Context, tx pgx.Tx, guard string) (superuser bool, err error) {
	rows, err := tx.Query(ctx, guard)
	if err != nil {
		return false, fmt.Errorf("set the transaction limits: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("set the transaction limits: %w", err)
		}
		return false, errors.New("set the transaction limits: the guard returned no row")
	}
	values, err := rows.Values()
	if err != nil {
		return false, fmt.Errorf("set the transaction limits: %w", err)
	}
	superuser, ok := values[len(values)-1].(bool)
	if !ok {
		return false, errors.New("set the transaction limits: the guard's last column is not a boolean")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("set the transaction limits: %w", err)
	}
	return superuser, nil
}

// dsn builds a keyword/value connection string. Empty values are written as a quoted empty string so that
// they cannot swallow the next pair and so that no default from $HOME applies.
func dsn(c Config, database, password string) string {
	timeout := c.ConnectTimeout
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}
	seconds := max(1, int64((timeout+time.Second-1)/time.Second))

	pairs := [][2]string{
		{"host", c.Host},
		{"port", strconv.Itoa(int(c.Port))},
		{"user", c.User},
		{"password", password},
		{"dbname", database},
		{"sslmode", "disable"},
		{"sslrootcert", ""},
		{"passfile", ""},
		{"options", ""},
		{"application_name", applicationName},
		{"connect_timeout", strconv.FormatInt(seconds, 10)},
		{"target_session_attrs", "any"},
	}

	var b strings.Builder
	escape := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(p[0])
		b.WriteString("='")
		b.WriteString(escape.Replace(p[1]))
		b.WriteByte('\'')
	}
	return b.String()
}

func milliseconds(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}
