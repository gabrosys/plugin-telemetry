// Package pg opens read-only monitoring connections to PostgreSQL.
package pg

import (
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

// GuardSQL sets the limits of c for the current transaction only: statement_timeout, lock_timeout,
// idle_in_transaction_session_timeout (twice the query timeout), jit and max_parallel_workers_per_gather.
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
	}, ", ")
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
