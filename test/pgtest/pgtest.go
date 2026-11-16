// Package pgtest runs PostgreSQL in a container for integration and e2e tests.
package pgtest

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
)

const (
	// MonitorUser is the role MonitorConfig connects as; it only has pg_monitor.
	MonitorUser = "cnpg_metrics_exporter"
	// MonitorPassword is the password of MonitorUser.
	MonitorPassword = "monitor-password"

	dataDir    = "/tmp/pgdata"
	holdFile   = "/tmp/pg-hold"
	socketDir  = "/var/run/postgresql"
	readyTries = 300
)

//go:embed server.sh
var serverScript string

// Postgres is a PostgreSQL server in a container.
type Postgres struct {
	Host string
	Port uint16

	container *testcontainers.DockerContainer
}

// Start runs PostgreSQL from image (POSTGRES_IMAGE) with initdb and postgres under a shell
// that can stop and start the server without stopping the container, so the host port stays.
func Start(ctx context.Context, image string) (*Postgres, error) {
	c, err := testcontainers.Run(ctx, image,
		testcontainers.WithCmd("bash", "-c", serverScript),
		testcontainers.WithEnv(map[string]string{"PGDATA": dataDir, "HOLD": holdFile}),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections")),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("start container: %w", err), Terminate(ctx, c))
	}

	host, err := c.Host(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("get container host: %w", err), Terminate(ctx, c))
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return nil, errors.Join(fmt.Errorf("get mapped port: %w", err), Terminate(ctx, c))
	}

	p := &Postgres{Host: host, Port: port.Num(), container: c}
	for _, stmt := range []string{
		"CREATE ROLE " + MonitorUser + " LOGIN PASSWORD '" + MonitorPassword + "'",
		"GRANT pg_monitor TO " + MonitorUser,
		"CREATE DATABASE app",
		"CREATE DATABASE noconnect",
		"REVOKE CONNECT ON DATABASE noconnect FROM PUBLIC",
	} {
		if err := p.Exec(ctx, stmt); err != nil {
			return nil, errors.Join(fmt.Errorf("bootstrap: %w", err), Terminate(ctx, c))
		}
	}
	return p, nil
}

// Terminate removes the container, with its volumes, and wraps the error.
func Terminate(ctx context.Context, c *testcontainers.DockerContainer) error {
	if err := c.Terminate(ctx); err != nil {
		return fmt.Errorf("terminate container: %w", err)
	}
	return nil
}

// Exec runs one SQL statement as postgres in database postgres over the container's socket.
func (p *Postgres) Exec(ctx context.Context, sql string) error {
	return p.ExecIn(ctx, "postgres", sql)
}

// ExecIn runs one SQL statement as postgres in database over the container's socket.
func (p *Postgres) ExecIn(ctx context.Context, database, sql string) error {
	return p.run(ctx, "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", socketDir, "-U", "postgres", "-d", database, "-c", sql)
}

// MonitorConfig returns the settings of the monitoring role, its password in a file under dir.
// It panics when the password file cannot be written.
func (p *Postgres) MonitorConfig(dir string) pg.Config {
	file := filepath.Join(dir, "password")
	if err := os.WriteFile(file, []byte(MonitorPassword+"\n"), 0o600); err != nil {
		panic(fmt.Sprintf("write password file: %v", err))
	}
	return pg.Config{
		Host:           p.Host,
		Port:           p.Port,
		User:           MonitorUser,
		PasswordFile:   file,
		QueryTimeout:   10 * time.Second,
		LockTimeout:    5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}
}

// PlainConn connects to database as the monitoring role with pgx's defaults and no runtime parameter of
// its own, so its settings are the server's.
func (p *Postgres) PlainConn(ctx context.Context, database string) (*pgx.Conn, error) {
	cc, err := pgx.ParseConfig("sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cc.Host, cc.Port, cc.User, cc.Password, cc.Database = p.Host, p.Port, MonitorUser, MonitorPassword, database
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return conn, nil
}

// StopServer stops PostgreSQL and keeps it stopped, the container keeps running.
func (p *Postgres) StopServer(ctx context.Context) error {
	if err := p.run(ctx, "touch", holdFile); err != nil {
		return err
	}
	return p.run(ctx, "pg_ctl", "-D", dataDir, "stop", "-m", "fast")
}

// StopServerSmart starts a smart shutdown and keeps the server stopped once it is down: the postmaster
// waits for the open sessions to end, and the call returns without waiting.
func (p *Postgres) StopServerSmart(ctx context.Context) error {
	if err := p.run(ctx, "touch", holdFile); err != nil {
		return err
	}
	return p.run(ctx, "pg_ctl", "-D", dataDir, "stop", "-m", "smart", "-W")
}

// Running reports whether the postmaster is alive, also while it only waits to shut down.
func (p *Postgres) Running(ctx context.Context) bool {
	return p.run(ctx, "pg_ctl", "-D", dataDir, "status") == nil
}

// StartServer starts PostgreSQL again and returns once it accepts connections on the same port.
func (p *Postgres) StartServer(ctx context.Context) error {
	if err := p.run(ctx, "rm", "-f", holdFile); err != nil {
		return err
	}
	for range readyTries {
		if err := p.run(ctx, "pg_isready", "-h", socketDir, "-q"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for the server: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	return errors.New("the server did not accept connections after restart")
}

// Terminate removes the container.
func (p *Postgres) Terminate(ctx context.Context) error {
	return Terminate(ctx, p.container)
}

func (p *Postgres) run(ctx context.Context, cmd ...string) error {
	code, out, err := p.container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return fmt.Errorf("exec %s: %w", cmd[0], err)
	}
	if code != 0 {
		output, readErr := io.ReadAll(out)
		if readErr != nil {
			return fmt.Errorf("exec %s: exit code %d (read output: %w)", cmd[0], code, readErr)
		}
		return fmt.Errorf("exec %s: exit code %d: %s", cmd[0], code, strings.TrimSpace(string(output)))
	}
	return nil
}
