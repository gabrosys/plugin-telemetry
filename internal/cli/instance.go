package cli

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/spf13/cobra"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/featuregate"
	"github.com/gabrosys/plugin-telemetry/internal/memlimit"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

// instanceFlags are the flags every instance subcommand takes.
type instanceFlags struct {
	config string
	gates  string
}

func (f *instanceFlags) add(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()
	pf.StringVar(&f.config, "config", "", "engine settings file")
	pf.StringVar(&f.gates, "feature-gates", "", `feature gates to switch, as "+id,-id"`)
}

// registry returns the feature gates with the --feature-gates overrides applied.
func (f *instanceFlags) registry() (*featuregate.Registry, error) {
	r := featuregate.NewRegistry()
	if err := r.Set(f.gates); err != nil {
		return nil, fmt.Errorf("--feature-gates: %w", err)
	}
	return r, nil
}

// requireConfig fails when --config is not set.
func (f *instanceFlags) requireConfig() error {
	if f.config == "" {
		return errors.New("--config is required")
	}
	return nil
}

// setMemoryLimit applies the memory limit; specs replace it.
var setMemoryLimit = memlimit.Set

func newInstanceCommand() *cobra.Command {
	var flags instanceFlags
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Run the otel-metrics sidecar",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			gates, err := flags.registry()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			logger := log.FromContext(ctx)
			if flags.config == "" {
				logger.Info("starting instance", "version", version.Version)
				<-ctx.Done()

				return nil
			}

			clearEnv(ctx)
			if limit, err := setMemoryLimit(os.ReadFile, os.Getenv); err != nil {
				logger.Info("memory limit not set", "error", err.Error())
			} else {
				logger.Info("memory limit", "bytes", limit)
			}
			rnd, err := newRand()
			if err != nil {
				return err
			}
			if err := engine.Run(ctx, engine.Options{
				Config: engine.FileConfig(flags.config),
				Gates:  gates,
				Clock:  clock.RealClock{},
				Rand:   rnd,
				Getenv: os.Getenv,
			}); err != nil {
				return fmt.Errorf("run the engine: %w", err)
			}

			return nil
		},
	}
	flags.add(cmd)
	cmd.AddCommand(newPreflightCommand(), newValidateCommand(&flags), newTestCommand(&flags), newPrintConfigCommand(&flags))

	return cmd
}

// newRand returns a generator seeded from crypto/rand.
func newRand() (*rand.Rand, error) {
	var seed [16]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("seed the random generator: %w", err)
	}

	return rand.New(rand.NewPCG(binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:]))), nil //nolint:gosec // jitter, not security
}

// clearEnv unsets the PG* and OTEL_* variables and logs their names.
func clearEnv(ctx context.Context) {
	removed := append(pg.ClearEnv(), export.ClearEnv()...)
	if len(removed) > 0 {
		log.FromContext(ctx).Info("removed environment variables", "names", removed)
	}
}

func newPreflightCommand() *cobra.Command {
	var (
		pg   preflight.PostgresConfig
		cfg  = preflight.Config{PodTokenDir: metadata.PodTokenDir}
		port uint16
	)

	cmd := &cobra.Command{
		Use:   "preflight",
		Short: "Check PostgreSQL and Kubernetes access and print a JSON report",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pg.Port = port
			if pg.PasswordSecret == "" {
				pg.Password = os.Getenv("PGPASSWORD")
			}
			cfg.Postgres = pg

			if host, p := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT"); host != "" && p != "" {
				cfg.APIServer = "https://" + net.JoinHostPort(host, p)
			}

			report := preflight.Run(cmd.Context(), cfg)
			if err := json.NewEncoder(cmd.OutOrStdout()).Encode(report); err != nil {
				return fmt.Errorf("write report: %w", err)
			}

			return nil
		},
	}

	f := cmd.Flags()
	f.StringVar(&pg.Host, "pg-host", metadata.SocketDir, "PostgreSQL host, or a socket directory when it starts with /")
	f.Uint16Var(&port, "pg-port", 5432, "PostgreSQL port")
	f.StringVar(&pg.User, "pg-user", metadata.MetricsExporterRole, "PostgreSQL user")
	f.StringVar(&pg.Database, "pg-database", "postgres", "PostgreSQL database")
	f.StringVar(&pg.PasswordSecret, "password-secret", "", "Secret holding the password, read with the sidecar token (default $PGPASSWORD)")
	f.BoolVar(&cfg.Kube, "kube", true, "run the Kubernetes checks")
	f.StringVar(&cfg.TokenDir, "token-dir", metadata.SidecarTokenDir, "directory holding the sidecar token and ca.crt")
	f.StringVar(&cfg.Namespace, "namespace", os.Getenv("NAMESPACE"), "namespace of the Cluster (default $NAMESPACE)")
	f.StringVar(&cfg.Cluster, "cluster", os.Getenv("CLUSTER_NAME"), "name of the Cluster (default $CLUSTER_NAME)")

	return cmd
}
