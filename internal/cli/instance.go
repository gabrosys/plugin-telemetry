package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/kubesource"
	"github.com/gabrosys/plugin-telemetry/internal/memlimit"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
	"github.com/gabrosys/plugin-telemetry/internal/safe"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

// kubeSource is the Kubernetes-backed engine configuration and the loop that keeps it current.
type kubeSource interface {
	engine.Config
	Start(ctx context.Context) error
}

// snapshotTimeout bounds the wait of the tools for the token and the watched objects.
const snapshotTimeout = 15 * time.Second

// newKubeSource and snapshotKubeSource build the Kubernetes source; specs replace them.
var (
	newKubeSource = func(o kubesource.Options) kubeSource {
		return kubesource.New(o)
	}
	snapshotKubeSource = func(ctx context.Context, o kubesource.Options, wait time.Duration) (engine.Config, error) {
		src, err := kubesource.Snapshot(ctx, o, wait)
		if err != nil {
			return nil, err //nolint:wrapcheck // open adds the context
		}
		return src, nil
	}
)

// kubeOptions builds the Kubernetes source options from the Pod environment and names the variables that are unset.
func kubeOptions(getenv func(string) string) (kubesource.Options, error) {
	var missing []string
	value := func(name string) string {
		v := getenv(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}
	o := kubesource.Options{
		Namespace: value("NAMESPACE"),
		Cluster:   value("CLUSTER_NAME"),
		TokenDir:  metadata.SidecarTokenDir,
	}
	host, port := value("KUBERNETES_SERVICE_HOST"), value("KUBERNETES_SERVICE_PORT")
	if len(missing) > 0 {
		return kubesource.Options{}, fmt.Errorf("environment variable not set: %s; outside the Pod pass --config", strings.Join(missing, ", "))
	}
	o.APIServer = "https://" + net.JoinHostPort(host, port)

	return o, nil
}

// openFunc returns the configuration the tools read: the config file, or the Kubernetes objects once their watches have synced.
// The environment is checked here, before anything is read.
func openFunc(config string) (func(context.Context) (engine.Config, error), error) {
	if config != "" {
		return func(context.Context) (engine.Config, error) { return engine.FileConfig(config), nil }, nil
	}
	o, err := kubeOptions(os.Getenv)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) (engine.Config, error) {
		cfg, err := snapshotKubeSource(ctx, o, snapshotTimeout)
		if err != nil {
			return nil, fmt.Errorf("kubernetes snapshot: %w", err)
		}

		return cfg, nil
	}, nil
}

// setMemoryLimit applies the memory limit; specs replace it.
var setMemoryLimit = memlimit.Set

func newInstanceCommand() *cobra.Command {
	var config string
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Run the otel-metrics sidecar",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			logger := log.FromContext(ctx)
			logger.Info("starting instance", "version", version.Version)
			if config != "" {
				if err := runEngine(ctx, engine.FileConfig(config), nil); err != nil {
					return fmt.Errorf("run the engine: %w", err)
				}

				return nil
			}

			// In Kubernetes mode the sidecar logs its own problems and idles; exiting would restart the container.
			o, err := kubeOptions(os.Getenv)
			if err != nil {
				logger.Error(err, "the Kubernetes source is not configured")
				<-ctx.Done()

				return nil
			}
			src := newKubeSource(o)
			if err := runEngine(ctx, src, src.Start); err != nil {
				logger.Error(err, "the metrics engine stopped")
				<-ctx.Done()
			}

			return nil
		},
	}
	cmd.PersistentFlags().StringVar(&config, "config", "", "engine settings file")
	cmd.AddCommand(
		newPreflightCommand(),
		toolCommand("validate", "Check the settings and the query sources without connecting", &config, validate),
		toolCommand("test", "Run every planned query once and print the series", &config, runTest),
		toolCommand("print-config", "Print the effective settings, the sources and the plan", &config, printConfig),
	)

	return cmd
}

// runEngine clears the environment, sets the memory limit and runs the engine on cfg; background, when
// set, runs beside it and either ending stops both.
func runEngine(ctx context.Context, cfg engine.Config, background func(context.Context) error) error {
	logger := log.FromContext(ctx)
	clearEnv(ctx)
	if limit, err := setMemoryLimit(os.ReadFile, os.Getenv); err != nil {
		logger.Info("memory limit not set", "error", err.Error())
	} else {
		logger.Info("memory limit", "bytes", limit)
	}
	opts := engine.Options{Config: cfg}
	if background == nil {
		return engine.Run(ctx, opts) //nolint:wrapcheck // the caller adds the context
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(guard(logger.GetLogger(), "the Kubernetes source", func() error { return background(gctx) }))
	g.Go(guard(logger.GetLogger(), "the engine", func() error { return engine.Run(gctx, opts) }))
	if err := g.Wait(); err != nil {
		return fmt.Errorf("run the sidecar: %w", err)
	}

	return nil
}

// errPanicked is what guard returns for a function that panicked.
var errPanicked = errors.New("panicked")

// guard returns fn as an errgroup function that turns a panic into an error: errgroup doesn't recover.
func guard(logger logr.Logger, what string, fn func() error) func() error {
	return func() (err error) {
		if safe.Call(logger, what, func() { err = fn() }) {
			return fmt.Errorf("%s: %w", what, errPanicked)
		}
		return err
	}
}

// clearEnv unsets the PG* variables pgx reads (pg.EnvVars) and every OTEL_* variable, and logs their names.
func clearEnv(ctx context.Context) {
	removed := append(pg.ClearEnv(), export.ClearEnv()...)
	if len(removed) > 0 {
		log.FromContext(ctx).Info("removed environment variables", "names", removed)
	}
}

func newPreflightCommand() *cobra.Command {
	var (
		pgCfg preflight.PostgresConfig
		cfg   = preflight.Config{
			PodTokenDir: metadata.PodTokenDir,
			TokenDir:    metadata.SidecarTokenDir,
			Namespace:   os.Getenv("NAMESPACE"),
			Cluster:     os.Getenv("CLUSTER_NAME"),
		}
		port uint16
	)

	cmd := &cobra.Command{
		Use:   "preflight",
		Short: "Check PostgreSQL and Kubernetes access and print a JSON report",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pgCfg.Port = port
			if pgCfg.PasswordSecret == "" {
				pgCfg.Password = os.Getenv("PGPASSWORD")
			}
			cfg.Postgres = pgCfg
			clearEnv(cmd.Context())

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
	f.StringVar(&pgCfg.Host, "pg-host", metadata.SocketDir, "PostgreSQL host, or a socket directory when it starts with /")
	f.Uint16Var(&port, "pg-port", 5432, "PostgreSQL port")
	f.StringVar(&pgCfg.User, "pg-user", metadata.MetricsExporterRole, "PostgreSQL user")
	f.StringVar(&pgCfg.Database, "pg-database", "postgres", "PostgreSQL database")
	f.StringVar(&pgCfg.PasswordSecret, "password-secret", "", "Secret holding the password, read with the sidecar token (default $PGPASSWORD)")
	f.BoolVar(&cfg.Kube, "kube", true, "run the Kubernetes checks")

	return cmd
}
