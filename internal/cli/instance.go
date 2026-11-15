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
	"strings"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"k8s.io/utils/clock"

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

// instanceFlags are the flags every instance subcommand takes.
type instanceFlags struct {
	config string
}

func (f *instanceFlags) add(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()
	pf.StringVar(&f.config, "config", "", "engine settings file")
}

// kubeSource is the Kubernetes-backed engine configuration and the loop that keeps it current.
type kubeSource interface {
	engine.Config
	Start(ctx context.Context) error
}

// newKubeSource and snapshotKubeSource build the Kubernetes source; specs replace them.
var (
	newKubeSource = func(o kubesource.Options) kubeSource {
		return kubesource.New(o)
	}
	snapshotKubeSource = func(ctx context.Context, o kubesource.Options) (engine.Config, error) {
		src, err := kubesource.Snapshot(ctx, o)
		if err != nil {
			return nil, fmt.Errorf("kubernetes snapshot: %w", err)
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
		return kubesource.Options{}, fmt.Errorf("environment variable not set: %s", strings.Join(missing, ", "))
	}
	o.APIServer = "https://" + net.JoinHostPort(host, port)

	return o, nil
}

// open returns the configuration the tools read: the --config file, or a snapshot of the Kubernetes objects.
// The environment is checked here, before anything is read.
func (f *instanceFlags) open() (func(context.Context) (engine.Config, error), error) {
	if f.config != "" {
		return func(context.Context) (engine.Config, error) { return engine.FileConfig(f.config), nil }, nil
	}
	o, err := kubeOptions(os.Getenv)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) (engine.Config, error) { return snapshotKubeSource(ctx, o) }, nil
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
			ctx := cmd.Context()
			logger := log.FromContext(ctx)
			logger.Info("starting instance", "version", version.Version)
			if flags.config != "" {
				if err := runEngine(ctx, engine.FileConfig(flags.config), nil); err != nil {
					return fmt.Errorf("run the engine: %w", err)
				}

				return nil
			}

			// In Kubernetes mode the sidecar logs its own problems and idles; exiting would restart the container.
			o, err := kubeOptions(os.Getenv)
			if err != nil {
				logger.Info("the Kubernetes source is not configured", "error", err.Error())
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
	flags.add(cmd)
	cmd.AddCommand(newPreflightCommand(), newValidateCommand(&flags), newTestCommand(&flags), newPrintConfigCommand(&flags))

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
	rnd, err := newRand()
	if err != nil {
		return err
	}
	opts := engine.Options{Config: cfg, Clock: clock.RealClock{}, Rand: rnd, Getenv: os.Getenv}
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
		pgCfg preflight.PostgresConfig
		cfg   = preflight.Config{PodTokenDir: metadata.PodTokenDir}
		port  uint16
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
	f.StringVar(&cfg.TokenDir, "token-dir", metadata.SidecarTokenDir, "directory holding the sidecar token and ca.crt")
	f.StringVar(&cfg.Namespace, "namespace", os.Getenv("NAMESPACE"), "namespace of the Cluster (default $NAMESPACE)")
	f.StringVar(&cfg.Cluster, "cluster", os.Getenv("CLUSTER_NAME"), "name of the Cluster (default $CLUSTER_NAME)")

	return cmd
}
