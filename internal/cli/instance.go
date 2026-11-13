package cli

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/spf13/cobra"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

func newInstanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Run the otel-metrics sidecar",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			log.FromContext(cmd.Context()).Info("starting instance", "version", version.Version)
			<-cmd.Context().Done()

			return nil
		},
	}
	cmd.AddCommand(newPreflightCommand())

	return cmd
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
