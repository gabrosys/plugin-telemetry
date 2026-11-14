package cli

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/rest"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

var errNoNamespace = errors.New("namespace is empty: set --namespace or POD_NAMESPACE")

func newOperatorCommand() *cobra.Command {
	var (
		opts      operator.Options
		configMap string
		namespace string

		metricsEndpoint string
		metricsInterval time.Duration
	)

	cmd := &cobra.Command{
		Use:   "operator",
		Short: "Serve the CNPG-I Identity, OperatorLifecycle, ReconcilerHooks and Operator services over mTLS",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if namespace == "" {
				return errNoNamespace
			}

			metricsExporter, exportMetrics, err := operator.MetricsExporter(metricsEndpoint, metricsInterval)
			if err != nil {
				return fmt.Errorf("metrics flags: %w", err)
			}

			cfg, err := rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("load in-cluster config: %w", err)
			}

			mgr, err := operator.NewManager(cfg)
			if err != nil {
				return fmt.Errorf("build manager: %w", err)
			}

			var metrics *operator.PluginMetrics
			if exportMetrics {
				metrics = operator.NewPluginMetrics()
			}

			ensurer := &identity.Ensurer{Reader: mgr.GetCache(), Writer: mgr.GetClient()}
			controller := &operator.IdentityController{Client: mgr.GetClient(), Identity: ensurer, Metrics: metrics}
			if err := controller.SetupWithManager(mgr); err != nil {
				return fmt.Errorf("register identity controller: %w", err)
			}

			ctx := cmd.Context()
			store := operator.NewImageStore()
			g, gctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				return operator.WatchSidecarImage(gctx, cfg, namespace, configMap, store)
			})
			if exportMetrics {
				g.Go(func() error {
					if err := metrics.Run(gctx, metricsExporter); err != nil {
						log.FromContext(gctx).Error(err, "export plugin metrics")
					}

					return nil
				})
			}
			g.Go(func() error {
				if err := mgr.Start(gctx); err != nil {
					return fmt.Errorf("run manager: %w", err)
				}

				return nil
			})
			g.Go(func() error {
				return operator.Run(gctx, opts, store, operator.Deps{
					Cache:  mgr.GetCache(),
					Client: mgr.GetClient(),
					Reconciler: &operator.Reconciler{
						Identity:  ensurer,
						Exporters: mgr.GetCache(),
						Timeout:   operator.DefaultPreTimeout,
						Metrics:   metrics,
					},
					Status:  &operator.Status{Reader: mgr.GetCache(), Identity: ensurer, Metrics: metrics},
					Metrics: metrics,
				})
			})

			return g.Wait()
		},
	}

	f := cmd.Flags()
	f.StringVar(&opts.ServerAddress, "server-address", "", "address to listen on, host:port")
	f.StringVar(&opts.ServerCertPath, "server-cert", "", "path to the server certificate")
	f.StringVar(&opts.ServerKeyPath, "server-key", "", "path to the server private key")
	f.StringVar(&opts.ClientCAPath, "client-cert", "", "path to the client CA certificate")
	f.StringVar(&configMap, "sidecar-image-configmap", "", "name of the ConfigMap holding the sidecar image")
	f.StringVar(&metricsEndpoint, "metrics-endpoint", "",
		"base URL of the OTLP/HTTP collector for the plugin's self-metrics, like exporter.endpoint; none are exported when empty")
	f.DurationVar(&metricsInterval, "metrics-interval", settings.Defaults().Exporter.Interval, "export interval of the plugin's self-metrics")
	f.StringVar(&namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace of the ConfigMap (default $POD_NAMESPACE)")

	for _, name := range []string{"server-address", "server-cert", "server-key", "client-cert", "sidecar-image-configmap"} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(fmt.Sprintf("mark flag %q required: %v", name, err))
		}
	}

	return cmd
}
