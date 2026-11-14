package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/rest"

	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

var errNoNamespace = errors.New("namespace is empty: set --namespace or POD_NAMESPACE")

func newOperatorCommand() *cobra.Command {
	var (
		opts      operator.Options
		configMap string
		namespace string
	)

	cmd := &cobra.Command{
		Use:   "operator",
		Short: "Serve the CNPG-I Identity, OperatorLifecycle, ReconcilerHooks and Operator services over mTLS",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if namespace == "" {
				return errNoNamespace
			}

			cfg, err := rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("load in-cluster config: %w", err)
			}

			mgr, err := operator.NewManager(cfg)
			if err != nil {
				return fmt.Errorf("build manager: %w", err)
			}

			ctx := cmd.Context()
			store := operator.NewImageStore()
			g, gctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				return operator.WatchSidecarImage(gctx, cfg, namespace, configMap, store)
			})
			g.Go(func() error {
				if err := mgr.Start(gctx); err != nil {
					return fmt.Errorf("run manager: %w", err)
				}

				return nil
			})
			g.Go(func() error {
				return operator.Run(gctx, opts, store, operator.Deps{
					Cache:      mgr.GetCache(),
					Client:     mgr.GetClient(),
					Reconciler: &operator.Reconciler{},
					Status:     &operator.Status{},
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
	f.StringVar(&namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace of the ConfigMap (default $POD_NAMESPACE)")

	for _, name := range []string{"server-address", "server-cert", "server-key", "client-cert", "sidecar-image-configmap"} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(fmt.Sprintf("mark flag %q required: %v", name, err))
		}
	}

	return cmd
}
