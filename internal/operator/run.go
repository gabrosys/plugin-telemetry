package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/http"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// ErrInvalidOptions is returned when an Options field is empty.
	ErrInvalidOptions = errors.New("server address, server cert, server key and client CA are all required")
	// ErrInvalidDeps is returned when a Deps field the server needs is nil.
	ErrInvalidDeps = errors.New("cache, reconciler and status are required")
	// ErrCacheNotSynced is returned when the cache stops without syncing while ctx is still live.
	ErrCacheNotSynced = errors.New("cache did not sync")
	// ErrServerStopped is returned when the server stops while ctx is still live.
	ErrServerStopped = errors.New("server stopped unexpectedly")
)

// Options configures the mTLS gRPC server.
type Options struct {
	ServerAddress  string // host:port
	ServerCertPath string
	ServerKeyPath  string
	ClientCAPath   string
}

// ImageWaiter is an ImageSource that can block until an image is known.
type ImageWaiter interface {
	ImageSource
	Wait(ctx context.Context) error
}

// Deps are the collaborators the services share.
type Deps struct {
	Cache      cache.Cache
	Client     client.Client
	Reconciler *Reconciler
	Status     *Status
}

// Run waits for the image and the cache, then serves Identity, OperatorLifecycle, ReconcilerHooks and
// Operator until ctx ends. It returns nil when ctx ends and ErrServerStopped if the server stops first.
// The cache must be started by the caller.
func Run(ctx context.Context, opts Options, images ImageWaiter, deps Deps) error {
	if opts.ServerAddress == "" || opts.ServerCertPath == "" || opts.ServerKeyPath == "" || opts.ClientCAPath == "" {
		return ErrInvalidOptions
	}
	if deps.Cache == nil || deps.Reconciler == nil || deps.Status == nil {
		return ErrInvalidDeps
	}

	logger := log.FromContext(ctx)
	logger.Info("waiting for the sidecar image")
	if err := images.Wait(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil
		}

		return fmt.Errorf("wait for sidecar image: %w", err)
	}

	logger.Info("waiting for the caches to sync")
	if err := registerInformers(ctx, deps.Cache); err != nil {
		select {
		case <-ctx.Done():
			return nil
		default:
			return err
		}
	}
	if !deps.Cache.WaitForCacheSync(ctx) {
		select {
		case <-ctx.Done():
			return nil
		default:
			return ErrCacheNotSynced
		}
	}

	logger.Info("sidecar image loaded, serving")

	srv := &http.Server{
		IdentityImpl: Identity{},
		Enrichers: []http.ServerEnricher{
			func(s *grpc.Server) error {
				lifecycle.RegisterOperatorLifecycleServer(s, &Lifecycle{Images: images})
				reconciler.RegisterReconcilerHooksServer(s, deps.Reconciler)
				operator.RegisterOperatorServer(s, deps.Status)

				return nil
			},
		},
		ServerCertPath: opts.ServerCertPath,
		ServerKeyPath:  opts.ServerKeyPath,
		ClientCertPath: opts.ClientCAPath,
		ServerAddress:  opts.ServerAddress,
	}
	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("serve plugin: %w", err)
	}
	if ctx.Err() == nil {
		return ErrServerStopped
	}

	return nil
}
