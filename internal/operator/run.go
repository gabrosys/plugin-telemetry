package operator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/http"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// ErrInvalidOptions is returned when an Options field is empty.
	ErrInvalidOptions = errors.New("server address, server cert, server key and client CA are all required")
	// ErrInvalidDeps is returned when a Deps field the server needs is nil.
	ErrInvalidDeps = errors.New("cache and status are required")
	// ErrCacheNotSynced is returned when the cache stops without syncing while ctx is still live.
	ErrCacheNotSynced = errors.New("cache did not sync")
	// ErrServerStopped is returned when the server stops while ctx is still live.
	ErrServerStopped = errors.New("server stopped unexpectedly")
)

// drainTimeout bounds how long a stop waits for the calls in flight; a variable so tests can shorten it.
var drainTimeout = 10 * time.Second

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
	Cache  cache.Cache
	Client client.Client
	Status *Status
	// Metrics records the lifecycle hook calls; nil records nothing.
	Metrics *PluginMetrics
}

// Run waits for the image and the cache, then serves Identity, OperatorLifecycle and Operator until ctx
// ends. Once ctx ends it lets the calls in flight finish for up to drainTimeout, then stops.
// It returns nil when ctx ends and ErrServerStopped if the server stops first.
// The cache must be started by the caller.
func Run(ctx context.Context, opts Options, images ImageWaiter, deps Deps) error {
	if opts.ServerAddress == "" || opts.ServerCertPath == "" || opts.ServerKeyPath == "" || opts.ClientCAPath == "" {
		return ErrInvalidOptions
	}
	if deps.Cache == nil || deps.Status == nil {
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

	servers := make(chan *grpc.Server, 1)
	srv := &http.Server{
		IdentityImpl: Identity{},
		Enrichers: []http.ServerEnricher{
			func(s *grpc.Server) error {
				lifecycle.RegisterOperatorLifecycleServer(s, &Lifecycle{Images: images, Metrics: deps.Metrics})
				operator.RegisterOperatorServer(s, deps.Status)
				servers <- s

				return nil
			},
		},
		ServerCertPath: opts.ServerCertPath,
		ServerKeyPath:  opts.ServerKeyPath,
		ClientCertPath: opts.ClientCAPath,
		ServerAddress:  opts.ServerAddress,
	}

	// machinery's Start calls Stop once its context ends, so it gets one that ends only after the drain.
	serveCtx, stopServe := context.WithCancel(context.WithoutCancel(ctx))
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stopOnDone(ctx, serveCtx, servers, stopServe)
	}()
	err := srv.Start(serveCtx)
	stopServe()
	<-stopped
	if err != nil {
		return fmt.Errorf("serve plugin: %w", err)
	}
	if ctx.Err() == nil {
		return ErrServerStopped
	}

	return nil
}

// stopOnDone waits for ctx to end, drains the server the enricher sent, if any, and then calls
// stopServe. It returns at once when serveCtx ends first.
func stopOnDone(ctx, serveCtx context.Context, servers <-chan *grpc.Server, stopServe context.CancelFunc) {
	defer stopServe()

	select {
	case <-serveCtx.Done():
		return
	case <-ctx.Done():
	}

	select {
	case s := <-servers:
		drain(ctx, s)
	default:
	}
}

// drain stops s gracefully and stops it hard once drainTimeout has passed. The GracefulStop
// goroutine then ends with the last handler.
func drain(ctx context.Context, s *grpc.Server) {
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		s.GracefulStop()
	}()

	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		log.FromContext(ctx).Info("calls still running after the drain timeout, stopping", "timeout", drainTimeout)
		s.Stop()
	}
}
