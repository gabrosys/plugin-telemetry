package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/http"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"google.golang.org/grpc"
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

// Run waits for the image, then serves Identity and OperatorLifecycle until ctx ends.
// It returns nil when ctx ends.
func Run(ctx context.Context, opts Options, images ImageWaiter) error {
	if err := images.Wait(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil
		}

		return fmt.Errorf("wait for sidecar image: %w", err)
	}

	srv := &http.Server{
		IdentityImpl: Identity{},
		Enrichers: []http.ServerEnricher{
			func(s *grpc.Server) error {
				lifecycle.RegisterOperatorLifecycleServer(s, &Lifecycle{Images: images})

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

	return nil
}
