package export

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

const (
	componentHTTP = "otlp_http_metric_exporter"
	componentGRPC = "otlp_grpc_metric_exporter"

	// shutdownTimeout bounds the shutdown of an exporter that is no longer used.
	shutdownTimeout = 5 * time.Second
)

// Request caps in bytes before compression; the SDK refuses a larger request unsent.
var (
	maxRequestHTTP = 16 << 20
	maxRequestGRPC = 4 << 20
)

// Exporter wraps otlpmetrichttp or otlpmetricgrpc with our own HTTP client or gRPC
// connection, so no OTEL_EXPORTER_OTLP_* or proxy variable can change where or how it sends.
// It reads its files once; a configuration change builds a new Exporter.
type Exporter struct {
	s         settings.Exporter
	component string
	address   string
	port      int
	secure    bool

	exp  sdkmetric.Exporter
	conn *grpc.ClientConn // gRPC only
	rt   *roundTripper    // HTTP only
}

// NewExporter builds the exporter from s and reads its TLS and header files once with read.
func NewExporter(ctx context.Context, s settings.Exporter, read sources.ReadFile) (*Exporter, error) {
	component := componentGRPC
	if s.Protocol == settings.ProtocolHTTP {
		component = componentHTTP
	}
	u, err := url.Parse(s.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return nil, fmt.Errorf("exporter endpoint %q: not an http or https URL with a host", settings.RedactEndpoint(s.Endpoint))
	}
	e := &Exporter{s: s, component: component, address: u.Hostname(), secure: u.Scheme == "https"}
	e.port = 80
	if e.secure {
		e.port = 443
	}
	if p := u.Port(); p != "" {
		if e.port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("exporter endpoint port %q: %w", p, err)
		}
	}

	m, err := loadMaterial(s, e.secure, read)
	if err != nil {
		return nil, err
	}
	if s.Protocol == settings.ProtocolHTTP {
		err = e.initHTTP(ctx, m)
	} else {
		err = e.initGRPC(ctx, m)
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}

// exportTimeout bounds one export with its retries.
func (e *Exporter) exportTimeout() time.Duration { return e.s.Interval * 9 / 10 }

// retry returns the backoff of one export: the first and the longest wait, and the time left for waits after one attempt.
func (e *Exporter) retry() (initial, maxInterval, maxElapsed time.Duration) {
	return time.Second, 5 * time.Second, e.exportTimeout() - e.s.Timeout
}

// initHTTP builds the OTLP/HTTP exporter on e's own transport.
func (e *Exporter) initHTTP(ctx context.Context, m material) error {
	tlsCfg := tlsConfig(e.s.TLS, m)
	if !e.secure {
		tlsCfg = nil
	}
	e.rt = &roundTripper{base: newHTTPTransport(tlsCfg)}
	client := &http.Client{
		Transport: e.rt,
		Timeout:   e.s.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	compression := otlpmetrichttp.GzipCompression
	if e.s.Compression == "none" {
		compression = otlpmetrichttp.NoCompression
	}
	initial, maxInterval, maxElapsed := e.retry()

	exp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(e.s.Endpoint),
		otlpmetrichttp.WithHTTPClient(client),
		otlpmetrichttp.WithTLSClientConfig(tlsCfg),
		otlpmetrichttp.WithHeaders(m.headers),
		otlpmetrichttp.WithCompression(compression),
		otlpmetrichttp.WithTimeout(e.s.Timeout),
		otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
			Enabled: true, InitialInterval: initial, MaxInterval: maxInterval, MaxElapsedTime: maxElapsed,
		}),
		otlpmetrichttp.WithMaxRequestSize(maxRequestHTTP),
		otlpmetrichttp.WithTemporalitySelector(sdkmetric.CumulativeTemporalitySelector),
		otlpmetrichttp.WithAggregationSelector(sdkmetric.DefaultAggregationSelector),
	)
	if err != nil {
		e.rt.CloseIdleConnections()
		return fmt.Errorf("create otlp http exporter: %w", err)
	}
	e.exp = exp
	return nil
}

// connectParams are grpc-go's backoff defaults with MaxDelay capped at half an interval and BaseDelay
// at MaxDelay, and MinConnectTimeout set to the per-attempt Timeout instead of grpc's 20s.
func (e *Exporter) connectParams() grpc.ConnectParams {
	maxDelay := min(e.s.Interval/2, 120*time.Second)
	return grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  min(time.Second, maxDelay), // grpc waits BaseDelay for the first retry whatever MaxDelay is
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   maxDelay,
		},
		MinConnectTimeout: e.s.Timeout,
	}
}

// initGRPC builds the OTLP/gRPC exporter on e's own connection.
func (e *Exporter) initGRPC(ctx context.Context, m material) error {
	creds := insecure.NewCredentials()
	if e.secure {
		creds = credentials.NewTLS(tlsConfig(e.s.TLS, m))
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(),
		grpc.WithConnectParams(e.connectParams()),
		grpc.WithUnaryInterceptor(boundAttempt(e.s.Timeout)),
		grpc.WithUserAgent("OTel Go OTLP over gRPC metrics exporter/" + otlpmetricgrpc.Version()),
	}
	if e.s.Compression != "none" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}
	conn, err := grpc.NewClient(net.JoinHostPort(e.address, strconv.Itoa(e.port)), opts...)
	if err != nil {
		return fmt.Errorf("create grpc client: %w", err)
	}

	initial, maxInterval, maxElapsed := e.retry()
	expOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithGRPCConn(conn),
		otlpmetricgrpc.WithHeaders(m.headers),
		otlpmetricgrpc.WithTimeout(e.exportTimeout()),
		otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
			Enabled: true, InitialInterval: initial, MaxInterval: maxInterval, MaxElapsedTime: maxElapsed,
		}),
		otlpmetricgrpc.WithMaxRequestSize(maxRequestGRPC),
		otlpmetricgrpc.WithTemporalitySelector(sdkmetric.CumulativeTemporalitySelector),
		otlpmetricgrpc.WithAggregationSelector(sdkmetric.DefaultAggregationSelector),
	}
	if e.s.Compression != "none" {
		expOpts = append(expOpts, otlpmetricgrpc.WithCompressor(gzip.Name))
	}
	exp, err := otlpmetricgrpc.New(ctx, expOpts...)
	if err != nil {
		return errors.Join(fmt.Errorf("create otlp grpc exporter: %w", err), conn.Close())
	}
	e.exp, e.conn = exp, conn
	return nil
}

// ComponentType is the semconv otel.component.type of the exporter.
func (e *Exporter) ComponentType() string { return e.component }

// export sends rm and returns the last HTTP status the transport saw, 0 for gRPC or no response.
func (e *Exporter) export(ctx context.Context, rm *metricdata.ResourceMetrics) (int, error) {
	// Created before the SDK's own export timeout, so it expires first with context.DeadlineExceeded.
	ctx, cancel := context.WithTimeout(ctx, e.exportTimeout())
	defer cancel()
	a := &attempts{}
	err := e.exp.Export(withAttempts(ctx, a), rm)
	last, final := int(a.last.Load()), int(a.final.Load())
	if err == nil {
		return last, nil
	}
	if final != 0 && (final < 200 || final > 299) {
		err = &httpStatusError{code: final, err: err}
	}
	return last, fmt.Errorf("otlp export: %w", err)
}

// Shutdown closes the SDK exporter and its connection.
func (e *Exporter) Shutdown(ctx context.Context) error {
	var errs []error
	if err := e.exp.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("shut down otlp exporter: %w", err))
	}
	if e.conn != nil {
		if err := e.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close grpc connection: %w", err))
		}
	}
	if e.rt != nil {
		e.rt.CloseIdleConnections()
	}
	return errors.Join(errs...)
}
