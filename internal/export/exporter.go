package export

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const (
	componentHTTP = "otlp_http_metric_exporter"
	componentGRPC = "otlp_grpc_metric_exporter"

	maxRequestHTTP = 16 << 20
	maxRequestGRPC = 4 << 20

	// shutdownTimeout bounds the shutdown of an exporter that is no longer used.
	shutdownTimeout = 5 * time.Second
)

// ErrExporterShutdown is returned by Export and Reload after Shutdown.
var ErrExporterShutdown = errors.New("exporter is shut down")

// Exporter wraps otlpmetrichttp or otlpmetricgrpc with our own HTTP client or gRPC
// connection, so no OTEL_EXPORTER_OTLP_* or proxy variable can change where or how it sends.
type Exporter struct {
	s         settings.Exporter
	component string
	address   string
	port      int
	secure    bool

	live live
	rt   *roundTripper // HTTP only

	reloadMu sync.Mutex // serializes Reload and guards caPEM
	caPEM    []byte

	mu         sync.Mutex // guards current, closed and retireErrs
	current    *backend
	closed     bool
	retireErrs []error
	retired    sync.WaitGroup // backends replaced by Reload, until they are closed
}

// backend is one SDK exporter and, for gRPC, the connection it sends on.
type backend struct {
	exp      sdkmetric.Exporter
	conn     *grpc.ClientConn
	inflight sync.WaitGroup
}

func (b *backend) close(ctx context.Context) error {
	var errs []error
	if err := b.exp.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("shut down otlp exporter: %w", err))
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close grpc connection: %w", err))
		}
	}
	return errors.Join(errs...)
}

// NewExporter builds the exporter from s and reads its TLS and header files.
func NewExporter(ctx context.Context, s settings.Exporter) (*Exporter, error) {
	var component string
	switch s.Protocol {
	case settings.ProtocolHTTP:
		component = componentHTTP
	case settings.ProtocolGRPC:
		component = componentGRPC
	default:
		return nil, fmt.Errorf("exporter protocol %q: must be %q or %q", s.Protocol, settings.ProtocolHTTP, settings.ProtocolGRPC)
	}
	u, err := url.Parse(s.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return nil, fmt.Errorf("exporter endpoint %q: not an http or https URL with a host", s.Endpoint)
	}
	if s.Timeout <= 0 || s.Interval*9/10 <= s.Timeout {
		return nil, fmt.Errorf("exporter timeout %s: must be positive and below 0.9 × interval %s", s.Timeout, s.Interval)
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

	m, err := loadMaterial(s, e.secure)
	if err != nil {
		return nil, err
	}
	e.store(m)

	if s.Protocol == settings.ProtocolHTTP {
		e.current, err = e.newHTTPBackend(ctx, m)
	} else {
		e.current, err = e.newGRPCBackend(ctx, m)
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}

// store publishes the headers, the client certificate and the CA of m.
func (e *Exporter) store(m material) {
	e.live.headers.Store(&m.headers)
	if m.cert != nil {
		e.live.cert.Store(m.cert)
	}
	e.caPEM = m.caPEM
}

// exportTimeout bounds one export with its retries.
func (e *Exporter) exportTimeout() time.Duration { return e.s.Interval * 9 / 10 }

func (e *Exporter) retry() (initial, maxInterval, maxElapsed time.Duration) {
	return time.Second, 5 * time.Second, e.exportTimeout() - e.s.Timeout
}

func (e *Exporter) newHTTPBackend(ctx context.Context, m material) (*backend, error) {
	e.rt = &roundTripper{live: &e.live}
	tlsCfg := tlsConfig(e.s.TLS, m.roots, &e.live)
	if !e.secure {
		tlsCfg = nil
	}
	e.rt.base.Store(newHTTPTransport(tlsCfg))
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
		otlpmetrichttp.WithHeaders(map[string]string{}),
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
		return nil, fmt.Errorf("create otlp http exporter: %w", err)
	}
	return &backend{exp: exp}, nil
}

func (e *Exporter) newGRPCBackend(ctx context.Context, m material) (*backend, error) {
	creds := insecure.NewCredentials()
	if e.secure {
		creds = credentials.NewTLS(tlsConfig(e.s.TLS, m.roots, &e.live))
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(rpcHeaders{live: &e.live, secure: e.secure}),
		grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(),
		grpc.WithUnaryInterceptor(boundAttempt(e.s.Timeout)),
		grpc.WithUserAgent("OTel Go OTLP over gRPC metrics exporter/" + otlpmetricgrpc.Version()),
	}
	if e.s.Compression != "none" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}
	conn, err := grpc.NewClient(net.JoinHostPort(e.address, strconv.Itoa(e.port)), opts...)
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}

	initial, maxInterval, maxElapsed := e.retry()
	expOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithGRPCConn(conn),
		otlpmetricgrpc.WithHeaders(map[string]string{}),
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
		return nil, errors.Join(fmt.Errorf("create otlp grpc exporter: %w", err), conn.Close())
	}
	return &backend{exp: exp, conn: conn}, nil
}

// ComponentType is the semconv otel.component.type of the exporter.
func (e *Exporter) ComponentType() string { return e.component }

// Reload re-reads the TLS and header files. HTTP swaps certificates, CA and headers in
// place; gRPC swaps the client certificate and headers in place and redials on a CA change.
// On error the exporter keeps what it had.
func (e *Exporter) Reload() error {
	e.reloadMu.Lock()
	defer e.reloadMu.Unlock()
	if e.isClosed() {
		return ErrExporterShutdown
	}

	m, err := loadMaterial(e.s, e.secure)
	if err != nil {
		return fmt.Errorf("reload exporter: %w", err)
	}
	caChanged := e.secure && !bytes.Equal(m.caPEM, e.caPEM)
	certChanged := m.cert != nil && !sameLeaf(m.cert, e.live.cert.Load())

	var next *backend
	if caChanged && e.s.Protocol == settings.ProtocolGRPC {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if next, err = e.newGRPCBackend(ctx, m); err != nil {
			return fmt.Errorf("reload exporter: %w", err)
		}
	}
	e.store(m)

	switch {
	case next != nil:
		return e.replace(next)
	case caChanged:
		e.rt.swap(newHTTPTransport(tlsConfig(e.s.TLS, m.roots, &e.live)))
	case certChanged && e.rt != nil:
		e.rt.CloseIdleConnections()
	}
	return nil
}

func sameLeaf(a, b *tls.Certificate) bool {
	return a != nil && b != nil && len(a.Certificate) > 0 && len(b.Certificate) > 0 &&
		bytes.Equal(a.Certificate[0], b.Certificate[0])
}

func (e *Exporter) isClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// replace makes next the current backend and closes the old one after its exports end.
func (e *Exporter) replace(next *backend) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return errors.Join(ErrExporterShutdown, next.close(ctx))
	}
	old := e.current
	e.current = next
	e.retired.Add(1)
	e.mu.Unlock()

	go func() {
		defer e.retired.Done()
		old.inflight.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := old.close(ctx); err != nil {
			e.mu.Lock()
			e.retireErrs = append(e.retireErrs, err)
			e.mu.Unlock()
		}
	}()
	return nil
}

// Export sends rm in one request, retrying for at most 0.9 × interval.
func (e *Exporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	_, err := e.export(ctx, rm)
	return err
}

// export sends rm and returns the last HTTP status the transport saw, 0 for gRPC or no response.
func (e *Exporter) export(ctx context.Context, rm *metricdata.ResourceMetrics) (int, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return 0, ErrExporterShutdown
	}
	b := e.current
	b.inflight.Add(1)
	e.mu.Unlock()
	defer b.inflight.Done()

	// Created before the SDK's own export timeout, so it expires first with context.DeadlineExceeded.
	ctx, cancel := context.WithTimeout(ctx, e.exportTimeout())
	defer cancel()
	a := &attempts{}
	err := b.exp.Export(withAttempts(ctx, a), rm)
	last, final := int(a.last.Load()), int(a.final.Load())
	if err == nil {
		return last, nil
	}
	if final != 0 && (final < 200 || final > 299) {
		err = &httpStatusError{code: final, err: err}
	}
	return last, fmt.Errorf("otlp export: %w", err)
}

// Shutdown waits for running exports, then closes the SDK exporter and its connections.
// If ctx ends first it returns, and the closing still happens once those exports end.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	cur := e.current
	e.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		cur.inflight.Wait()
		e.retired.Wait()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		err := cur.close(cctx)
		if e.rt != nil {
			e.rt.CloseIdleConnections()
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		result <- errors.Join(append([]error{err}, e.retireErrs...)...)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("shut down exporter: %w", ctx.Err())
	}
}
