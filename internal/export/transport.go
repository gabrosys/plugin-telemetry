package export

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

// material is what Reload re-reads from the files named in the settings.
type material struct {
	headers map[string]string
	caPEM   []byte
	roots   *x509.CertPool   // system roots plus caPEM; nil for plaintext
	cert    *tls.Certificate // nil without a client certificate
}

// loadMaterial reads the headers file and, for TLS, the system roots, the CA file and the client key pair.
func loadMaterial(s settings.Exporter, secure bool) (material, error) {
	m := material{headers: map[string]string{}}
	if s.HeadersFile != "" {
		data, err := os.ReadFile(s.HeadersFile)
		if err != nil {
			return m, fmt.Errorf("read headers file: %w", err)
		}
		if m.headers, err = parseHeaders(string(data)); err != nil {
			return m, fmt.Errorf("parse headers file %s: %w", s.HeadersFile, err)
		}
	}
	if !secure {
		return m, nil
	}

	roots, err := x509.SystemCertPool()
	if err != nil {
		return m, fmt.Errorf("load system roots: %w", err)
	}
	if s.TLS.CAFile != "" {
		if m.caPEM, err = os.ReadFile(s.TLS.CAFile); err != nil {
			return m, fmt.Errorf("read ca file: %w", err)
		}
		if !roots.AppendCertsFromPEM(m.caPEM) {
			return m, fmt.Errorf("ca file %s: no PEM certificate", s.TLS.CAFile)
		}
	}
	m.roots = roots
	if s.TLS.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(s.TLS.CertFile, s.TLS.KeyFile)
		if err != nil {
			return m, fmt.Errorf("load client certificate: %w", err)
		}
		m.cert = &pair
	}
	return m, nil
}

// parseHeaders reads the OTEL_EXPORTER_OTLP_HEADERS format, "k=v,k2=v2" with percent-encoded
// values, as otlpmetrichttp v1.47.0 internal/envconfig/envconfig.go:159-190 does, but fails on a
// bad entry. Keys and values must be valid gRPC metadata (grpc v1.85.0 internal/metadata/metadata.go:90-144),
// which HTTP also accepts. Errors give the entry's position, never its content.
func parseHeaders(s string) (map[string]string, error) {
	out := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}
	for i, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("entry %d: missing '='", i+1)
		}
		k = strings.TrimSpace(k)
		if k == "" || strings.ContainsFunc(k, func(r rune) bool { return !isKeyChar(r) }) {
			return nil, fmt.Errorf("entry %d: key must be letters, digits, '-', '_' or '.'", i+1)
		}
		v, err := url.PathUnescape(v)
		if err != nil {
			return nil, fmt.Errorf("entry %d: invalid percent-encoding", i+1)
		}
		v = strings.TrimSpace(v)
		if strings.ContainsFunc(v, func(r rune) bool { return r < ' ' || r > '~' }) {
			return nil, fmt.Errorf("entry %d: value must be printable ASCII", i+1)
		}
		out[k] = v
	}
	return out, nil
}

func isKeyChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
}

// live holds what Reload swaps in place.
type live struct {
	headers atomic.Pointer[map[string]string]
	cert    atomic.Pointer[tls.Certificate]
}

func (l *live) currentHeaders() map[string]string { return *l.headers.Load() }

// clientCertificate serves tls.Config.GetClientCertificate from the current key pair.
func (l *live) clientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if c := l.cert.Load(); c != nil {
		return c, nil
	}
	return &tls.Certificate{}, nil
}

func tlsConfig(t settings.TLS, roots *x509.CertPool, l *live) *tls.Config {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: t.ServerName,
		RootCAs:    roots,
	}
	if t.MinVersion == "1.3" {
		cfg.MinVersion = tls.VersionTLS13
	}
	if t.CertFile != "" {
		cfg.GetClientCertificate = l.clientCertificate
	}
	return cfg
}

// newHTTPTransport mirrors otlpmetrichttp v1.47.0's own transport (client.go:48-59) without the proxy.
func newHTTPTransport(cfg *tls.Config) *http.Transport {
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       cfg,
	}
}

// roundTripper sends through the current *http.Transport, adds the current headers to every
// request and records each response status in the request's attempts.
type roundTripper struct {
	base atomic.Pointer[http.Transport]
	live *live
}

func (t *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	for k, v := range t.live.currentHeaders() {
		if out.Header.Get(k) == "" {
			out.Header.Set(k, v)
		}
	}
	a := attemptsFrom(req.Context())
	a.begin()
	resp, err := t.base.Load().RoundTrip(out)
	if resp != nil {
		a.response(resp.StatusCode)
	}
	return resp, err //nolint:wrapcheck // http.Client and url.Error.Temporary need the transport's error as is
}

// CloseIdleConnections closes the idle connections of the current transport.
func (t *roundTripper) CloseIdleConnections() { t.base.Load().CloseIdleConnections() }

// swap replaces the transport and closes the old one's idle connections; its busy ones
// close after IdleConnTimeout once their requests end.
func (t *roundTripper) swap(next *http.Transport) {
	if old := t.base.Swap(next); old != nil {
		old.CloseIdleConnections()
	}
}

// attempts records the HTTP responses of one export.
type attempts struct {
	last  atomic.Int64 // status of the last response, 0 before any
	final atomic.Int64 // status of the last attempt, 0 when it got no response
}

func (a *attempts) begin() {
	if a != nil {
		a.final.Store(0)
	}
}

func (a *attempts) response(code int) {
	if a != nil {
		a.last.Store(int64(code))
		a.final.Store(int64(code))
	}
}

type attemptsKey struct{}

func withAttempts(ctx context.Context, a *attempts) context.Context {
	return context.WithValue(ctx, attemptsKey{}, a)
}

func attemptsFrom(ctx context.Context) *attempts {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	return a
}

// rpcHeaders sends the current headers as gRPC metadata on every call.
type rpcHeaders struct {
	live   *live
	secure bool
}

func (h rpcHeaders) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return h.live.currentHeaders(), nil
}

func (h rpcHeaders) RequireTransportSecurity() bool { return h.secure }

// markDeadline adds context.DeadlineExceeded to the error of a call whose own context
// expired, which grpc-go reports only as a DeadlineExceeded status.
func markDeadline(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	}
	return err
}
