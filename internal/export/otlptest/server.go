// Package otlptest is an OTLP/HTTP and OTLP/gRPC metrics receiver for tests.
package otlptest

import (
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // registers the gzip decompressor
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Request is one export the server received.
type Request struct {
	Path            string      // HTTP only
	Header          http.Header // HTTP headers, or gRPC metadata
	ContentEncoding string      // HTTP Content-Encoding, or the gRPC grpc-encoding
	ClientCerts     []*x509.Certificate
	Metrics         *colmetricpb.ExportMetricsServiceRequest // nil when the body did not decode
	Received        time.Time
}

// ClientCN is the common name of the client certificate, or "" without one.
func (r Request) ClientCN() string {
	if len(r.ClientCerts) == 0 {
		return ""
	}
	return r.ClientCerts[0].Subject.CommonName
}

// Points counts the data points in the request.
func (r Request) Points() int {
	if r.Metrics == nil {
		return 0
	}
	n := 0
	for _, rm := range r.Metrics.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += len(m.GetGauge().GetDataPoints()) + len(m.GetSum().GetDataPoints()) +
					len(m.GetHistogram().GetDataPoints())
			}
		}
	}
	return n
}

// Response is how the server answers one request.
type Response struct {
	HTTPStatus int             // HTTP status; 0 is 200
	RetryAfter string          // HTTP Retry-After header
	Header     http.Header     // more HTTP response headers
	Code       codes.Code      // gRPC status code
	Rejected   int64           // partial success: rejected data points
	Message    string          // partial success: error message
	Delay      time.Duration   // wait before answering
	Block      <-chan struct{} // answer only once closed
}

// Server records every export and answers with the scripted responses.
type Server struct {
	// URL is the endpoint: http:// or https://, host and port.
	URL string

	mu        sync.Mutex // guards requests, responses and errs
	requests  []Request
	responses []Response
	errs      []error

	closed    chan struct{}
	closeOnce sync.Once
	stop      func()
}

// Respond scripts the answers: one per request in order, the last one repeating.
func (s *Server) Respond(rs ...Response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(rs) == 0 {
		rs = []Response{{}}
	}
	s.responses = rs
}

// Requests returns the requests received so far.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// Err returns the errors the server hit while answering, joined.
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.errs...)
}

func (s *Server) note(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
}

// Close releases blocked requests and stops the server.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.stop()
	})
}

func newServer() *Server {
	return &Server{responses: []Response{{}}, closed: make(chan struct{})}
}

// record stores r and returns the response for it.
func (s *Server) record(r Request) Response {
	r.Received = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r)
	resp := s.responses[0]
	if len(s.responses) > 1 {
		s.responses = s.responses[1:]
	}
	return resp
}

// wait holds the response for its Delay and Block, or until ctx ends or the server closes.
func (s *Server) wait(ctx context.Context, r Response) {
	if r.Delay > 0 {
		t := time.NewTimer(r.Delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
		case <-s.closed:
		}
	}
	if r.Block != nil {
		select {
		case <-r.Block:
		case <-ctx.Done():
		case <-s.closed:
		}
	}
}

func partialSuccess(r Response) *colmetricpb.ExportMetricsServiceResponse {
	out := &colmetricpb.ExportMetricsServiceResponse{}
	if r.Rejected != 0 || r.Message != "" {
		out.PartialSuccess = &colmetricpb.ExportMetricsPartialSuccess{RejectedDataPoints: r.Rejected, ErrorMessage: r.Message}
	}
	return out
}

// NewHTTP starts an OTLP/HTTP receiver on 127.0.0.1, with TLS when tlsCfg is not nil.
func NewHTTP(tlsCfg *tls.Config) *Server {
	s := newServer()
	hs := httptest.NewUnstartedServer(http.HandlerFunc(s.serveHTTP))
	if tlsCfg != nil {
		hs.TLS = tlsCfg
		hs.StartTLS()
	} else {
		hs.Start()
	}
	s.URL = hs.URL
	s.stop = hs.Close
	return s
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	req := Request{Path: r.URL.Path, Header: r.Header.Clone(), ContentEncoding: r.Header.Get("Content-Encoding")}
	if r.TLS != nil {
		req.ClientCerts = r.TLS.PeerCertificates
	}
	m, err := decodeHTTP(r)
	if err != nil {
		s.note(err)
	}
	req.Metrics = m
	resp := s.record(req)
	s.wait(r.Context(), resp)

	if resp.HTTPStatus != 0 && resp.HTTPStatus != http.StatusOK {
		for k, vs := range resp.Header {
			w.Header()[k] = vs
		}
		if resp.RetryAfter != "" {
			w.Header().Set("Retry-After", resp.RetryAfter)
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(resp.HTTPStatus)
		if _, err := io.WriteString(w, "otlptest: status "+strconv.Itoa(resp.HTTPStatus)); err != nil {
			s.note(fmt.Errorf("write response: %w", err))
		}
		return
	}
	body, err := proto.Marshal(partialSuccess(resp))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	if _, err := w.Write(body); err != nil {
		s.note(fmt.Errorf("write response: %w", err))
	}
}

func decodeHTTP(r *http.Request) (*colmetricpb.ExportMetricsServiceRequest, error) {
	data, err := readBody(r)
	if err != nil {
		return nil, err
	}
	m := &colmetricpb.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	return m, nil
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Header.Get("Content-Encoding") != "gzip" {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
		return data, nil
	}
	gz, err := gzip.NewReader(r.Body)
	if err != nil {
		return nil, fmt.Errorf("open gzip body: %w", err)
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		err = fmt.Errorf("read gzip body: %w", err)
	}
	if cerr := gz.Close(); cerr != nil {
		err = errors.Join(err, fmt.Errorf("close gzip body: %w", cerr))
	}
	return data, err
}

type metricsService struct {
	colmetricpb.UnimplementedMetricsServiceServer
	s *Server
}

// NewGRPC starts an OTLP/gRPC receiver on 127.0.0.1, with TLS when tlsCfg is not nil.
func NewGRPC(tlsCfg *tls.Config) (*Server, error) {
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	s := newServer()
	opts := []grpc.ServerOption{grpc.StatsHandler(encodingRecorder{})}
	scheme := "http://"
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		scheme = "https://"
	}
	gs := grpc.NewServer(opts...)
	colmetricpb.RegisterMetricsServiceServer(gs, &metricsService{s: s})
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := gs.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.note(fmt.Errorf("serve grpc: %w", err))
		}
	}()
	s.URL = scheme + lis.Addr().String()
	s.stop = func() {
		gs.Stop()
		<-served
	}
	return s, nil
}

func (m *metricsService) Export(ctx context.Context, in *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	req := Request{Header: http.Header{}, Metrics: in}
	md, _ := metadata.FromIncomingContext(ctx)
	for k, vs := range md {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if enc, ok := ctx.Value(encodingKey{}).(*atomic.Value); ok {
		if v, ok := enc.Load().(string); ok {
			req.ContentEncoding = v
		}
	}
	if p, ok := peer.FromContext(ctx); ok {
		if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			req.ClientCerts = info.State.PeerCertificates
		}
	}
	resp := m.s.record(req)
	m.s.wait(ctx, resp)
	if resp.Code != codes.OK {
		return nil, status.Error(resp.Code, "otlptest: "+resp.Code.String()) //nolint:wrapcheck // gRPC sends the status as is
	}
	return partialSuccess(resp), nil
}

type encodingKey struct{}

// encodingRecorder keeps the grpc-encoding of each RPC, which metadata does not carry.
type encodingRecorder struct{}

func (encodingRecorder) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, encodingKey{}, &atomic.Value{})
}

func (encodingRecorder) HandleRPC(ctx context.Context, st stats.RPCStats) {
	h, ok := st.(*stats.InHeader)
	if !ok {
		return
	}
	if enc, ok := ctx.Value(encodingKey{}).(*atomic.Value); ok {
		enc.Store(h.Compression)
	}
}

func (encodingRecorder) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (encodingRecorder) HandleConn(context.Context, stats.ConnStats) {}

// CA is a certificate authority for test certificates.
type CA struct {
	// PEM is the CA certificate.
	PEM []byte
	// Pool holds only the CA certificate.
	Pool *x509.CertPool

	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// NewCA creates a CA named name.
func NewCA(name string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ca key: %w", err)
	}
	tmpl, err := template(name)
	if err != nil {
		return nil, err
	}
	tmpl.IsCA = true
	tmpl.BasicConstraintsValid = true
	tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create ca certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse ca certificate: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{PEM: pemBlock("CERTIFICATE", der), Pool: pool, cert: cert, key: key}, nil
}

// Issue returns a PEM certificate and key named name, for 127.0.0.1, ::1 and localhost,
// usable by a server and by a client.
func (ca *CA) Issue(name string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	tmpl, err := template(name)
	if err != nil {
		return nil, nil, err
	}
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	tmpl.DNSNames = []string{"localhost"}
	tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal key: %w", err)
	}
	return pemBlock("CERTIFICATE", der), pemBlock("PRIVATE KEY", keyDER), nil
}

// ServerTLS returns a server configuration with a certificate from ca that asks for a
// client certificate and verifies one signed by clientCA when the client sends it.
func (ca *CA) ServerTLS(clientCA *CA) (*tls.Config, error) {
	certPEM, keyPEM, err := ca.Issue("otlptest-server")
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{pair},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    clientCA.Pool,
	}, nil
}

func template(name string) (*x509.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now()
	return &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
	}, nil
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}
