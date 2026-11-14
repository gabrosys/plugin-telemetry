package export_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/export/otlptest"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

func exporterSettings(protocol, endpoint string) settings.Exporter {
	return settings.Exporter{
		Protocol:            protocol,
		Endpoint:            endpoint,
		TLS:                 settings.TLS{MinVersion: "1.2"},
		Compression:         "gzip",
		Interval:            10 * time.Second,
		Timeout:             2 * time.Second,
		MaxPointsPerRequest: 1000,
	}
}

// batch returns one gauge with n points, each with an attribute of size bytes.
func batch(n, size int) *metricdata.ResourceMetrics {
	v := strings.Repeat("x", size)
	dps := make([]metricdata.DataPoint[float64], n)
	for i := range dps {
		dps[i] = metricdata.DataPoint[float64]{
			Attributes: attribute.NewSet(attribute.Int("i", i), attribute.String("v", v)),
			Time:       time.Unix(1, 0),
			Value:      float64(i),
		}
	}
	return &metricdata.ResourceMetrics{
		Resource: resource.NewWithAttributes("", attribute.String("service.name", "pg")),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope:   instrumentation.Scope{Name: "s"},
			Metrics: []metricdata.Metrics{{Name: "g", Data: metricdata.Gauge[float64]{DataPoints: dps}}},
		}},
	}
}

func writeFile(dir, name string, data []byte) string {
	GinkgoHelper()
	p := filepath.Join(dir, name)
	Expect(os.WriteFile(p, data, 0o600)).To(Succeed())
	return p
}

func newExporter(s settings.Exporter) *export.Exporter {
	GinkgoHelper()
	e, err := export.NewExporter(context.Background(), s)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) { Expect(e.Shutdown(ctx)).To(Succeed()) })
	return e
}

func serverTLS(ca *otlptest.CA) *tls.Config {
	GinkgoHelper()
	cfg, err := ca.ServerTLS(pki.caA)
	Expect(err).NotTo(HaveOccurred())
	return cfg
}

func newGRPCServer(cfg *tls.Config) *otlptest.Server {
	GinkgoHelper()
	srv, err := otlptest.NewGRPC(cfg)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(srv.Close)
	return srv
}

func newHTTPServer(cfg *tls.Config) *otlptest.Server {
	GinkgoHelper()
	srv := otlptest.NewHTTP(cfg)
	DeferCleanup(srv.Close)
	return srv
}

func expectClass(err error, errorType string, rejected int) {
	GinkgoHelper()
	gotType, gotRejected := export.Classify(err)
	Expect(gotType).To(Equal(errorType))
	Expect(gotRejected).To(Equal(rejected))
}

// junkEnv points every variable the exporters and net/http read somewhere else.
func junkEnv(dir string) {
	GinkgoHelper()
	ca := writeFile(dir, "junk-ca.pem", pki.caB.PEM)
	cert := writeFile(dir, "junk.crt", pki.client2.cert)
	key := writeFile(dir, "junk.key", pki.client2.key)
	for _, p := range []string{"OTEL_EXPORTER_OTLP_", "OTEL_EXPORTER_OTLP_METRICS_"} {
		GinkgoT().Setenv(p+"ENDPOINT", "https://127.0.0.1:1/junk")
		GinkgoT().Setenv(p+"CERTIFICATE", ca)
		GinkgoT().Setenv(p+"CLIENT_CERTIFICATE", cert)
		GinkgoT().Setenv(p+"CLIENT_KEY", key)
		GinkgoT().Setenv(p+"INSECURE", "false")
		GinkgoT().Setenv(p+"HEADERS", "x-junk=1")
		GinkgoT().Setenv(p+"COMPRESSION", "none")
		GinkgoT().Setenv(p+"TIMEOUT", "1")
		GinkgoT().Setenv(p+"PROTOCOL", "http/json")
	}
	GinkgoT().Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta")
	GinkgoT().Setenv("OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION", "base2_exponential_bucket_histogram")
	for _, v := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
		GinkgoT().Setenv(v, "http://127.0.0.1:1")
	}
	GinkgoT().Setenv("NO_PROXY", "")
	GinkgoT().Setenv("no_proxy", "")
}

var _ = Describe("Exporter", func() {
	var dir string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
	})

	Context("over HTTP", func() {
		It("sends gzip-compressed protobuf to the endpoint URL path as given", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			e := newExporter(exporterSettings(settings.ProtocolHTTP, srv.URL+"/custom/otlp"))

			Expect(e.Export(ctx, batch(3, 1))).To(Succeed())

			reqs := srv.Requests()
			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].Path).To(Equal("/custom/otlp"))
			Expect(reqs[0].ContentEncoding).To(Equal("gzip"))
			Expect(reqs[0].Header.Get("Content-Type")).To(Equal("application/x-protobuf"))
			Expect(reqs[0].Points()).To(Equal(3))
			Expect(reqs[0].Metrics.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()[0].GetName()).To(Equal("g"))
			Expect(e.ComponentType()).To(Equal("otlp_http_metric_exporter"))
		})

		It("sends uncompressed protobuf with compression none", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.Compression = "none"
			e := newExporter(s)

			Expect(e.Export(ctx, batch(2, 1))).To(Succeed())

			reqs := srv.Requests()
			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].ContentEncoding).To(BeEmpty())
			Expect(reqs[0].Points()).To(Equal(2))
		})

		It("ignores every OTEL_EXPORTER_OTLP_* variable and HTTP(S)_PROXY", func(ctx SpecContext) {
			junkEnv(dir)
			srv := newHTTPServer(nil)
			e := newExporter(exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics"))

			Expect(e.Export(ctx, batch(2, 1))).To(Succeed())

			reqs := srv.Requests()
			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].Path).To(Equal("/v1/metrics"))
			Expect(reqs[0].ContentEncoding).To(Equal("gzip"))
			Expect(reqs[0].Header.Get("X-Junk")).To(BeEmpty())
			Expect(reqs[0].Points()).To(Equal(2))
			Expect(export.HTTPProxy(e)).To(BeNil())
		})

		It("adds the headers file's entries to every request and picks up a change without a new exporter", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.HeadersFile = writeFile(dir, "headers", []byte("Authorization=Bearer%20one, x-tenant = a\n"))
			e := newExporter(s)

			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())
			writeFile(dir, "headers", []byte("x-tenant=b"))
			Expect(e.Reload()).To(Succeed())
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			reqs := srv.Requests()
			Expect(reqs).To(HaveLen(3))
			for _, r := range reqs[:2] {
				Expect(r.Header.Get("Authorization")).To(Equal("Bearer one"))
				Expect(r.Header.Get("X-Tenant")).To(Equal("a"))
			}
			Expect(reqs[2].Header.Get("X-Tenant")).To(Equal("b"))
			Expect(reqs[2].Header.Get("Authorization")).To(BeEmpty())
		})

		It("keeps the last good headers when Reload fails, without the values in the error", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.HeadersFile = writeFile(dir, "headers", []byte("x-tenant=a"))
			e := newExporter(s)

			writeFile(dir, "headers", []byte("x-tenant=secret-value,broken"))
			err := e.Reload()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).NotTo(ContainSubstring("secret-value"))
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			Expect(srv.Requests()[0].Header.Get("X-Tenant")).To(Equal("a"))
		})

		It("presents the client certificate and picks up a renewed one without a new exporter", func(ctx SpecContext) {
			srv := newHTTPServer(serverTLS(pki.caA))
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.TLS.CAFile = writeFile(dir, "ca.pem", pki.caA.PEM)
			s.TLS.CertFile = writeFile(dir, "tls.crt", pki.client1.cert)
			s.TLS.KeyFile = writeFile(dir, "tls.key", pki.client1.key)
			e := newExporter(s)

			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())
			writeFile(dir, "tls.crt", pki.client2.cert)
			writeFile(dir, "tls.key", pki.client2.key)
			Expect(e.Reload()).To(Succeed())
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			reqs := srv.Requests()
			Expect(reqs).To(HaveLen(2))
			Expect(reqs[0].ClientCN()).To(Equal("client-1"))
			Expect(reqs[1].ClientCN()).To(Equal("client-2"))
		})

		It("trusts a CA file in place of the system roots after Reload", func(ctx SpecContext) {
			srv := newHTTPServer(serverTLS(pki.caB))
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.TLS.CAFile = writeFile(dir, "ca.pem", pki.caA.PEM)
			e := newExporter(s)

			Expect(e.Export(ctx, batch(1, 1))).NotTo(Succeed())
			Expect(srv.Requests()).To(BeEmpty())

			writeFile(dir, "ca.pem", pki.caB.PEM)
			Expect(e.Reload()).To(Succeed())
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())
			Expect(srv.Requests()).To(HaveLen(1))
		})

		It("verifies the server name from tls.serverName and refuses a server below tls.minVersion", func(ctx SpecContext) {
			cfg := serverTLS(pki.caA)
			cfg.MaxVersion = tls.VersionTLS12
			srv := newHTTPServer(cfg)
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.TLS.CAFile = writeFile(dir, "ca.pem", pki.caA.PEM)

			s.TLS.ServerName = "otlp.invalid"
			Expect(newExporter(s).Export(ctx, batch(1, 1))).NotTo(Succeed())
			s.TLS.ServerName = "localhost"
			Expect(newExporter(s).Export(ctx, batch(1, 1))).To(Succeed())
			s.TLS.MinVersion = "1.3"
			Expect(newExporter(s).Export(ctx, batch(1, 1))).NotTo(Succeed())

			Expect(srv.Requests()).To(HaveLen(1))
		})

		It("retries 503 with Retry-After and stops within 0.9 × interval", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			srv.Respond(otlptest.Response{HTTPStatus: 503, RetryAfter: "1"})
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.Interval = 2 * time.Second
			s.Timeout = 500 * time.Millisecond
			e := newExporter(s)

			start := time.Now()
			err := e.Export(ctx, batch(1, 1))
			elapsed := time.Since(start)

			Expect(err).To(HaveOccurred())
			expectClass(err, "503", 0)
			reqs := srv.Requests()
			Expect(len(reqs)).To(BeNumerically(">=", 2))
			Expect(reqs[1].Received.Sub(reqs[0].Received)).To(BeNumerically(">=", time.Second))
			Expect(elapsed).To(BeNumerically("<", 1800*time.Millisecond))
		}, SpecTimeout(10*time.Second))

		It("times out each attempt after exporter.timeout and retries", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			srv.Respond(otlptest.Response{Delay: 5 * time.Second}, otlptest.Response{})
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.Timeout = 200 * time.Millisecond
			e := newExporter(s)

			start := time.Now()
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			Expect(time.Since(start)).To(BeNumerically("<", 3*time.Second))
			Expect(srv.Requests()).To(HaveLen(2))
		}, SpecTimeout(10*time.Second))

		It("doesn't retry 400 and classifies it as error.type 400", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			srv.Respond(otlptest.Response{HTTPStatus: 400})
			e := newExporter(exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics"))

			err := e.Export(ctx, batch(1, 1))

			Expect(err).To(HaveOccurred())
			expectClass(err, "400", 0)
			Expect(srv.Requests()).To(HaveLen(1))
		})

		It("doesn't follow a redirect, so the headers never reach another host", func(ctx SpecContext) {
			other := newHTTPServer(nil)
			srv := newHTTPServer(nil)
			srv.Respond(otlptest.Response{
				HTTPStatus: 307,
				Header:     http.Header{"Location": {other.URL + "/v1/metrics"}},
			})
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.HeadersFile = writeFile(dir, "headers", []byte("authorization=secret"))
			e := newExporter(s)

			err := e.Export(ctx, batch(1, 1))

			expectClass(err, "307", 0)
			Expect(srv.Requests()).To(HaveLen(1))
			Expect(other.Requests()).To(BeEmpty())
		})

		It("counts a partial success as rejected points, not retried", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			srv.Respond(otlptest.Response{Rejected: 3, Message: "bad points"})
			e := newExporter(exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics"))

			err := e.Export(ctx, batch(5, 1))

			Expect(err).To(MatchError(ContainSubstring("OTLP partial success: bad points (3 metric data points rejected)")))
			expectClass(err, "rejected", 3)
			Expect(srv.Requests()).To(HaveLen(1))
		})

		It("classifies a context deadline as timeout and an oversized request as request_too_large", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			srv.Respond(otlptest.Response{Delay: 5 * time.Second}, otlptest.Response{})
			s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
			s.Timeout = 200 * time.Millisecond
			e := newExporter(s)

			tctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			err := e.Export(tctx, batch(1, 1))
			Expect(err).To(HaveOccurred())
			expectClass(err, "timeout", 0)

			sent := len(srv.Requests())
			err = e.Export(ctx, batch(17, 1<<20))
			Expect(err).To(MatchError(ContainSubstring("request body too large: exceeded 16777216 bytes")))
			expectClass(err, "request_too_large", 0)
			Expect(srv.Requests()).To(HaveLen(sent))
		}, SpecTimeout(10*time.Second))

		It("refuses exports and reloads after Shutdown", func(ctx SpecContext) {
			srv := newHTTPServer(nil)
			e := newExporter(exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics"))

			Expect(e.Shutdown(ctx)).To(Succeed())

			Expect(e.Export(ctx, batch(1, 1))).To(MatchError(export.ErrExporterShutdown))
			Expect(e.Reload()).To(MatchError(export.ErrExporterShutdown))
			Expect(srv.Requests()).To(BeEmpty())
		})
	})

	Context("over gRPC", func() {
		var (
			srv *otlptest.Server
			s   settings.Exporter
		)

		BeforeEach(func() {
			srv = newGRPCServer(serverTLS(pki.caA))
			s = exporterSettings(settings.ProtocolGRPC, srv.URL)
			s.TLS.CAFile = writeFile(dir, "ca.pem", pki.caA.PEM)
			s.TLS.CertFile = writeFile(dir, "tls.crt", pki.client1.cert)
			s.TLS.KeyFile = writeFile(dir, "tls.key", pki.client1.key)
			s.HeadersFile = writeFile(dir, "headers", []byte("x-tenant=a"))
		})

		It("sends gzip-compressed requests over TLS with the client certificate and the headers file's entries", func(ctx SpecContext) {
			e := newExporter(s)

			Expect(e.Export(ctx, batch(3, 1))).To(Succeed())
			writeFile(dir, "headers", []byte("x-tenant=b"))
			Expect(e.Reload()).To(Succeed())
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			reqs := srv.Requests()
			Expect(reqs).To(HaveLen(2))
			Expect(reqs[0].ContentEncoding).To(Equal("gzip"))
			Expect(reqs[0].ClientCN()).To(Equal("client-1"))
			Expect(reqs[0].Header.Get("X-Tenant")).To(Equal("a"))
			Expect(reqs[0].Points()).To(Equal(3))
			Expect(reqs[1].Header.Get("X-Tenant")).To(Equal("b"))
			Expect(e.ComponentType()).To(Equal("otlp_grpc_metric_exporter"))
		})

		It("redials when Reload finds a new CA file", func(ctx SpecContext) {
			s.TLS.CAFile = writeFile(dir, "ca.pem", pki.caB.PEM)
			s.Interval = time.Second
			s.Timeout = 300 * time.Millisecond
			e := newExporter(s)

			Expect(e.Export(ctx, batch(1, 1))).NotTo(Succeed())
			Expect(srv.Requests()).To(BeEmpty())

			writeFile(dir, "ca.pem", pki.caA.PEM)
			Expect(e.Reload()).To(Succeed())
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())
			Expect(srv.Requests()).To(HaveLen(1))
		}, SpecTimeout(10*time.Second))

		It("times out each attempt after exporter.timeout and retries", func(ctx SpecContext) {
			srv.Respond(otlptest.Response{Delay: 5 * time.Second}, otlptest.Response{})
			s.Timeout = 200 * time.Millisecond
			e := newExporter(s)

			start := time.Now()
			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			Expect(time.Since(start)).To(BeNumerically("<", 3*time.Second))
			Expect(srv.Requests()).To(HaveLen(2))
		}, SpecTimeout(10*time.Second))

		It("bounds the whole export at 0.9 × interval and classifies it as timeout", func(ctx SpecContext) {
			srv.Respond(otlptest.Response{Code: codes.Unavailable})
			s.Interval = 500 * time.Millisecond
			s.Timeout = 100 * time.Millisecond
			e := newExporter(s)

			start := time.Now()
			err := e.Export(ctx, batch(1, 1))

			expectClass(err, "timeout", 0)
			Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		}, SpecTimeout(10*time.Second))

		It("retries Unavailable", func(ctx SpecContext) {
			srv.Respond(otlptest.Response{Code: codes.Unavailable}, otlptest.Response{})
			e := newExporter(s)

			Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

			Expect(srv.Requests()).To(HaveLen(2))
		}, SpecTimeout(10*time.Second))

		It("counts a partial success as rejected points, not retried", func(ctx SpecContext) {
			srv.Respond(otlptest.Response{Rejected: 2, Message: "bad"})
			e := newExporter(s)

			err := e.Export(ctx, batch(4, 1))

			Expect(err).To(MatchError(ContainSubstring("OTLP partial success: bad (2 metric data points rejected)")))
			expectClass(err, "rejected", 2)
			Expect(srv.Requests()).To(HaveLen(1))
		})

		It("classifies a status as its code name, a deadline as timeout and an oversized request as request_too_large", func(ctx SpecContext) {
			srv.Respond(otlptest.Response{Code: codes.InvalidArgument}, otlptest.Response{Delay: 5 * time.Second})
			e := newExporter(s)
			short := s
			short.Interval = time.Second
			short.Timeout = 300 * time.Millisecond
			slow := newExporter(short)

			err := e.Export(ctx, batch(1, 1))
			expectClass(err, "InvalidArgument", 0)
			Expect(srv.Requests()).To(HaveLen(1))

			err = slow.Export(ctx, batch(1, 1))
			expectClass(err, "timeout", 0)

			sent := len(srv.Requests())
			err = e.Export(ctx, batch(5, 1<<20))
			Expect(err).To(MatchError(ContainSubstring("request message too large: exceeded 4194304 bytes")))
			expectClass(err, "request_too_large", 0)
			Expect(srv.Requests()).To(HaveLen(sent))
		}, SpecTimeout(10*time.Second))

		It("ignores every OTEL_EXPORTER_OTLP_* variable and HTTP(S)_PROXY", func(ctx SpecContext) {
			junkEnv(dir)
			plain := newGRPCServer(nil)
			p := exporterSettings(settings.ProtocolGRPC, plain.URL)
			p.HeadersFile = s.HeadersFile
			e := newExporter(p)

			Expect(e.Export(ctx, batch(2, 1))).To(Succeed())

			reqs := plain.Requests()
			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].ContentEncoding).To(Equal("gzip"))
			Expect(reqs[0].Header.Get("X-Junk")).To(BeEmpty())
			Expect(reqs[0].Header.Get("X-Tenant")).To(Equal("a"))
			Expect(reqs[0].Points()).To(Equal(2))
		})
	})

	DescribeTable("NewExporter rejects",
		func(mutate func(s *settings.Exporter, dir string), msg string) {
			s := exporterSettings(settings.ProtocolHTTP, "https://127.0.0.1:4318/v1/metrics")
			mutate(&s, dir)

			_, err := export.NewExporter(context.Background(), s)

			Expect(err).To(MatchError(ContainSubstring(msg)))
		},
		Entry("an unknown protocol", func(s *settings.Exporter, _ string) { s.Protocol = "http/json" }, "protocol"),
		Entry("an endpoint that is not an http or https URL",
			func(s *settings.Exporter, _ string) { s.Endpoint = "ftp://127.0.0.1:4318" }, "endpoint"),
		Entry("a timeout that leaves no time to retry within 0.9 × interval",
			func(s *settings.Exporter, _ string) { s.Timeout = s.Interval }, "timeout"),
		Entry("a timeout that is not positive", func(s *settings.Exporter, _ string) { s.Timeout = 0 }, "timeout"),
		Entry("a missing headers file",
			func(s *settings.Exporter, dir string) { s.HeadersFile = filepath.Join(dir, "missing") }, "headers"),
		Entry("a headers entry without =",
			func(s *settings.Exporter, dir string) {
				s.HeadersFile = writeFile(dir, "headers", []byte("a=1,novalue"))
			}, "headers"),
		Entry("a headers key gRPC metadata can't carry",
			func(s *settings.Exporter, dir string) { s.HeadersFile = writeFile(dir, "headers", []byte("a b=1")) }, "headers"),
		Entry("a headers value that is not printable ASCII",
			func(s *settings.Exporter, dir string) { s.HeadersFile = writeFile(dir, "headers", []byte("a=%C3%A9")) }, "headers"),
		Entry("a CA file without a certificate",
			func(s *settings.Exporter, dir string) { s.TLS.CAFile = writeFile(dir, "ca.pem", []byte("junk")) }, "ca"),
		Entry("a client key pair that doesn't load", func(s *settings.Exporter, dir string) {
			s.TLS.CertFile = writeFile(dir, "tls.crt", []byte("junk"))
			s.TLS.KeyFile = writeFile(dir, "tls.key", []byte("junk"))
		}, "client certificate"),
	)

	DescribeTable("Classify",
		func(err error, errorType string, rejected int) { expectClass(err, errorType, rejected) },
		Entry("no error", nil, "", 0),
		Entry("an unknown error", errors.New("boom"), "_OTHER", 0),
		Entry("a canceled context", fmt.Errorf("x: %w", context.Canceled), "_OTHER", 0),
		Entry("a wrapped context deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), "timeout", 0),
		Entry("a wrapped gRPC status", fmt.Errorf("x: %w", status.Error(codes.Unavailable, "down")), "Unavailable", 0),
		Entry("a gRPC export that ran out of time while retrying (otlpmetricgrpc v1.47.0 retry.go:113-114)",
			fmt.Errorf("failed to upload metrics: %w", fmt.Errorf("%w: %w", context.DeadlineExceeded, status.Error(codes.Unavailable, "down"))),
			"timeout", 0),
		Entry("a partial success wrapped as the SDK wraps it (otlpmetrichttp v1.47.0 exporter.go:81)",
			fmt.Errorf("failed to upload metrics: %w", errors.New("OTLP partial success: m (7) x (7 metric data points rejected)")),
			"rejected", 7),
	)
})
