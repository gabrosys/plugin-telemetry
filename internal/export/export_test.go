package export

import (
	"context"
	"net/http"
	"net/url"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// HTTPProxy returns the Proxy of the HTTP exporter's transport.
func HTTPProxy(e *Exporter) func(*http.Request) (*url.URL, error) {
	return e.rt.base.Proxy
}

// SetMaxRequestSize sets the request caps of the HTTP and gRPC exporters built after it, and returns a restore func.
func SetMaxRequestSize(httpMax, grpcMax int) (restore func()) {
	oldHTTP, oldGRPC := maxRequestHTTP, maxRequestGRPC
	maxRequestHTTP, maxRequestGRPC = httpMax, grpcMax
	return func() { maxRequestHTTP, maxRequestGRPC = oldHTTP, oldGRPC }
}

// Export sends rm in one request, retrying for at most 0.9 × interval.
func (e *Exporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	_, err := e.export(ctx, rm)
	return err
}
