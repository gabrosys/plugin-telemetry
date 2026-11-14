package export

import (
	"net/http"
	"net/url"
)

// HTTPProxy returns the Proxy of the HTTP exporter's current transport.
func HTTPProxy(e *Exporter) func(*http.Request) (*url.URL, error) {
	return e.rt.base.Load().Proxy
}
