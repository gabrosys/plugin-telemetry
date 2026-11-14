package export_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

// keepOTelEnv registers every OTEL_* variable already set, so the spec restores it.
func keepOTelEnv() []string {
	GinkgoHelper()
	var names []string
	for _, kv := range os.Environ() {
		if name, v, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "OTEL_") {
			GinkgoT().Setenv(name, v)
			names = append(names, name)
		}
	}
	return names
}

// otelLog captures what the OpenTelemetry SDK logs through its global logger.
type otelLog struct {
	mu    sync.Mutex // guards lines
	lines []string
}

func captureOTelLog() *otelLog {
	l := &otelLog{}
	otel.SetLogger(funcr.New(func(prefix, args string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, prefix+" "+args)
	}, funcr.Options{}))
	DeferCleanup(func() {
		otel.SetLogger(funcr.New(func(prefix, args string) { GinkgoWriter.Println(prefix, args) }, funcr.Options{}))
	})
	return l
}

func (l *otelLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

var _ = Describe("ClearEnv", func() {
	It("unsets every OTEL_ variable, returns their names sorted and leaves the others", func() {
		keepOTelEnv()
		GinkgoT().Setenv("OTEL_RESOURCE_ATTRIBUTES", "evil=1")
		GinkgoT().Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=secret")
		GinkgoT().Setenv("OTEL_GO_X_OBSERVABILITY", "true")
		GinkgoT().Setenv("PLUGIN_TELEMETRY_UNRELATED", "kept")
		want := keepOTelEnv()
		slices.Sort(want)

		removed := export.ClearEnv()

		Expect(removed).To(Equal(want))
		Expect(removed).To(ContainElements("OTEL_EXPORTER_OTLP_HEADERS", "OTEL_GO_X_OBSERVABILITY", "OTEL_RESOURCE_ATTRIBUTES"))
		for _, kv := range os.Environ() {
			Expect(kv).NotTo(HavePrefix("OTEL_"))
		}
		Expect(os.Getenv("PLUGIN_TELEMETRY_UNRELATED")).To(Equal("kept"))
		Expect(export.ClearEnv()).To(BeEmpty())
	})

	It("keeps the SDK from reading the files OTEL_EXPORTER_OTLP_CERTIFICATE names", func(ctx SpecContext) {
		keepOTelEnv()
		missing := filepath.Join(GinkgoT().TempDir(), "missing.pem")
		GinkgoT().Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", missing)
		srv := newHTTPServer(nil)
		s := exporterSettings(settings.ProtocolHTTP, srv.URL+"/v1/metrics")
		log := captureOTelLog()

		newExporter(s)
		Expect(log.String()).To(ContainSubstring(missing))

		export.ClearEnv()
		log = captureOTelLog()
		e := newExporter(s)
		Expect(e.Export(ctx, batch(1, 1))).To(Succeed())

		Expect(log.String()).NotTo(ContainSubstring(missing))
		Expect(srv.Requests()).To(HaveLen(1))
	})
})
