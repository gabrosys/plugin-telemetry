package operator

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

// Values of the service and operation attributes of the hook metrics.
const (
	serviceLifecycle = "lifecycle"
	serviceOperator  = "operator"

	operationCreate    = "create"
	operationEvaluate  = "evaluate"
	operationSetStatus = "set_status"
)

// Outcomes of SetStatusInCluster, as recorded, and of identity reconciles, as recorded or logged.
const (
	outcomeNoop     = "noop"
	outcomeStatus   = "status"
	outcomeOK       = "ok"
	outcomeApplied  = "applied"
	outcomeError    = "error"
	outcomeInTheWay = "in the way"
)

const (
	pluginServiceName  = "plugin-telemetry"
	metricsCardinality = 10000
)

var errNoFiles = errors.New("the plugin's metrics exporter reads no files")

// PluginMetrics holds the plugin's self-metrics. A nil *PluginMetrics records nothing.
type PluginMetrics struct {
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
	// The instruments are set once by NewPluginMetrics.
	hookCalls  metric.Int64Counter
	duration   metric.Float64Histogram
	reconciles metric.Int64Counter
	// initErr is the instrument setup failure Run returns.
	initErr error
}

// NewPluginMetrics returns the plugin's self-metrics with their instruments registered on a
// MeterProvider that only Run reads.
func NewPluginMetrics() *PluginMetrics {
	m := &PluginMetrics{reader: sdkmetric.NewManualReader()}
	m.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(m.reader),
		sdkmetric.WithView(selfmetrics.Views()...),
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
		sdkmetric.WithCardinalityLimit(metricsCardinality),
	)
	m.initErr = m.register(m.mp.Meter(export.ScopeName, metric.WithInstrumentationVersion(version.Version)))

	return m
}

// register creates the instruments of the catalogue's plugin section on meter.
func (m *PluginMetrics) register(meter metric.Meter) error {
	for _, def := range selfmetrics.ByComponent(selfmetrics.ComponentPlugin) {
		switch selfmetrics.Instrument(def.Instrument) {
		case selfmetrics.InstrumentCounter:
			c, err := meter.Int64Counter(def.Name, metric.WithUnit(def.Unit), metric.WithDescription(def.Description))
			if err != nil {
				return fmt.Errorf("create counter %s: %w", def.Name, err)
			}
			switch def.Name {
			case selfmetrics.HookCalls:
				m.hookCalls = c
			case selfmetrics.IdentityReconciles:
				m.reconciles = c
			default:
				return fmt.Errorf("counter %s: no recorder", def.Name)
			}
		case selfmetrics.InstrumentHistogram:
			if def.Name != selfmetrics.HookDuration {
				return fmt.Errorf("histogram %s: no recorder", def.Name)
			}
			h, err := meter.Float64Histogram(def.Name, metric.WithUnit(def.Unit), metric.WithDescription(def.Description),
				metric.WithExplicitBucketBoundaries())
			if err != nil {
				return fmt.Errorf("create histogram %s: %w", def.Name, err)
			}
			m.duration = h
		case selfmetrics.InstrumentGauge:
			return fmt.Errorf("metric %s: gauges are not used by the plugin", def.Name)
		default:
			return fmt.Errorf("metric %s: unknown instrument %q", def.Name, def.Instrument)
		}
	}
	return m.missingInstruments()
}

// missingInstruments fails naming, sorted, each plugin metric that has no instrument.
func (m *PluginMetrics) missingInstruments() error {
	var missing []string
	for name, absent := range map[string]bool{
		selfmetrics.HookCalls:          m.hookCalls == nil,
		selfmetrics.HookDuration:       m.duration == nil,
		selfmetrics.IdentityReconciles: m.reconciles == nil,
	} {
		if absent {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	slices.Sort(missing)

	return fmt.Errorf("the catalogue has no plugin metric %s", strings.Join(missing, ", "))
}

// Hook records one hook call and its duration. A nil receiver records nothing.
func (m *PluginMetrics) Hook(ctx context.Context, service, operation, outcome string, d time.Duration) {
	if m == nil || m.initErr != nil {
		return
	}
	m.hookCalls.Add(ctx, 1, metric.WithAttributes(
		attribute.String("service", service),
		attribute.String("operation", operation),
		attribute.String("outcome", outcome)))
	m.duration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("service", service), attribute.String("operation", operation)))
}

// IdentityReconcile records one identity controller reconcile: outcome ok when err is nil, else
// outcome error with the reason of err. A nil receiver records nothing.
func (m *PluginMetrics) IdentityReconcile(ctx context.Context, err error) {
	if m == nil || m.initErr != nil {
		return
	}
	if err == nil {
		m.reconciles.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcomeOK)))

		return
	}
	m.reconciles.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcomeError), attribute.String("reason", failureReason(err))))
}

// reasonUnknown is the reason of an error with no known cause.
const reasonUnknown = "Unknown"

// apiReasons are the reasons the API defines (apimachinery v0.37.2 pkg/apis/meta/v1/types.go:919-1067, bar
// Unknown), which bound the reason attribute.
var apiReasons = map[metav1.StatusReason]struct{}{
	metav1.StatusReasonUnauthorized:          {},
	metav1.StatusReasonForbidden:             {},
	metav1.StatusReasonNotFound:              {},
	metav1.StatusReasonAlreadyExists:         {},
	metav1.StatusReasonConflict:              {},
	metav1.StatusReasonGone:                  {},
	metav1.StatusReasonInvalid:               {},
	metav1.StatusReasonServerTimeout:         {},
	metav1.StatusReasonStoreReadError:        {},
	metav1.StatusReasonTimeout:               {},
	metav1.StatusReasonTooManyRequests:       {},
	metav1.StatusReasonBadRequest:            {},
	metav1.StatusReasonMethodNotAllowed:      {},
	metav1.StatusReasonNotAcceptable:         {},
	metav1.StatusReasonRequestEntityTooLarge: {},
	metav1.StatusReasonUnsupportedMediaType:  {},
	metav1.StatusReasonInternalError:         {},
	metav1.StatusReasonExpired:               {},
	metav1.StatusReasonServiceUnavailable:    {},
}

// failureReason names why err failed: the API status reason when the API defines it, Timeout for a
// deadline, else Unknown. A server-supplied reason outside the API's list is Unknown.
func failureReason(err error) string {
	if reason := apierrors.ReasonForError(err); reason != metav1.StatusReasonUnknown {
		if _, ok := apiReasons[reason]; ok {
			return string(reason)
		}

		return reasonUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return string(metav1.StatusReasonTimeout)
	}

	return reasonUnknown
}

// collect reads the current metrics.
func (m *PluginMetrics) collect(ctx context.Context) (*metricdata.ResourceMetrics, error) {
	rm := &metricdata.ResourceMetrics{}
	if err := m.reader.Collect(ctx, rm); err != nil {
		return nil, fmt.Errorf("collect plugin metrics: %w", err)
	}

	return rm, nil
}

// Run exports the metrics over OTLP with s until ctx ends. The resource is service.name
// plugin-telemetry, service.namespace and k8s.namespace.name from POD_NAMESPACE, k8s.pod.name from
// POD_NAME and service.instance.id <namespace>.<pod>.plugin-telemetry.
func (m *PluginMetrics) Run(ctx context.Context, s settings.Exporter) error {
	if m.initErr != nil {
		return m.initErr
	}
	exp, err := export.NewExporter(ctx, s, func(string) ([]byte, error) { return nil, errNoFiles })
	if err != nil {
		return fmt.Errorf("create metrics exporter: %w", err)
	}

	rnd := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // jitter, not security
	loop := export.NewLoop(m.reader, pluginResource(os.Getenv), clock.RealClock{}, rnd, selfmetrics.NewStats(), exp, s.Interval, s.MaxPointsPerRequest)

	runErr := loop.Run(ctx)
	if err := m.mp.Shutdown(context.WithoutCancel(ctx)); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("shut down meter provider: %w", err))
	}
	if runErr != nil {
		return fmt.Errorf("run metrics export: %w", runErr)
	}

	return nil
}

// pluginResource builds the resource of the plugin's metrics from the downward-API env.
func pluginResource(getenv func(string) string) *resource.Resource {
	namespace, pod := getenv("POD_NAMESPACE"), getenv("POD_NAME")
	instance := pod
	if instance == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		instance = host
	}

	attrs := []attribute.KeyValue{
		semconv.ServiceName(pluginServiceName),
		semconv.ServiceNamespace(namespace),
		semconv.ServiceInstanceID(namespace + "." + instance + "." + pluginServiceName),
	}
	if namespace != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(namespace))
	}
	if pod != "" {
		attrs = append(attrs, semconv.K8SPodName(pod))
	}

	return resource.NewWithAttributes(semconv.SchemaURL, attrs...)
}

// MetricsExporter returns the exporter settings for --metrics-endpoint, a base URL, and
// --metrics-interval. It reports false when the endpoint is empty, and fails with the settings
// message when the result is invalid.
func MetricsExporter(endpoint string, interval time.Duration) (settings.Exporter, bool, error) {
	if endpoint == "" {
		return settings.Exporter{}, false, nil
	}

	s := settings.Defaults()
	s.Cluster = pluginServiceName
	s.Exporter.Endpoint = clusterconfig.SignalURL(endpoint)
	s.Exporter.Interval = interval
	s.Exporter.Timeout = settings.DefaultExporterTimeout(interval)
	if err := s.Validate(); err != nil {
		return settings.Exporter{}, false, fmt.Errorf("invalid metrics export settings: %w", err)
	}

	return s.Exporter, true, nil
}
