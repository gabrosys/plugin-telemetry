package clusterconfig

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

const (
	envPrefix        = "OTEL_EXPORTER_OTLP_"
	envMetricsPrefix = "OTEL_EXPORTER_OTLP_METRICS_"
	envServiceName   = "OTEL_SERVICE_NAME"
	envResourceAttrs = "OTEL_RESOURCE_ATTRIBUTES"

	metricsPath = "v1/metrics"
)

// otlpEnv reads the OTEL_EXPORTER_OTLP_* keys of the envFrom ConfigMap and Secret.
// Values are never put in messages: a Secret may hold them.
type otlpEnv struct {
	configMap  map[string]string
	secret     map[string]string
	secretName string
}

type envValue struct {
	value      string
	key        string
	fromSecret bool
}

// get returns the value of one key: the Secret over the ConfigMap, an empty value being unset.
func (o otlpEnv) get(key string) (envValue, bool) {
	if v := strings.TrimSpace(o.secret[key]); v != "" {
		return envValue{value: v, key: key, fromSecret: true}, true
	}
	if v := strings.TrimSpace(o.configMap[key]); v != "" {
		return envValue{value: v, key: key}, true
	}
	return envValue{}, false
}

// signal returns the value of OTEL_EXPORTER_OTLP_METRICS_<name>, else OTEL_EXPORTER_OTLP_<name>.
func (o otlpEnv) signal(name string) (envValue, bool) {
	if v, ok := o.get(envMetricsPrefix + name); ok {
		return v, true
	}
	return o.get(envPrefix + name)
}

// unsupported returns an error for each key the sidecar cannot honour, naming the key.
func (o otlpEnv) unsupported() []error {
	var errs []error
	for _, name := range []string{"CERTIFICATE", "CLIENT_KEY", "CLIENT_CERTIFICATE"} {
		for _, key := range []string{envPrefix + name, envMetricsPrefix + name} {
			if _, ok := o.get(key); ok {
				errs = append(errs, fmt.Errorf("%s: not supported, set exporter.tls instead", key))
			}
		}
	}
	for _, key := range []string{envPrefix + "HEADERS", envMetricsPrefix + "HEADERS"} {
		if v := strings.TrimSpace(o.configMap[key]); v != "" {
			errs = append(errs, fmt.Errorf("%s: headers are credentials and must be in the Secret, not the ConfigMap", key))
		}
	}
	if v, ok := o.get(envMetricsPrefix + "TEMPORALITY_PREFERENCE"); ok && !strings.EqualFold(v.value, "cumulative") {
		errs = append(errs, fmt.Errorf("%s: only cumulative is supported", v.key))
	}
	if v, ok := o.get(envMetricsPrefix + "DEFAULT_HISTOGRAM_AGGREGATION"); ok && !strings.EqualFold(v.value, "explicit_bucket_histogram") {
		errs = append(errs, fmt.Errorf("%s: only explicit_bucket_histogram is supported", v.key))
	}
	return errs
}

// ignored returns a warning for each key that is read but has no effect.
func (o otlpEnv) ignored() []string {
	var out []string
	for _, key := range []string{envPrefix + "INSECURE", envMetricsPrefix + "INSECURE"} {
		if _, ok := o.get(key); ok {
			out = append(out, key+": ignored, the endpoint scheme selects TLS")
		}
	}
	if _, ok := o.get(envServiceName); ok {
		out = append(out, envServiceName+": ignored, service.name is the Cluster name")
	}
	return out
}

// apply sets the protocol, compression, timeout and endpoint that spec leaves unset, from the OTEL_* keys.
func (o otlpEnv) apply(e *settings.Exporter, spec v1alpha1.OTLPExporter) (warnings []string, errs []error) {
	if spec.Protocol == "" {
		if v, ok := o.signal("PROTOCOL"); ok {
			switch p := strings.ToLower(v.value); p {
			case settings.ProtocolHTTP, settings.ProtocolGRPC:
				e.Protocol = p
			case "http/json":
				errs = append(errs, fmt.Errorf("%s: this protocol is not supported", v.key))
			default:
				warnings = append(warnings, v.key+": unknown protocol, ignored")
			}
		}
	}

	if spec.Compression == "" {
		if v, ok := o.signal("COMPRESSION"); ok {
			switch c := strings.ToLower(v.value); c {
			case "gzip", "none":
				e.Compression = c
			default:
				warnings = append(warnings, v.key+": unknown compression, ignored")
			}
		}
	}

	if spec.Timeout == nil {
		if v, ok := o.signal("TIMEOUT"); ok {
			ms, err := strconv.ParseInt(v.value, 10, 64)
			switch {
			case err != nil || ms <= 0 || ms > math.MaxInt64/int64(time.Millisecond):
				warnings = append(warnings, v.key+": not a positive number of milliseconds, ignored")
			default:
				timeout := time.Duration(ms) * time.Millisecond
				if err := settings.CheckExporterTimeout(timeout, e.Interval); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", v.key, err))
				} else {
					e.Timeout = timeout
				}
			}
		}
	}

	if spec.Endpoint == "" {
		endpoint, err := o.endpoint(e.Protocol)
		if err != nil {
			errs = append(errs, err)
		} else if endpoint != "" {
			e.Endpoint = endpoint
		}
	}

	return warnings, errs
}

// endpoint resolves OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, used as is, else OTEL_EXPORTER_OTLP_ENDPOINT,
// which gets the signal path for http/protobuf (spec v1.62.0 protocol/exporter.md:108-171).
func (o otlpEnv) endpoint(protocol string) (string, error) {
	if v, ok := o.get(envMetricsPrefix + "ENDPOINT"); ok {
		if err := settings.CheckEndpoint(v.value); err != nil {
			return "", fmt.Errorf("%s: %w", v.key, err)
		}
		return v.value, nil
	}
	if v, ok := o.get(envPrefix + "ENDPOINT"); ok {
		if err := settings.CheckEndpoint(v.value); err != nil {
			return "", fmt.Errorf("%s: %w", v.key, err)
		}
		if protocol == settings.ProtocolHTTP {
			return signalURL(v.value), nil
		}
		return v.value, nil
	}
	return "", nil
}

// headersFile names the Secret key holding the headers: METRICS_HEADERS over HEADERS.
func (o otlpEnv) headersFile() string {
	for _, key := range []string{envMetricsPrefix + "HEADERS", envPrefix + "HEADERS"} {
		if strings.TrimSpace(o.secret[key]) != "" {
			return SecretFile(o.secretName, key)
		}
	}
	return ""
}

// signalURL appends the metrics path to a base URL, ensuring a trailing slash first.
func signalURL(base string) string {
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return base + metricsPath
}
