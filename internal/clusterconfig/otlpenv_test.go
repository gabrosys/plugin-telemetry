package clusterconfig_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
)

const (
	envEndpoint        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envMetricsEndpoint = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
)

var _ = Describe("OTEL_EXPORTER_OTLP_* keys", func() {
	DescribeTable("builds the http/protobuf URL",
		func(endpoint, want string) {
			in := newInputs()
			in.Exporter.Spec.Exporter.Endpoint = endpoint
			Expect(mustBuild(in).Exporter.Endpoint).To(Equal(want))

			in = withEnv(nil, map[string]string{envEndpoint: endpoint})
			in.Exporter.Spec.Exporter.Endpoint = ""
			Expect(mustBuild(in).Exporter.Endpoint).To(Equal(want))
		},
		Entry("no path", "http://c:4318", "http://c:4318/v1/metrics"),
		Entry("root", "http://c:4318/", "http://c:4318/v1/metrics"),
		Entry("a base path", "http://c:4318/base", "http://c:4318/base/v1/metrics"),
		Entry("a base path with a slash", "http://c:4318/base/", "http://c:4318/base/v1/metrics"),
	)

	It("ignores an EnvSecret when spec.envFrom names no Secret", func() {
		in := withEnv(nil, map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "a=1"})
		in.Exporter.Spec.EnvFrom = &v1alpha1.EnvFromSource{ConfigMap: &v1alpha1.LocalObjectReference{Name: "envcm"}}
		Expect(mustBuild(in).Exporter.HeadersFile).To(BeEmpty())
	})

	It("keeps OTEL_..._METRICS_ENDPOINT as is", func() {
		in := withEnv(map[string]string{envMetricsEndpoint: "http://c:4318/custom/metrics"}, nil)
		in.Exporter.Spec.Exporter.Endpoint = ""
		Expect(mustBuild(in).Exporter.Endpoint).To(Equal("http://c:4318/custom/metrics"))
	})

	It("passes exporter.endpoint to gRPC as given", func() {
		in := newInputs()
		in.Exporter.Spec.Exporter.Protocol = "grpc"
		in.Exporter.Spec.Exporter.Endpoint = "http://c:4317"
		Expect(mustBuild(in).Exporter.Endpoint).To(Equal("http://c:4317"))

		in = withEnv(map[string]string{envEndpoint: "http://c:4317/"}, nil)
		in.Exporter.Spec.Exporter.Protocol = "grpc"
		in.Exporter.Spec.Exporter.Endpoint = ""
		Expect(mustBuild(in).Exporter.Endpoint).To(Equal("http://c:4317/"))
	})

	It("reads OTEL_EXPORTER_OTLP_PROTOCOL for the endpoint rule and the transport", func() {
		in := withEnv(map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc", envEndpoint: "http://c:4317"}, nil)
		in.Exporter.Spec.Exporter.Endpoint = ""
		s := mustBuild(in)
		Expect(s.Exporter.Protocol).To(Equal("grpc"))
		Expect(s.Exporter.Endpoint).To(Equal("http://c:4317"))
	})

	It("lets a METRICS_ key win over the generic one and a Secret key over the ConfigMap key", func() {
		in := withEnv(
			map[string]string{
				envEndpoint:                      "http://cm-generic:4318",
				envMetricsEndpoint:               "http://cm-metrics:4318/m",
				"OTEL_EXPORTER_OTLP_TIMEOUT":     "1000",
				"OTEL_EXPORTER_OTLP_COMPRESSION": "none",
			},
			map[string]string{
				envEndpoint:                              "http://s-generic:4318",
				"OTEL_EXPORTER_OTLP_TIMEOUT":             "2000",
				"OTEL_EXPORTER_OTLP_METRICS_COMPRESSION": "gzip",
				"OTEL_EXPORTER_OTLP_COMPRESSION":         "none",
			},
		)
		in.Exporter.Spec.Exporter.Endpoint = ""
		e := mustBuild(in).Exporter
		Expect(e.Endpoint).To(Equal("http://cm-metrics:4318/m"))
		Expect(e.Timeout).To(Equal(2 * time.Second))
		Expect(e.Compression).To(Equal("gzip"))

		in.EnvConfigMap = map[string]string{envEndpoint: "http://cm-generic:4318"}
		Expect(mustBuild(in).Exporter.Endpoint).To(Equal("http://s-generic:4318/v1/metrics"))
	})

	It("lets explicit exporter fields win over every envFrom key", func() {
		in := withEnv(
			map[string]string{envMetricsEndpoint: "http://cm:4318/x", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"},
			map[string]string{
				"OTEL_EXPORTER_OTLP_METRICS_TIMEOUT":     "1000",
				"OTEL_EXPORTER_OTLP_METRICS_COMPRESSION": "none",
				"OTEL_EXPORTER_OTLP_METRICS_HEADERS":     "a=1",
			},
		)
		in.Exporter.Spec.Exporter.Protocol = "http/protobuf"
		in.Exporter.Spec.Exporter.Compression = "gzip"
		in.Exporter.Spec.Exporter.Timeout = dur(7 * time.Second)
		in.Exporter.Spec.Exporter.Headers = &v1alpha1.KeySelector{Name: "hdr", Key: "h"}
		e := mustBuild(in).Exporter
		Expect(e.Protocol).To(Equal("http/protobuf"))
		Expect(e.Endpoint).To(Equal("http://c:4318/v1/metrics"))
		Expect(e.Compression).To(Equal("gzip"))
		Expect(e.Timeout).To(Equal(7 * time.Second))
		Expect(e.HeadersFile).To(Equal("secret/hdr/h"))
	})

	It("treats an empty value as unset", func() {
		in := withEnv(
			map[string]string{envEndpoint: "http://cm:4318"},
			map[string]string{envEndpoint: "", envMetricsEndpoint: "  ", "OTEL_EXPORTER_OTLP_TIMEOUT": ""},
		)
		in.Exporter.Spec.Exporter.Endpoint = ""
		e := mustBuild(in).Exporter
		Expect(e.Endpoint).To(Equal("http://cm:4318/v1/metrics"))
		Expect(e.Timeout).To(Equal(10 * time.Second))
	})

	It("reads _TIMEOUT in milliseconds as the per-attempt timeout", func() {
		in := withEnv(nil, map[string]string{"OTEL_EXPORTER_OTLP_TIMEOUT": "5000"})
		Expect(mustBuild(in).Exporter.Timeout).To(Equal(5 * time.Second))
	})

	It("warns and falls back to the default for an unusable _TIMEOUT", func() {
		for _, v := range []string{"abc", "-5", "0"} {
			in := withEnv(nil, map[string]string{"OTEL_EXPORTER_OTLP_TIMEOUT": v})
			s, warnings, err := clusterconfig.Build(in)
			Expect(err).NotTo(HaveOccurred())
			Expect(s.Exporter.Timeout).To(Equal(10 * time.Second))
			Expect(warnings).To(ConsistOf(ContainSubstring("OTEL_EXPORTER_OTLP_TIMEOUT")))
		}
	})

	DescribeTable("rejects, naming the key,",
		func(key, value string) {
			for _, in := range []clusterconfig.Inputs{
				withEnv(map[string]string{key: value}, nil),
				withEnv(nil, map[string]string{key: value}),
			} {
				_, _, err := clusterconfig.Build(in)
				Expect(err).To(MatchError(ContainSubstring(key)))
				Expect(err.Error()).NotTo(ContainSubstring(value))
			}
		},
		Entry("CERTIFICATE", "OTEL_EXPORTER_OTLP_CERTIFICATE", "/secret-value-xyz"),
		Entry("METRICS_CERTIFICATE", "OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE", "/secret-value-xyz"),
		Entry("CLIENT_KEY", "OTEL_EXPORTER_OTLP_CLIENT_KEY", "/secret-value-xyz"),
		Entry("METRICS_CLIENT_KEY", "OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY", "/secret-value-xyz"),
		Entry("CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "/secret-value-xyz"),
		Entry("METRICS_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE", "/secret-value-xyz"),
		Entry("http/json", "OTEL_EXPORTER_OTLP_PROTOCOL", "http/json"),
		Entry("METRICS_PROTOCOL http/json", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/json"),
		Entry("delta temporality", "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta"),
		Entry("lowmemory temporality", "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "LowMemory"),
		Entry("exponential histograms", "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION", "base2_exponential_bucket_histogram"),
	)

	It("accepts cumulative temporality and explicit buckets in any case", func() {
		in := withEnv(map[string]string{
			"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE":        "Cumulative",
			"OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION": "EXPLICIT_BUCKET_HISTOGRAM",
			"OTEL_EXPORTER_OTLP_PROTOCOL":                              "HTTP/Protobuf",
		}, nil)
		s, _, err := clusterconfig.Build(in)
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Exporter.Protocol).To(Equal("http/protobuf"))
	})

	It("reads headers from the Secret as a virtual file, METRICS_HEADERS over HEADERS", func() {
		in := withEnv(nil, map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "a=1"})
		Expect(mustBuild(in).Exporter.HeadersFile).To(Equal("secret/envsecret/OTEL_EXPORTER_OTLP_HEADERS"))

		in.EnvSecret["OTEL_EXPORTER_OTLP_METRICS_HEADERS"] = "b=2"
		Expect(mustBuild(in).Exporter.HeadersFile).To(Equal("secret/envsecret/OTEL_EXPORTER_OTLP_METRICS_HEADERS"))
	})

	It("rejects OTEL_EXPORTER_OTLP_HEADERS in a ConfigMap", func() {
		for _, key := range []string{"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_METRICS_HEADERS"} {
			_, _, err := clusterconfig.Build(withEnv(map[string]string{key: "a=secret-value-xyz"}, nil))
			Expect(err).To(MatchError(ContainSubstring(key)))
			Expect(err.Error()).NotTo(ContainSubstring("secret-value-xyz"))
		}
	})

	It("ignores _INSECURE and OTEL_SERVICE_NAME with a warning, and every other key silently", func() {
		in := withEnv(
			map[string]string{"OTEL_SERVICE_NAME": "x", "OTEL_METRIC_EXPORT_INTERVAL": "1", "OTHER": "y"},
			map[string]string{"OTEL_EXPORTER_OTLP_INSECURE": "true", "OTEL_EXPORTER_OTLP_METRICS_INSECURE": "true", "PASSWORD": "z"},
		)
		_, warnings, err := clusterconfig.Build(in)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ConsistOf(
			ContainSubstring("OTEL_SERVICE_NAME"),
			ContainSubstring("OTEL_EXPORTER_OTLP_INSECURE"),
			ContainSubstring("OTEL_EXPORTER_OTLP_METRICS_INSECURE"),
		))
	})

	It("rejects a _TIMEOUT above half the interval without echoing it", func() {
		in := withEnv(nil, map[string]string{"OTEL_EXPORTER_OTLP_TIMEOUT": "20000"})
		_, _, err := clusterconfig.Build(in)
		Expect(err).To(MatchError(And(ContainSubstring("OTEL_EXPORTER_OTLP_TIMEOUT"), ContainSubstring("15s"))))
		Expect(err.Error()).NotTo(ContainSubstring("20s"))
		Expect(err.Error()).NotTo(ContainSubstring("20000"))
	})

	It("never puts a value read from the Secret in an error or warning", func() {
		const secret = "secret-value-xyz"
		cases := []map[string]string{
			{envEndpoint: "http://user:" + secret + "@c:4318"},
			{envEndpoint: "ftp://" + secret},
			{"OTEL_EXPORTER_OTLP_TIMEOUT": secret},
			{"OTEL_EXPORTER_OTLP_COMPRESSION": secret},
			{"OTEL_EXPORTER_OTLP_PROTOCOL": secret},
			{"OTEL_EXPORTER_OTLP_INSECURE": secret},
			{"OTEL_EXPORTER_OTLP_CERTIFICATE": secret},
			{"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE": secret},
			{"OTEL_RESOURCE_ATTRIBUTES": secret},
			{"OTEL_SERVICE_NAME": secret},
			{"OTEL_EXPORTER_OTLP_TIMEOUT": "20000"},
			{"OTEL_EXPORTER_OTLP_TIMEOUT": "99999999999999999"},
		}
		for _, data := range cases {
			in := withEnv(nil, data)
			if _, ok := data[envEndpoint]; ok {
				in.Exporter.Spec.Exporter.Endpoint = ""
			}
			_, warnings, err := clusterconfig.Build(in)
			messages := append([]string{}, warnings...)
			if err != nil {
				messages = append(messages, err.Error())
			}
			for _, m := range messages {
				Expect(m).NotTo(ContainSubstring(secret))
			}
		}
	})
})
