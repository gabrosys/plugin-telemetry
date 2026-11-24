package clusterconfig_test

import (
	"errors"
	"regexp"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

func mustBuild(in clusterconfig.Inputs) settings.Settings {
	GinkgoHelper()
	s, _, err := clusterconfig.Build(in)
	Expect(err).NotTo(HaveOccurred())
	return s
}

func dur(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

var _ = Describe("virtual file names", func() {
	It("builds them", func() {
		Expect(clusterconfig.ConfigMapFile("n", "k")).To(Equal("configmap/n/k"))
		Expect(clusterconfig.SecretFile("n", "k")).To(Equal("secret/n/k"))
		Expect(clusterconfig.QuerySetFile("n")).To(Equal("metricqueryset/n"))
	})
})

var _ = Describe("Build", func() {
	It("maps a minimal exporter onto settings.Defaults with the Cluster name, namespace and app database", func() {
		in := newInputs()
		in.Cluster.Spec.Bootstrap = &apiv1.BootstrapConfiguration{InitDB: &apiv1.BootstrapInitDB{Database: "app"}}

		s, warnings, err := clusterconfig.Build(in)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(BeEmpty())

		want := settings.Defaults()
		want.Cluster, want.Namespace = "pg", "ns"
		want.Postgres.Database = "app"
		want.Exporter.Endpoint = "http://c:4318/v1/metrics"
		want.Exporter.Timeout = 10 * time.Second
		Expect(s).To(Equal(want))
	})

	DescribeTable("picks the application database like CloudNativePG",
		func(b *apiv1.BootstrapConfiguration, want string) {
			in := newInputs()
			in.Cluster.Spec.Bootstrap = b
			Expect(mustBuild(in).Postgres.Database).To(Equal(want))
		},
		Entry("no bootstrap", nil, "postgres"),
		Entry("no database anywhere", &apiv1.BootstrapConfiguration{InitDB: &apiv1.BootstrapInitDB{}}, "postgres"),
		Entry("initdb", &apiv1.BootstrapConfiguration{InitDB: &apiv1.BootstrapInitDB{Database: "i"}}, "i"),
		Entry("pg_basebackup over initdb", &apiv1.BootstrapConfiguration{
			InitDB:       &apiv1.BootstrapInitDB{Database: "i"},
			PgBaseBackup: &apiv1.BootstrapPgBaseBackup{Database: "b"},
		}, "b"),
		Entry("recovery over the others", &apiv1.BootstrapConfiguration{
			InitDB:       &apiv1.BootstrapInitDB{Database: "i"},
			PgBaseBackup: &apiv1.BootstrapPgBaseBackup{Database: "b"},
			Recovery:     &apiv1.BootstrapRecovery{Database: "r"},
		}, "r"),
	)

	It("uses the default target database postgres when the Cluster has no application database", func() {
		Expect(mustBuild(newInputs()).Postgres.Database).To(Equal("postgres"))
	})

	It("connects peer mode to /controller/run as cnpg_metrics_exporter with no password", func() {
		s := mustBuild(newInputs())
		Expect(s.Postgres.Host).To(Equal("/controller/run"))
		Expect(s.Postgres.Port).To(Equal(uint16(5432)))
		Expect(s.Postgres.User).To(Equal("cnpg_metrics_exporter"))
		Expect(s.Postgres.PasswordFile).To(BeEmpty())
	})

	It("connects password mode to 127.0.0.1:5432 as the Secret's username with PasswordFile secret/<name>/password", func() {
		in := newInputs()
		in.Params = podpatch.Params{Exporter: "default", Auth: podpatch.AuthPassword, PasswordSecret: "pw"}
		in.PasswordUsername = "metrics_user"
		s := mustBuild(in)
		Expect(s.Postgres.Host).To(Equal("127.0.0.1"))
		Expect(s.Postgres.Port).To(Equal(uint16(5432)))
		Expect(s.Postgres.User).To(Equal("metrics_user"))
		Expect(s.Postgres.PasswordFile).To(Equal("secret/pw/password"))
	})

	It("fails in password mode when the username is empty, naming the Secret and the key", func() {
		in := newInputs()
		in.Params = podpatch.Params{Exporter: "default", Auth: podpatch.AuthPassword, PasswordSecret: "pw"}
		_, _, err := clusterconfig.Build(in)
		Expect(err).To(MatchError(And(ContainSubstring(`"pw"`), ContainSubstring(`"username"`))))
	})

	It("fails in password mode without a passwordSecret parameter", func() {
		in := newInputs()
		in.Params = podpatch.Params{Exporter: "default", Auth: podpatch.AuthPassword}
		in.PasswordUsername = "u"
		_, _, err := clusterconfig.Build(in)
		Expect(err).To(MatchError(ContainSubstring("passwordSecret")))
	})

	It("takes metrics.defaultInterval, else spec.monitoring.metricsQueriesTTL, else 30s", func() {
		Expect(mustBuild(newInputs()).DefaultInterval).To(Equal(30 * time.Second))

		in := newInputs()
		in.Cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{MetricsQueriesTTL: dur(45 * time.Second)}
		Expect(mustBuild(in).DefaultInterval).To(Equal(45 * time.Second))

		in.Exporter.Spec.Metrics.DefaultInterval = dur(20 * time.Second)
		Expect(mustBuild(in).DefaultInterval).To(Equal(20 * time.Second))
	})

	DescribeTable("keeps 30s and warns when metricsQueriesTTL is under 1s",
		func(ttl time.Duration) {
			in := newInputs()
			in.Cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{MetricsQueriesTTL: dur(ttl)}
			in.Exporter.Spec.Metrics.Groups = []v1alpha1.QueryGroup{{Name: "g", Queries: []string{"q"}}}
			s, warnings, err := clusterconfig.Build(in)
			Expect(err).NotTo(HaveOccurred())
			Expect(s.DefaultInterval).To(Equal(30 * time.Second))
			Expect(s.Groups[0].Interval).To(Equal(30 * time.Second))
			Expect(warnings).To(ConsistOf(ContainSubstring("spec.monitoring.metricsQueriesTTL")))
		},
		Entry("zero, which disables CNPG's cache", time.Duration(0)),
		Entry("500ms", 500*time.Millisecond),
	)

	It("orders sources as CNPG does: ConfigMaps, then Secrets, then querySets, all after the built-in queries", func() {
		in := newInputs()
		in.Cluster = monitored()
		in.Exporter.Spec.Metrics.QuerySets = []string{"qs"}
		Expect(mustBuild(in).Sources).To(Equal([]string{
			"configmap/cnpg-default-monitoring/queries",
			"configmap/mine/q.yaml",
			"secret/sq/q.yaml",
			"metricqueryset/qs",
		}))
	})

	It("leaves out spec.monitoring sources when useClusterMonitoring is false", func() {
		in := newInputs()
		in.Cluster = monitored()
		in.Exporter.Spec.Metrics.UseClusterMonitoring = ptr.To(false)
		in.Exporter.Spec.Metrics.QuerySets = []string{"qs"}
		Expect(mustBuild(in).Sources).To(Equal([]string{"metricqueryset/qs"}))
	})

	It("maps groups, limits, exclude and planInterval, keeping Go defaults for unset fields", func() {
		in := newInputs()
		m := &in.Exporter.Spec.Metrics
		m.Interval = dur(time.Minute)
		m.MaxPointsPerRequest = ptr.To(int32(500))
		m.PlanInterval = dur(time.Minute)
		m.DefaultInterval = dur(15 * time.Second)
		m.Groups = []v1alpha1.QueryGroup{
			{Name: "fast", Interval: dur(5 * time.Second), Queries: []string{"pg_stat_*"}},
			{Name: "inherit", Queries: []string{"x"}},
		}
		m.Limits = v1alpha1.Limits{
			MaxConnections:      ptr.To(int32(2)),
			MaxRowsPerQuery:     ptr.To(int32(11)),
			MaxSeriesPerQuery:   ptr.To(int32(12)),
			MaxSeriesTotal:      ptr.To(int32(13)),
			MaxDatabases:        ptr.To(int32(14)),
			MaxLabelValueLength: ptr.To(int32(15)),
			MaxLabelsPerSeries:  ptr.To(int32(16)),
			QueryTimeout:        dur(3 * time.Second),
			LockTimeout:         dur(2 * time.Second),
		}
		m.Exclude = v1alpha1.Exclude{Queries: []string{"q"}, Metrics: []string{"m*"}, Databases: []string{"template*"}}

		s := mustBuild(in)
		Expect(s.Exporter.Interval).To(Equal(time.Minute))
		Expect(s.Exporter.MaxPointsPerRequest).To(Equal(500))
		Expect(s.PlanInterval).To(Equal(time.Minute))
		Expect(s.DefaultInterval).To(Equal(15 * time.Second))
		Expect(s.Groups).To(Equal([]settings.Group{
			{Name: "fast", Interval: 5 * time.Second, Queries: []string{"pg_stat_*"}},
			{Name: "inherit", Interval: 15 * time.Second, Queries: []string{"x"}},
		}))
		Expect(s.Postgres.MaxConnections).To(Equal(2))
		Expect(s.Limits).To(Equal(settings.Limits{
			QueryTimeout: 3 * time.Second, LockTimeout: 2 * time.Second,
			MaxRowsPerQuery: 11, MaxSeriesPerQuery: 12, MaxSeriesTotal: 13, MaxDatabases: 14,
			MaxLabelValueLength: 15, MaxLabelsPerSeries: 16,
		}))
		Expect(s.Exclude).To(Equal(settings.Exclude{Queries: []string{"q"}, Metrics: []string{"m*"}, Databases: []string{"template*"}}))

		def := settings.Defaults()
		Expect(mustBuild(newInputs()).Limits).To(Equal(def.Limits))
		Expect(mustBuild(newInputs()).Postgres.MaxConnections).To(Equal(def.Postgres.MaxConnections))
	})

	It("maps the exporter fields", func() {
		in := newInputs()
		in.Exporter.Spec.Exporter = v1alpha1.OTLPExporter{
			Protocol:    "grpc",
			Endpoint:    "https://c:4317",
			Compression: "none",
			Timeout:     dur(4 * time.Second),
			Headers:     &v1alpha1.KeySelector{Name: "hdr", Key: "h"},
			TLS: &v1alpha1.OTLPTLS{
				CA:                &v1alpha1.KeyReference{Secret: &v1alpha1.KeySelector{Name: "ca", Key: "ca.crt"}},
				ClientCertificate: &v1alpha1.LocalObjectReference{Name: "client"},
				ServerName:        "collector",
				MinVersion:        "1.3",
			},
		}
		e := mustBuild(in).Exporter
		Expect(e.Protocol).To(Equal("grpc"))
		Expect(e.Endpoint).To(Equal("https://c:4317"))
		Expect(e.Compression).To(Equal("none"))
		Expect(e.Timeout).To(Equal(4 * time.Second))
		Expect(e.HeadersFile).To(Equal("secret/hdr/h"))
		Expect(e.TLS).To(Equal(settings.TLS{
			CAFile: "secret/ca/ca.crt", CertFile: "secret/client/tls.crt", KeyFile: "secret/client/tls.key",
			ServerName: "collector", MinVersion: "1.3",
		}))

		in.Exporter.Spec.Exporter.TLS.CA = &v1alpha1.KeyReference{ConfigMap: &v1alpha1.KeySelector{Name: "cacm", Key: "bundle"}}
		Expect(mustBuild(in).Exporter.TLS.CAFile).To(Equal("configmap/cacm/bundle"))
	})

	It("defaults exporter.timeout to min(10s, interval/2) when unset", func() {
		in := newInputs()
		in.Exporter.Spec.Metrics.Interval = dur(10 * time.Second)
		Expect(mustBuild(in).Exporter.Timeout).To(Equal(5 * time.Second))
		in.Exporter.Spec.Metrics.Interval = dur(time.Minute)
		Expect(mustBuild(in).Exporter.Timeout).To(Equal(10 * time.Second))
	})

	It("adds allowlisted Cluster labels as cnpg.cluster.label.<key>; missing labels add nothing", func() {
		in := newInputs()
		in.Cluster.Labels = map[string]string{"app.kubernetes.io/part-of": "shop", "other": "x"}
		in.Exporter.Spec.ResourceLabels = []string{"app.kubernetes.io/part-of", "absent"}
		Expect(mustBuild(in).ResourceAttributes).To(Equal(map[string]string{"cnpg.cluster.label.app.kubernetes.io/part-of": "shop"}))

		in.Cluster.Labels = nil
		Expect(mustBuild(in).ResourceAttributes).To(BeEmpty())
	})

	It("never copies the instance role label", func() {
		in := newInputs()
		in.Cluster.Labels = map[string]string{"cnpg.io/instanceRole": "primary"}
		in.Exporter.Spec.ResourceLabels = []string{"cnpg.io/instanceRole"}
		Expect(mustBuild(in).ResourceAttributes).To(BeEmpty())
	})

	It("lets label attributes win over resourceAttributes, and resourceAttributes over OTEL_RESOURCE_ATTRIBUTES", func() {
		in := withEnv(
			map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "env=cm,a=cm,cnpg.cluster.label.l=cm"},
			map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "env=secret,a=secret,s=1"},
		)
		in.Cluster.Labels = map[string]string{"l": "fromlabel"}
		in.Exporter.Spec.ResourceLabels = []string{"l"}
		in.Exporter.Spec.ResourceAttributes = map[string]string{"a": "spec", "cnpg.cluster.label.l": "spec"}
		Expect(mustBuild(in).ResourceAttributes).To(Equal(map[string]string{
			"env": "secret", "a": "spec", "s": "1", "cnpg.cluster.label.l": "fromlabel",
		}))
	})

	It("warns and drops an invalid OTEL_RESOURCE_ATTRIBUTES without echoing it", func() {
		in := withEnv(nil, map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "a=1,secret-value-xyz"})
		s, warnings, err := clusterconfig.Build(in)
		Expect(err).NotTo(HaveOccurred())
		Expect(s.ResourceAttributes).To(BeEmpty())
		Expect(warnings).To(HaveLen(1))
		Expect(strings.Count(warnings[0], "OTEL_RESOURCE_ATTRIBUTES")).To(Equal(1))
		Expect(warnings[0]).NotTo(ContainSubstring("secret-value-xyz"))
	})

	It("fails with a message naming the field when settings.Validate fails, e.g. no endpoint anywhere", func() {
		in := newInputs()
		in.Exporter.Spec.Exporter.Endpoint = ""
		_, _, err := clusterconfig.Build(in)
		Expect(err).To(MatchError(ContainSubstring("exporter.endpoint")))

		in = newInputs()
		in.Exporter.Spec.Metrics.Interval = dur(2 * time.Second)
		in.Exporter.Spec.Exporter.Timeout = dur(5 * time.Second)
		_, _, err = clusterconfig.Build(in)
		Expect(err).To(MatchError(ContainSubstring("exporter.timeout")))
	})
})

var _ = Describe("Build messages", func() {
	DescribeTable("name the TelemetryExporter field, not the settings path",
		func(edit func(*clusterconfig.Inputs), field, settingsPath string) {
			in := newInputs()
			edit(&in)

			_, _, err := clusterconfig.Build(in)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(field))
			Expect(err.Error()).NotTo(MatchRegexp(`(^|: |\n|\()` + regexp.QuoteMeta(settingsPath)))
		},
		Entry("limits.maxConnections", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Limits.MaxConnections = ptr.To[int32](0)
		}, "spec.metrics.limits.maxConnections: must be at least 1", "postgres.maxConnections"),
		Entry("limits.maxRowsPerQuery", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Limits.MaxRowsPerQuery = ptr.To[int32](0)
		}, "spec.metrics.limits.maxRowsPerQuery: must be at least 1", "limits.maxRowsPerQuery"),
		Entry("limits.queryTimeout", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Limits.QueryTimeout = dur(0)
		}, "spec.metrics.limits.queryTimeout: must be at least", "limits.queryTimeout"),
		Entry("planInterval", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.PlanInterval = dur(0)
		}, "spec.metrics.planInterval: must be at least 1s", "planInterval"),
		Entry("defaultInterval", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.DefaultInterval = dur(0)
		}, "spec.metrics.defaultInterval: must be at least 1s", "defaultInterval"),
		Entry("groups[i].name", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Groups = []v1alpha1.QueryGroup{{Queries: []string{"q"}}}
		}, "spec.metrics.groups[0].name: must not be empty", "groups[0]"),
		Entry("groups[i].queries[j]", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Groups = []v1alpha1.QueryGroup{{Name: "g", Queries: []string{"["}}}
		}, "spec.metrics.groups[0].queries[0]: invalid pattern", "groups[0]"),
		Entry("exclude.queries", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Exclude.Queries = []string{"["}
		}, "spec.metrics.exclude.queries[0]: invalid pattern", "exclude.queries"),
		Entry("exclude.metrics", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Exclude.Metrics = []string{"["}
		}, "spec.metrics.exclude.metrics[0]: invalid pattern", "exclude.metrics"),
		Entry("exclude.databases", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Exclude.Databases = []string{"["}
		}, "spec.metrics.exclude.databases[0]: invalid pattern", "exclude.databases"),
		Entry("metrics.interval", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Interval = dur(0)
		}, "spec.metrics.interval: must be at least 1s", "exporter.interval"),
		Entry("metrics.maxPointsPerRequest", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.MaxPointsPerRequest = ptr.To[int32](0)
		}, "spec.metrics.maxPointsPerRequest: must be at least 1", "exporter.maxPointsPerRequest"),
		Entry("exporter.protocol", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Exporter.Protocol = "http/json"
		}, "spec.exporter.protocol: must be", "exporter.protocol"),
		Entry("exporter.compression", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Exporter.Compression = "zstd"
		}, "spec.exporter.compression: must be", "exporter.compression"),
		Entry("exporter.timeout", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Metrics.Interval = dur(2 * time.Second)
			in.Exporter.Spec.Exporter.Timeout = dur(5 * time.Second)
		}, "spec.exporter.timeout: must be above 0", "exporter.timeout"),
		Entry("exporter.tls.minVersion", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Exporter.Endpoint = "https://c:4318"
			in.Exporter.Spec.Exporter.TLS = &v1alpha1.OTLPTLS{MinVersion: "1.1"}
		}, "spec.exporter.tls.minVersion: must be", "exporter.tls.minVersion"),
		Entry("exporter.tls with http", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Exporter.TLS = &v1alpha1.OTLPTLS{ServerName: "c"}
		}, "spec.exporter.tls: cannot be set with an http:// endpoint", "exporter.tls"),
		Entry("exporter.endpoint empty", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Exporter.Endpoint = ""
		}, "spec.exporter.endpoint: must not be empty", "exporter.endpoint"),
		Entry("exporter.endpoint with user info, and where headers go", func(in *clusterconfig.Inputs) {
			in.Exporter.Spec.Exporter.Endpoint = "http://u:p@c:4318"
		}, "spec.exporter.endpoint: must not have user info, a query or a fragment (headers go in spec.exporter.headers)",
			"exporter.headersFile"),
		Entry("an OTEL_* TLS key", func(in *clusterconfig.Inputs) {
			*in = withEnv(map[string]string{"OTEL_EXPORTER_OTLP_CERTIFICATE": "/ca.pem"}, nil)
		}, "OTEL_EXPORTER_OTLP_CERTIFICATE: not supported, set spec.exporter.tls instead", "exporter.tls"),
	)

	It("names spec.resourceLabels in the instance role warning", func() {
		in := newInputs()
		in.Exporter.Spec.ResourceLabels = []string{"cnpg.io/instanceRole"}

		_, warnings, err := clusterconfig.Build(in)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ConsistOf(HavePrefix("spec.resourceLabels: cnpg.io/instanceRole is ignored")))
	})

	It("keeps the settings error in the chain", func() {
		in := newInputs()
		in.Exporter.Spec.Exporter.Endpoint = "ftp://c"

		_, _, err := clusterconfig.Build(in)
		Expect(errors.Is(err, settings.CheckEndpoint("ftp://c"))).To(BeTrue())
	})
})
