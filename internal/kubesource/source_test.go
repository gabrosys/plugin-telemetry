package kubesource_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io/fs"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/kubesource"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

var _ = Describe("Source", func() {
	var (
		ctx   context.Context
		fw    *fakeWatcher
		clk   *clocktesting.FakeClock
		src   *kubesource.Source
		logs  *bytes.Buffer
		start func()
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		logs = &bytes.Buffer{}
		logger := funcr.New(func(prefix, args string) { logs.WriteString(prefix + args + "\n") }, funcr.Options{Verbosity: 10})
		ctx, cancel = context.WithCancel(logr.NewContext(context.Background(), logger))
		DeferCleanup(cancel)

		fw = newFakeWatcher()
		clk = clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
		var err error
		src, err = kubesource.New(kubesource.Options{
			Namespace: "ns", Cluster: "pg", APIServer: "https://127.0.0.1:1", TokenDir: GinkgoT().TempDir(), Clock: clk,
		})
		Expect(err).NotTo(HaveOccurred())
		start = func() { src.Begin(ctx, fw.watch) }
	})

	// ready feeds the Cluster and the default exporter.
	ready := func() {
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", exporter(nil))
	}

	It("is named kubernetes and fails to build on empty options", func() {
		Expect(src.Name()).To(Equal("kubernetes"))
		_, err := kubesource.New(kubesource.Options{})
		Expect(err).To(HaveOccurred())
	})

	It("returns ErrNoConfiguration until the Cluster is known, then while the exporter is missing", func() {
		start()
		_, _, err := src.Settings()
		Expect(err).To(MatchError(engine.ErrNoConfiguration))

		fw.set("clusters", "pg", nil)
		_, _, err = src.Settings()
		Expect(err).To(MatchError(engine.ErrNoConfiguration))

		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", nil)
		_, _, err = src.Settings()
		Expect(err).To(MatchError(engine.ErrNoConfiguration))
		Expect(err).To(MatchError(ContainSubstring("default")))
	})

	DescribeTable("returns ErrNoConfiguration when the plugin entry is absent or disabled, or auth is invalid",
		func(plugins []any) {
			start()
			cl := cluster(clusterOpts{plugins: plugins})
			fw.set("clusters", "pg", cl)
			_, _, err := src.Settings()
			Expect(err).To(MatchError(engine.ErrNoConfiguration))
		},
		Entry("absent", []any{map[string]any{"name": "other"}}),
		Entry("disabled", []any{pluginEntry(map[string]any{}, map[string]any{"enabled": false})}),
		Entry("invalid auth", []any{pluginEntry(map[string]any{"auth": "kerberos"}, nil)}),
	)

	It("does not watch the exporter while the plugin is disabled", func() {
		start()
		fw.set("clusters", "pg", cluster(clusterOpts{plugins: []any{pluginEntry(map[string]any{}, map[string]any{"enabled": false})}}))
		Expect(fw.active()).To(ConsistOf("clusters/pg"))
	})

	It("builds settings once the Cluster and the exporter are known", func() {
		ready()
		s, data, err := src.Settings()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Cluster).To(Equal("pg"))
		Expect(s.Namespace).To(Equal("ns"))
		Expect(s.Exporter.Endpoint).To(HavePrefix("http://c:4318"))
		Expect(data).NotTo(BeEmpty())
	})

	It("starts watches for exactly References and stops those no longer referenced", func() {
		start()
		Expect(fw.active()).To(ConsistOf("clusters/pg"))

		fw.set("clusters", "pg", cluster(clusterOpts{
			plugins: []any{pluginEntry(map[string]any{"auth": "password", "passwordSecret": "pw", "exporter": "ex"}, nil)},
			monitoring: map[string]any{
				"customQueriesConfigMap": []any{map[string]any{"name": "mon-cm", "key": "q.yaml"}},
				"customQueriesSecret":    []any{map[string]any{"name": "mon-sec", "key": "q.yaml"}},
			},
		}))
		Expect(fw.active()).To(ConsistOf("clusters/pg", "telemetryexporters/ex", "secrets/pw", "configmaps/mon-cm", "secrets/mon-sec"))

		fw.set("telemetryexporters", "ex", exporter(map[string]any{
			"exporter": map[string]any{"endpoint": "http://c:4318", "headers": map[string]any{"name": "hdr", "key": "h"}},
			"envFrom": map[string]any{
				"configMap": map[string]any{"name": "envcm"},
				"secret":    map[string]any{"name": "envsecret"},
			},
			"metrics": map[string]any{"querySets": []any{"qs1"}},
		}))
		Expect(fw.active()).To(ConsistOf(
			"clusters/pg", "telemetryexporters/ex", "secrets/pw", "configmaps/mon-cm", "secrets/mon-sec",
			"secrets/hdr", "configmaps/envcm", "secrets/envsecret", "metricquerysets/qs1",
		))

		fw.set("telemetryexporters", "ex", exporter(nil))
		Expect(fw.active()).To(ConsistOf("clusters/pg", "telemetryexporters/ex", "secrets/pw", "configmaps/mon-cm", "secrets/mon-sec"))

		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		Expect(fw.active()).To(ConsistOf("clusters/pg", "telemetryexporters/default"))
	})

	Describe("ReadFile", func() {
		BeforeEach(func() {
			start()
			fw.set("clusters", "pg", cluster(clusterOpts{
				plugins:    []any{pluginEntry(map[string]any{}, nil)},
				monitoring: map[string]any{"customQueriesConfigMap": []any{map[string]any{"name": "mon-cm", "key": "q.yaml"}}},
			}))
			fw.set("telemetryexporters", "default", exporter(map[string]any{
				"exporter": map[string]any{"endpoint": "http://c:4318", "headers": map[string]any{"name": "hdr", "key": "h"}},
				"metrics":  map[string]any{"querySets": []any{"qs1"}},
			}))
		})

		It("answers a plain error until the object's watch has synced", func() {
			_, err := src.ReadFile(clusterconfig.ConfigMapFile("mon-cm", "q.yaml"))
			Expect(err).To(HaveOccurred())
			Expect(err).NotTo(MatchError(fs.ErrNotExist))
			Expect(err).NotTo(MatchError(sources.ErrDenied))
		})

		It("serves ConfigMap and Secret keys and MetricQuerySet JSON as virtual files", func() {
			fw.set("configmaps", "mon-cm", configMap("mon-cm", map[string]any{"q.yaml": "a: 1\n"}))
			fw.set("secrets", "hdr", secret("hdr", map[string]string{"h": "k=v"}))
			fw.set("metricquerysets", "qs1", querySet("qs1", map[string]any{
				"q": map[string]any{"query": "SELECT 1", "metrics": []any{map[string]any{"v": map[string]any{"usage": "GAUGE", "metric_mapping": map[string]any{}}}}},
			}))

			Expect(src.ReadFile(clusterconfig.ConfigMapFile("mon-cm", "q.yaml"))).To(Equal([]byte("a: 1\n")))
			Expect(src.ReadFile(clusterconfig.SecretFile("hdr", "h"))).To(Equal([]byte("k=v")))
			Expect(src.ReadFile(clusterconfig.QuerySetFile("qs1"))).To(MatchJSON(
				`{"q":{"query":"SELECT 1","metrics":[{"v":{"usage":"GAUGE","metric_mapping":{}}}]}}`))
		})

		It("answers fs.ErrNotExist for an unknown name or a missing key", func() {
			fw.set("configmaps", "mon-cm", configMap("mon-cm", map[string]any{"q.yaml": "a"}))
			for _, name := range []string{
				"other/x", "configmap/unwatched/k", clusterconfig.ConfigMapFile("mon-cm", "missing"), "configmap/mon-cm",
			} {
				_, err := src.ReadFile(name)
				Expect(err).To(MatchError(fs.ErrNotExist), name)
			}
		})

		It("answers fs.ErrNotExist for an object that doesn't exist, then serves it when it appears", func() {
			fw.set("configmaps", "mon-cm", nil)
			_, err := src.ReadFile(clusterconfig.ConfigMapFile("mon-cm", "q.yaml"))
			Expect(err).To(MatchError(fs.ErrNotExist))

			fw.set("configmaps", "mon-cm", configMap("mon-cm", map[string]any{"q.yaml": "a"}))
			Expect(src.ReadFile(clusterconfig.ConfigMapFile("mon-cm", "q.yaml"))).To(Equal([]byte("a")))
		})

		It("answers ErrDenied for an object whose watch got a 403, and recovers when access is granted", func() {
			fw.fail("configmaps", "mon-cm", errors.Join(errors.New("forbidden"), sources.ErrDenied))
			_, err := src.ReadFile(clusterconfig.ConfigMapFile("mon-cm", "q.yaml"))
			Expect(err).To(MatchError(sources.ErrDenied))

			fw.set("configmaps", "mon-cm", configMap("mon-cm", map[string]any{"q.yaml": "a"}))
			Expect(src.ReadFile(clusterconfig.ConfigMapFile("mon-cm", "q.yaml"))).To(Equal([]byte("a")))
		})
	})

	Describe("Settings", func() {
		It("keeps the bytes unchanged when only the Cluster status or an unrelated annotation changes", func() {
			ready()
			_, before, err := src.Settings()
			Expect(err).NotTo(HaveOccurred())

			fw.set("clusters", "pg", cluster(clusterOpts{
				plugins:     []any{pluginEntry(map[string]any{}, nil)},
				status:      map[string]any{"phase": "Cluster in healthy state", "readyInstances": int64(3)},
				annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "x"},
			}))
			_, after, err := src.Settings()
			Expect(err).NotTo(HaveOccurred())
			Expect(after).To(Equal(before))

			fw.set("clusters", "pg", enabledCluster(map[string]any{"sidecarCPU": "100m"}))
			_, changed, err := src.Settings()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).NotTo(Equal(before))
		})

		It("reads the password username and the envFrom data, and waits for their watches", func() {
			start()
			fw.set("clusters", "pg", enabledCluster(map[string]any{"auth": "password", "passwordSecret": "pw"}))
			fw.set("telemetryexporters", "default", exporter(map[string]any{
				"exporter": map[string]any{"endpoint": "http://c:4318"},
				"envFrom":  map[string]any{"configMap": map[string]any{"name": "envcm"}},
			}))
			_, _, err := src.Settings()
			Expect(err).To(HaveOccurred())
			Expect(err).NotTo(MatchError(engine.ErrNoConfiguration))

			fw.set("secrets", "pw", secret("pw", map[string]string{"username": "monitor", "password": "p"}))
			fw.set("configmaps", "envcm", configMap("envcm", map[string]any{"OTEL_EXPORTER_OTLP_COMPRESSION": "gzip"}))
			s, _, err := src.Settings()
			Expect(err).NotTo(HaveOccurred())
			Expect(s.Postgres.User).To(Equal("monitor"))
			Expect(s.Postgres.PasswordFile).To(Equal(clusterconfig.SecretFile("pw", "password")))
			Expect(s.Exporter.Compression).To(Equal("gzip"))
		})
	})

	Describe("Watch", func() {
		It("notifies once, debounced, after a burst of events", func() {
			var notified atomic.Int32
			readyCh := make(chan struct{})
			done := make(chan error, 1)
			start()
			go func() {
				done <- src.Watch(ctx, nil, func() { notified.Add(1) }, sources.WithReady(func() { close(readyCh) }))
			}()
			Eventually(readyCh).Should(BeClosed())

			fw.set("clusters", "pg", enabledCluster(map[string]any{}))
			fw.set("telemetryexporters", "default", exporter(nil))
			fw.set("clusters", "pg", enabledCluster(map[string]any{"sidecarCPU": "100m"}))
			Eventually(clk.HasWaiters).Should(BeTrue())

			clk.Step(999 * time.Millisecond)
			Consistently(notified.Load, "100ms").Should(BeZero())
			clk.Step(time.Millisecond)
			Eventually(notified.Load).Should(BeEquivalentTo(1))
			Consistently(notified.Load, "100ms").Should(BeEquivalentTo(1))

			fw.set("telemetryexporters", "default", exporter(nil))
			Eventually(clk.HasWaiters).Should(BeTrue())
			clk.Step(time.Second)
			Eventually(notified.Load).Should(BeEquivalentTo(2))
		})

		It("reports watch errors other than a 403 to the error handler and returns nil when ctx ends", func() {
			errs := make(chan error, 4)
			readyCh := make(chan struct{})
			done := make(chan error, 1)
			wctx, cancel := context.WithCancel(ctx)
			start()
			go func() {
				done <- src.Watch(wctx, nil, func() {}, sources.WithReady(func() { close(readyCh) }),
					sources.WithErrorHandler(func(err error) { errs <- err }))
			}()
			Eventually(readyCh).Should(BeClosed())

			fw.fail("clusters", "pg", errors.New("connection refused"))
			Eventually(errs).Should(Receive(MatchError(ContainSubstring("connection refused"))))

			cancel()
			Eventually(done).Should(Receive(BeNil()))
		})
	})

	It("never logs a Secret's data", func() {
		const sensitive = "hunter2-sekret"
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{"auth": "password", "passwordSecret": "pw"}))
		fw.set("telemetryexporters", "default", exporter(map[string]any{
			"exporter": map[string]any{"endpoint": "http://c:4318"},
			"envFrom":  map[string]any{"secret": map[string]any{"name": "envsecret"}},
		}))
		data := map[string]string{"username": "monitor", "password": sensitive}
		fw.set("secrets", "pw", secret("pw", data))
		fw.set("secrets", "envsecret", secret("envsecret", map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "k=" + sensitive}))
		fw.fail("secrets", "pw", errors.Join(errors.New("watch broke"), sources.ErrDenied))
		fw.fail("secrets", "envsecret", errors.New("watch broke"))
		_, _, err := src.Settings()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).NotTo(ContainSubstring(sensitive))

		Expect(logs.Len()).NotTo(BeZero())
		Expect(logs.String()).NotTo(ContainSubstring(sensitive))
		Expect(logs.String()).NotTo(ContainSubstring(base64.StdEncoding.EncodeToString([]byte(sensitive))))
	})

	It("waits for the token file", func() {
		dir := GinkgoT().TempDir()
		s, err := kubesource.New(kubesource.Options{Namespace: "ns", Cluster: "pg", APIServer: "https://x", TokenDir: dir, Clock: clk})
		Expect(err).NotTo(HaveOccurred())
		found := make(chan bool, 1)
		go func() { found <- s.WaitForToken(ctx) }()

		Eventually(clk.HasWaiters).Should(BeTrue())
		clk.Step(5 * time.Second)
		Consistently(found, "50ms").ShouldNot(Receive())

		Expect(writeFile(dir+"/token", "t")).To(Succeed())
		Eventually(clk.HasWaiters).Should(BeTrue())
		clk.Step(5 * time.Second)
		Eventually(found).Should(Receive(BeTrue()))
	})
})
