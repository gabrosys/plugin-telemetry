package kubesource_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
		src = kubesource.New(kubesource.Options{
			Namespace: "ns", Cluster: "pg", APIServer: "https://127.0.0.1:1", TokenDir: GinkgoT().TempDir(), Clock: clk,
		})
		start = func() {
			rctx, stop := context.WithCancel(ctx)
			ran := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(ran)
				src.RunWith(rctx, fw.watch)
			}()
			DeferCleanup(func() {
				stop()
				Eventually(ran).Should(BeClosed())
			})
		}
	})

	// watching waits until exactly the given watches run.
	watching := func(keys ...string) {
		GinkgoHelper()
		Eventually(fw.active).Should(ConsistOf(keys))
	}

	// ready feeds the Cluster and the default exporter.
	ready := func() {
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", exporter(nil))
	}

	It("is named kubernetes", func() {
		Expect(src.Name()).To(Equal("kubernetes"))
	})

	It("stops every watch and returns when ctx ends", func() {
		rctx, stop := context.WithCancel(ctx)
		ran := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(ran)
			src.RunWith(rctx, fw.watch)
		}()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		watching("clusters/pg", "telemetryexporters/default")
		Consistently(ran, "50ms").ShouldNot(BeClosed())

		stop()
		Eventually(ran).Should(BeClosed())
		Expect(fw.active()).To(BeEmpty())
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
		Consistently(fw.active, "100ms").Should(ConsistOf("clusters/pg"))
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
		watching("clusters/pg")

		fw.set("clusters", "pg", cluster(clusterOpts{
			plugins: []any{pluginEntry(map[string]any{"auth": "password", "passwordSecret": "pw", "exporter": "ex"}, nil)},
			monitoring: map[string]any{
				"customQueriesConfigMap": []any{map[string]any{"name": "mon-cm", "key": "q.yaml"}},
				"customQueriesSecret":    []any{map[string]any{"name": "mon-sec", "key": "q.yaml"}},
			},
		}))
		watching("clusters/pg", "telemetryexporters/ex", "secrets/pw", "configmaps/mon-cm", "secrets/mon-sec")

		fw.set("telemetryexporters", "ex", exporter(map[string]any{
			"exporter": map[string]any{"endpoint": "http://c:4318", "headers": map[string]any{"name": "hdr", "key": "h"}},
			"envFrom": map[string]any{
				"configMap": map[string]any{"name": "envcm"},
				"secret":    map[string]any{"name": "envsecret"},
			},
			"metrics": map[string]any{"querySets": []any{"qs1"}},
		}))
		watching(
			"clusters/pg", "telemetryexporters/ex", "secrets/pw", "configmaps/mon-cm", "secrets/mon-sec",
			"secrets/hdr", "configmaps/envcm", "secrets/envsecret", "metricquerysets/qs1",
		)

		fw.set("telemetryexporters", "ex", exporter(nil))
		watching("clusters/pg", "telemetryexporters/ex", "secrets/pw", "configmaps/mon-cm", "secrets/mon-sec")

		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		watching("clusters/pg", "telemetryexporters/default")
	})

	It("keeps watching the objects of the last settings that built while an edit fails Build", func() {
		withHeaders := func(name string, timeout string) *unstructured.Unstructured {
			return exporter(map[string]any{
				"exporter": map[string]any{
					"endpoint": "http://c:4318", "timeout": timeout, "headers": map[string]any{"name": name, "key": "h"},
				},
				"metrics": map[string]any{"interval": "30s"},
			})
		}
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", withHeaders("h1", "10s"))
		fw.set("secrets", "h1", secret("h1", map[string]string{"h": "k=1"}))
		_, _, err := src.Settings()
		Expect(err).NotTo(HaveOccurred())

		fw.set("telemetryexporters", "default", withHeaders("h2", "20s"))
		watching("clusters/pg", "telemetryexporters/default", "secrets/h1", "secrets/h2")
		fw.set("secrets", "h2", secret("h2", map[string]string{"h": "k=2"}))
		_, _, err = src.Settings()
		Expect(err).To(MatchError(ContainSubstring("exporter.timeout")))
		Expect(src.ReadFile(clusterconfig.SecretFile("h1", "h"))).To(Equal([]byte("k=1")))
		Consistently(fw.active, "100ms").Should(ContainElement("secrets/h1"))

		fw.set("telemetryexporters", "default", withHeaders("h2", "10s"))
		s, _, err := src.Settings()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Exporter.HeadersFile).To(Equal(clusterconfig.SecretFile("h2", "h")))
		watching("clusters/pg", "telemetryexporters/default", "secrets/h2")
	})

	DescribeTable("forgets the references of the last settings that built once there is no configuration",
		func(idle *unstructured.Unstructured, missingExporter string, still ...string) {
			start()
			fw.set("clusters", "pg", enabledCluster(map[string]any{}))
			fw.set("telemetryexporters", "default", exporter(map[string]any{
				"exporter": map[string]any{"endpoint": "http://c:4318", "headers": map[string]any{"name": "h1", "key": "h"}},
			}))
			fw.set("secrets", "h1", secret("h1", map[string]string{"h": "k=1"}))
			_, _, err := src.Settings()
			Expect(err).NotTo(HaveOccurred())

			fw.set("clusters", "pg", idle)
			if missingExporter != "" {
				fw.set("telemetryexporters", missingExporter, nil)
			}
			Eventually(func() error { _, _, err := src.Settings(); return err }).Should(MatchError(engine.ErrNoConfiguration))
			watching(still...)
		},
		Entry("the exporter is missing", enabledCluster(map[string]any{"exporter": "other"}), "other",
			"clusters/pg", "telemetryexporters/other"),
		Entry("the plugin is disabled",
			cluster(clusterOpts{plugins: []any{pluginEntry(map[string]any{}, map[string]any{"enabled": false})}}), "",
			"clusters/pg"),
	)

	It("drops reports from a watch that was stopped", func() {
		withQuerySet := exporter(map[string]any{
			"exporter": map[string]any{"endpoint": "http://c:4318"},
			"metrics":  map[string]any{"querySets": []any{"qs1"}},
		})
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", withQuerySet)
		stale := fw.handler("metricquerysets", "qs1")

		fw.set("telemetryexporters", "default", exporter(nil))
		watching("clusters/pg", "telemetryexporters/default")
		fw.set("telemetryexporters", "default", withQuerySet)
		watching("clusters/pg", "telemetryexporters/default", "metricquerysets/qs1")

		var notified atomic.Int32
		go func() {
			defer GinkgoRecover()
			Expect(src.Watch(ctx, nil, func() { notified.Add(1) })).To(Succeed())
		}()
		Eventually(notified.Load).Should(BeEquivalentTo(1))
		stale.Set(querySet("qs1", map[string]any{"q": map[string]any{"query": "SELECT 1"}}))
		stale.Fail(errors.New("connection refused"))

		_, err := src.ReadFile(clusterconfig.QuerySetFile("qs1"))
		Expect(err).To(MatchError(kubesource.ErrNotSynced))
		Expect(logs.String()).NotTo(ContainSubstring("watch failed"))
		Consistently(clk.HasWaiters, "100ms").Should(BeFalse())
	})

	It("goes on reconciling after a watch start panics, and starts that watch again after 5 s", func() {
		fw.panicOnce("configmaps", "mon-cm")
		start()
		fw.set("clusters", "pg", cluster(clusterOpts{
			plugins:    []any{pluginEntry(map[string]any{}, nil)},
			monitoring: map[string]any{"customQueriesConfigMap": []any{map[string]any{"name": "mon-cm", "key": "q.yaml"}}},
		}))

		watching("clusters/pg", "telemetryexporters/default")
		Eventually(clk.HasWaiters).Should(BeTrue())
		Expect(logs.String()).To(ContainSubstring("recovered from a panic"))
		clk.Step(5 * time.Second)
		watching("clusters/pg", "telemetryexporters/default", "configmaps/mon-cm")
	})

	It("still starts the new watches when a stop panics", func() {
		withCM := func(name string) *unstructured.Unstructured {
			return cluster(clusterOpts{
				plugins:    []any{pluginEntry(map[string]any{}, nil)},
				monitoring: map[string]any{"customQueriesConfigMap": []any{map[string]any{"name": name, "key": "q.yaml"}}},
			})
		}
		start()
		fw.set("clusters", "pg", withCM("old-cm"))
		watching("clusters/pg", "telemetryexporters/default", "configmaps/old-cm")

		fw.panicOnStop("configmaps", "old-cm")
		fw.set("clusters", "pg", withCM("new-cm"))
		watching("clusters/pg", "telemetryexporters/default", "configmaps/new-cm")
	})

	It("starts a watch again after 5 s when its informer died", func() {
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		watching("clusters/pg", "telemetryexporters/default")
		Expect(fw.started("telemetryexporters", "default")).To(Equal(1))

		fw.handler("telemetryexporters", "default").Crashed()
		Eventually(clk.HasWaiters).Should(BeTrue())
		clk.Step(5 * time.Second)
		Eventually(func() int { return fw.started("telemetryexporters", "default") }).Should(Equal(2))
	})

	It("keeps handling events after a handler panics", func() {
		var (
			mu    sync.Mutex // guards lines
			lines []string
			once  sync.Once
		)
		logger := funcr.New(func(_, args string) {
			if strings.Contains(args, "watch failed") {
				once.Do(func() { panic("log broke") })
			}
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, args)
		}, funcr.Options{})
		ctx = logr.NewContext(ctx, logger)
		start()

		fw.fail("clusters", "pg", errors.New("first failure"))
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			n := 0
			for _, l := range lines {
				if strings.Contains(l, "recovered from a panic") {
					n++
				}
			}
			return n
		}).Should(Equal(1))

		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", exporter(nil))
		Eventually(func() error { _, _, err := src.Settings(); return err }).Should(Succeed())
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

		It("answers not synced, not missing, for a virtual file with no watch yet", func() {
			for _, name := range []string{
				clusterconfig.ConfigMapFile("unwatched", "k"), clusterconfig.SecretFile("unwatched", "k"), clusterconfig.QuerySetFile("unwatched"),
			} {
				_, err := src.ReadFile(name)
				Expect(err).To(MatchError(kubesource.ErrNotSynced), name)
				Expect(err).NotTo(MatchError(fs.ErrNotExist), name)
			}
		})

		It("answers fs.ErrNotExist for an unknown name or a missing key", func() {
			fw.set("configmaps", "mon-cm", configMap("mon-cm", map[string]any{"q.yaml": "a"}))
			for _, name := range []string{
				"other/x", clusterconfig.ConfigMapFile("mon-cm", "missing"), "configmap/mon-cm",
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
		It("keeps the bytes unchanged when only the Cluster status, an unrelated annotation or a plugin parameter Build ignores changes", func() {
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
			_, after, err = src.Settings()
			Expect(err).NotTo(HaveOccurred())
			Expect(after).To(Equal(before))

			fw.set("telemetryexporters", "default", exporter(map[string]any{
				"exporter": map[string]any{"endpoint": "http://other:4318"},
			}))
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
		It("notifies once watching, then once, debounced, after a burst of events", func() {
			var mu sync.Mutex // guards notified
			var notified []time.Time
			count := func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(notified)
			}
			done := make(chan error, 1)
			start()
			go func() {
				done <- src.Watch(ctx, nil, func() {
					mu.Lock()
					defer mu.Unlock()
					notified = append(notified, clk.Now())
				})
			}()
			Eventually(count).Should(Equal(1))

			burst := clk.Now()
			fw.set("clusters", "pg", enabledCluster(map[string]any{}))
			fw.set("telemetryexporters", "default", exporter(nil))
			fw.set("clusters", "pg", enabledCluster(map[string]any{"sidecarCPU": "100m"}))
			Eventually(clk.HasWaiters).Should(BeTrue())
			// A wake Watch takes late re-arms the timer, so step until the notification.
			Eventually(func() int {
				clk.Step(100 * time.Millisecond)
				return count()
			}).Should(Equal(2))
			Consistently(count, "100ms").Should(Equal(2))
			mu.Lock()
			second := notified[1]
			mu.Unlock()
			Expect(second.Sub(burst)).To(BeNumerically(">=", time.Second))

			fw.set("telemetryexporters", "default", exporter(nil))
			Eventually(clk.HasWaiters).Should(BeTrue())
			clk.Step(999 * time.Millisecond)
			Consistently(count, "100ms").Should(Equal(2))
			clk.Step(time.Millisecond)
			Eventually(count).Should(Equal(3))
		})

		It("returns nil when ctx ends", func() {
			done := make(chan error, 1)
			wctx, cancel := context.WithCancel(ctx)
			start()
			go func() { done <- src.Watch(wctx, nil, func() {}) }()
			cancel()
			Eventually(done).Should(Receive(BeNil()))
		})
	})

	It("logs a repeated watch error once, and again when the error changes or after an event", func() {
		start()
		fw.fail("clusters", "pg", errors.New("connection refused"))
		fw.fail("clusters", "pg", errors.New("connection refused"))
		Expect(strings.Count(logs.String(), "watch failed")).To(Equal(1))

		fw.fail("clusters", "pg", errors.New("no route to host"))
		Expect(strings.Count(logs.String(), "watch failed")).To(Equal(2))

		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.fail("clusters", "pg", errors.New("no route to host"))
		Expect(strings.Count(logs.String(), "watch failed")).To(Equal(3))
	})

	It("logs A, 403, A as two failures and never notifies for a watch error", func() {
		var notified atomic.Int32
		start()
		go func() {
			defer GinkgoRecover()
			Expect(src.Watch(ctx, nil, func() { notified.Add(1) })).To(Succeed())
		}()
		Eventually(notified.Load).Should(BeEquivalentTo(1))

		fw.fail("clusters", "pg", errors.New("connection refused"))
		Consistently(notified.Load, "100ms").Should(BeEquivalentTo(1))
		Expect(clk.HasWaiters()).To(BeFalse())

		fw.fail("clusters", "pg", errors.Join(errors.New("forbidden"), sources.ErrDenied))
		fw.fail("clusters", "pg", errors.New("connection refused"))
		Expect(strings.Count(logs.String(), "watch failed")).To(Equal(2))
		Expect(strings.Count(logs.String(), "access to object denied")).To(Equal(1))
	})

	It("keeps the watches and answers ErrDenied while the exporter is denied and a Cluster event arrives", func() {
		start()
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))
		fw.set("telemetryexporters", "default", exporter(map[string]any{
			"exporter": map[string]any{"endpoint": "http://c:4318"},
			"metrics":  map[string]any{"querySets": []any{"qs1"}},
		}))
		fw.set("metricquerysets", "qs1", querySet("qs1", map[string]any{"q": map[string]any{"query": "SELECT 1"}}))
		before := fw.active()

		fw.fail("telemetryexporters", "default", errors.Join(errors.New("forbidden"), sources.ErrDenied))
		fw.set("clusters", "pg", enabledCluster(map[string]any{}))

		Consistently(fw.active, "100ms").Should(ConsistOf(before))
		Expect(src.ReadFile(clusterconfig.QuerySetFile("qs1"))).To(MatchJSON(`{"q":{"query":"SELECT 1"}}`))
		_, _, err := src.Settings()
		Expect(err).To(MatchError(sources.ErrDenied))
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
		fw.set("secrets", "envsecret", secret("envsecret", map[string]string{"OTEL_EXPORTER_OTLP_TIMEOUT": sensitive}))
		_, _, err := src.Settings()
		Expect(err).NotTo(HaveOccurred())
		Expect(logs.String()).To(ContainSubstring("OTEL_EXPORTER_OTLP_TIMEOUT"))

		fw.set("secrets", "envsecret", secret("envsecret", map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "k=" + sensitive}))
		fw.fail("secrets", "pw", errors.Join(errors.New("watch broke"), sources.ErrDenied))
		fw.fail("secrets", "envsecret", errors.New("watch broke"))
		_, _, err = src.Settings()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).NotTo(ContainSubstring(sensitive))

		Expect(logs.Len()).NotTo(BeZero())
		Expect(logs.String()).NotTo(ContainSubstring(sensitive))
		Expect(logs.String()).NotTo(ContainSubstring(base64.StdEncoding.EncodeToString([]byte(sensitive))))
	})

	It("waits for the token file and logs the first wait with the stat error", func() {
		dir := GinkgoT().TempDir()
		s := kubesource.New(kubesource.Options{Namespace: "ns", Cluster: "pg", APIServer: "https://x", TokenDir: dir, Clock: clk})
		found := make(chan bool, 1)
		go func() { found <- s.WaitForToken(ctx) }()

		Eventually(clk.HasWaiters).Should(BeTrue())
		clk.Step(5 * time.Second)
		Consistently(found, "50ms").ShouldNot(Receive())

		Expect(writeFile(dir+"/token", "t")).To(Succeed())
		Eventually(clk.HasWaiters).Should(BeTrue())
		clk.Step(5 * time.Second)
		Eventually(found).Should(Receive(BeTrue()))
		Expect(strings.Count(logs.String(), "waiting for the sidecar token")).To(Equal(1))
		Expect(logs.String()).To(ContainSubstring("no such file or directory"))
	})
})

var _ = Describe("Source.Sensitive", func() {
	It("is true for secret/ names only", func() {
		src := kubesource.New(kubesource.Options{Namespace: "ns", Cluster: "c", APIServer: "https://x", TokenDir: "/t"})
		Expect(src.Sensitive("secret/s/k")).To(BeTrue())
		Expect(src.Sensitive("configmap/s/k")).To(BeFalse())
		Expect(src.Sensitive("metricqueryset/s")).To(BeFalse())
		Expect(src.Sensitive("/etc/secret/x")).To(BeFalse())
	})
})
