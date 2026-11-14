package cli_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/cli"
	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/kubesource"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// fakeKube is a Kubernetes source over the settings of a testdata file, with virtual file names.
type fakeKube struct {
	base    engine.Config
	mutate  func(*settings.Settings)
	files   map[string][]byte
	mu      sync.Mutex
	started int
	reads   []string
}

func newFakeKube(config string, files map[string][]byte, mutate func(*settings.Settings)) *fakeKube {
	return &fakeKube{base: engine.FileConfig(testdata(config)), files: files, mutate: mutate}
}

// sensitive makes the names with the prefix sensitive, as the real source does for Secrets.
func (f *fakeKube) sensitive(prefix string) engine.Config {
	return &sensitiveKube{fakeKube: f, prefix: prefix}
}

type sensitiveKube struct {
	*fakeKube
	prefix string
}

func (s *sensitiveKube) Sensitive(name string) bool { return strings.HasPrefix(name, s.prefix) }

func (f *fakeKube) Name() string { return "kubernetes" }

func (f *fakeKube) Settings() (settings.Settings, []byte, error) {
	s, raw, err := f.base.Settings()
	if err != nil {
		return s, raw, err
	}
	if f.mutate != nil {
		f.mutate(&s)
	}
	return s, raw, nil
}

func (f *fakeKube) ReadFile(name string) ([]byte, error) {
	f.mu.Lock()
	f.reads = append(f.reads, name)
	f.mu.Unlock()
	if data, ok := f.files[name]; ok {
		return data, nil
	}
	return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
}

func (f *fakeKube) Watch(ctx context.Context, _ []string, _ func(), _ ...sources.WatchOption) error {
	<-ctx.Done()
	return nil
}

func (f *fakeKube) Start(ctx context.Context) error {
	f.mu.Lock()
	f.started++
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (f *fakeKube) readNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.reads...)
}

func setKubeEnv() {
	GinkgoHelper()
	GinkgoT().Setenv("NAMESPACE", "app")
	GinkgoT().Setenv("CLUSTER_NAME", "demo")
	GinkgoT().Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	GinkgoT().Setenv("KUBERNETES_SERVICE_PORT", "443")
}

func readTestdata(name string) []byte {
	GinkgoHelper()
	raw, err := os.ReadFile(testdata(name))
	Expect(err).NotTo(HaveOccurred())
	return raw
}

var _ = Describe("instance in Kubernetes mode", func() {
	var gotOptions []kubesource.Options

	useRun := func(src *fakeKube) {
		DeferCleanup(cli.SetKubeSources(
			func(o kubesource.Options) (cli.KubeSource, error) {
				gotOptions = append(gotOptions, o)
				return src, nil
			},
			func(context.Context, kubesource.Options) (engine.Config, error) {
				return nil, errors.New("unexpected snapshot")
			}))
	}

	BeforeEach(func() { gotOptions = nil })

	It("runs the engine on the Kubernetes source when --config is not set", func() {
		setKubeEnv()
		src := newFakeKube("engine-valid.yaml", nil, nil)
		useRun(src)
		DeferCleanup(cli.SetMemlimit(func(func(string) ([]byte, error), func(string) string) (int64, error) { return 0, nil }))

		logFile := filepath.Join(GinkgoT().TempDir(), "log.json")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := execute(ctx, "--log-destination", logFile, "instance")
			done <- err
		}()
		Eventually(func(g Gomega) {
			raw, err := os.ReadFile(logFile) //nolint:gosec // a file the spec created
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(string(raw)).To(ContainSubstring("starting the metrics engine"))
		}).WithTimeout(10 * time.Second).Should(Succeed())
		cancel()
		Eventually(done, 20*time.Second).Should(Receive(BeNil()))

		Expect(gotOptions).To(HaveLen(1))
		Expect(gotOptions[0].Namespace).To(Equal("app"))
		Expect(gotOptions[0].Cluster).To(Equal("demo"))
		Expect(gotOptions[0].APIServer).To(Equal("https://10.0.0.1:443"))
		Expect(gotOptions[0].TokenDir).To(Equal(metadata.SidecarTokenDir))
		src.mu.Lock()
		defer src.mu.Unlock()
		Expect(src.started).To(Equal(1))
	})

	DescribeTable("logs the variable and idles instead of exiting when the environment is incomplete",
		func(unset string) {
			setKubeEnv()
			GinkgoT().Setenv(unset, "")
			useRun(newFakeKube("engine-valid.yaml", nil, nil))

			logFile := filepath.Join(GinkgoT().TempDir(), "log.json")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := execute(ctx, "--log-destination", logFile, "instance")
				done <- err
			}()
			Eventually(func(g Gomega) {
				raw, err := os.ReadFile(logFile) //nolint:gosec // a file the spec created
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(raw)).To(ContainSubstring(unset))
			}).WithTimeout(5 * time.Second).Should(Succeed())
			Consistently(done, 300*time.Millisecond).ShouldNot(Receive())
			cancel()
			Eventually(done, time.Second).Should(Receive(BeNil()))
			Expect(gotOptions).To(BeEmpty())
		},
		Entry("NAMESPACE", "NAMESPACE"),
		Entry("CLUSTER_NAME", "CLUSTER_NAME"),
		Entry("KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_HOST"),
		Entry("KUBERNETES_SERVICE_PORT", "KUBERNETES_SERVICE_PORT"),
	)

	It("clears PG* and OTEL_* and sets the memory limit", func() {
		setKubeEnv()
		const pgValue, otelValue = "secret-pg-host", "secret-otel-token"
		GinkgoT().Setenv("PGHOST", pgValue)
		GinkgoT().Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization="+otelValue)
		useRun(newFakeKube("engine-valid.yaml", nil, nil))
		limits := 0
		DeferCleanup(cli.SetMemlimit(func(func(string) ([]byte, error), func(string) string) (int64, error) {
			limits++
			return 1 << 20, nil
		}))

		logFile := filepath.Join(GinkgoT().TempDir(), "log.json")
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := execute(ctx, "--log-destination", logFile, "instance")
		Expect(err).NotTo(HaveOccurred())

		_, pgSet := os.LookupEnv("PGHOST")
		_, otelSet := os.LookupEnv("OTEL_EXPORTER_OTLP_HEADERS")
		Expect(pgSet).To(BeFalse())
		Expect(otelSet).To(BeFalse())
		Expect(limits).To(Equal(1))
		logged, err := os.ReadFile(logFile) //nolint:gosec // a file the spec created
		Expect(err).NotTo(HaveOccurred())
		Expect(string(logged)).To(ContainSubstring("PGHOST"))
		Expect(string(logged)).NotTo(ContainSubstring(pgValue))
		Expect(string(logged)).NotTo(ContainSubstring(otelValue))
	})
})

var _ = Describe("instance tools in Kubernetes mode", func() {
	var (
		gotOptions []kubesource.Options
		src        *fakeKube
	)

	useSnapshot := func(s engine.Config) {
		if fk, ok := s.(*fakeKube); ok {
			src = fk
		}
		DeferCleanup(cli.SetKubeSources(
			func(kubesource.Options) (cli.KubeSource, error) { return nil, errors.New("unexpected run") },
			func(_ context.Context, o kubesource.Options) (engine.Config, error) {
				gotOptions = append(gotOptions, o)
				return s, nil
			}))
	}

	virtual := func(s *settings.Settings) {
		s.Sources = []string{"configmap/queries/probe.yaml", "metricqueryset/extra"}
		s.Exporter.HeadersFile = "secret/otlp/headers"
	}
	virtualFiles := func() map[string][]byte {
		return map[string][]byte{
			"configmap/queries/probe.yaml": readTestdata("queries-warn.yaml"),
			"metricqueryset/extra":         readTestdata("queries-warn.yaml"),
			"secret/otlp/headers":          []byte("authorization=s3cr3t-token,x-tenant=acme-secret"),
		}
	}

	BeforeEach(func() {
		gotOptions = nil
		setKubeEnv()
	})

	It("print-config reads a snapshot, shows the virtual names and never a Secret value", func() {
		useSnapshot(newFakeKube("engine-valid.yaml", virtualFiles(), virtual))
		out, err := execute(context.Background(), "instance", "print-config", "--offline")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("ok configmap/queries/probe.yaml"))
		Expect(out).To(ContainSubstring("ok metricqueryset/extra"))
		Expect(out).To(ContainSubstring("authorization: <redacted>"))
		Expect(out).To(ContainSubstring("x-tenant: <redacted>"))
		Expect(out).NotTo(ContainSubstring("s3cr3t-token"))
		Expect(out).NotTo(ContainSubstring("acme-secret"))

		Expect(gotOptions).To(HaveLen(1))
		Expect(gotOptions[0]).To(Equal(kubesource.Options{
			Namespace: "app", Cluster: "demo", APIServer: "https://10.0.0.1:443", TokenDir: metadata.SidecarTokenDir,
		}))
	})

	It("validate reads a snapshot", func() {
		useSnapshot(newFakeKube("engine-valid.yaml", virtualFiles(), virtual))
		out, err := execute(context.Background(), "instance", "validate")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("warning metricqueryset/extra probe: "))
		Expect(gotOptions).To(HaveLen(1))
	})

	It("test reads the password through the Kubernetes source", func() {
		useSnapshot(newFakeKube("engine-unreachable.yaml", virtualFiles(), func(s *settings.Settings) {
			virtual(s)
			s.Postgres.PasswordFile = "secret/pg/password"
		}))
		_, err := execute(context.Background(), "instance", "test")
		Expect(err).To(HaveOccurred())
		Expect(src.readNames()).To(ContainElement("secret/pg/password"))
	})

	It("fails and names the variable when the environment is incomplete", func() {
		GinkgoT().Setenv("CLUSTER_NAME", "")
		useSnapshot(newFakeKube("engine-valid.yaml", nil, nil))
		for _, sub := range []string{"validate", "test", "print-config"} {
			_, err := execute(context.Background(), "instance", sub)
			Expect(err).To(MatchError(ContainSubstring("CLUSTER_NAME")), sub)
		}
		Expect(gotOptions).To(BeEmpty())
	})

	It("never prints the content of a malformed Secret-backed source", func() {
		files := virtualFiles()
		files["secret/queries/q.yaml"] = []byte("a:\n  query: SELECT 1\nb: SENTINELVALUE-do-not-leak\n")
		useSnapshot(newFakeKube("engine-valid.yaml", files, func(s *settings.Settings) {
			virtual(s)
			s.Sources = append(s.Sources, "secret/queries/q.yaml")
		}).sensitive("secret/"))

		out, err := execute(context.Background(), "instance", "validate")
		Expect(err).To(HaveOccurred())
		Expect(out).To(ContainSubstring("error secret/queries/q.yaml -: "))
		Expect(out + err.Error()).NotTo(ContainSubstring("SENTINE"))

		out, err = execute(context.Background(), "instance", "print-config", "--offline")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("invalid secret/queries/q.yaml: "))
		Expect(out).NotTo(ContainSubstring("SENTINE"))
	})
})
