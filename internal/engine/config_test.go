package engine_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/fs"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// memConfig is an engine.Config held in memory. Its paths exist nowhere on disk.
type memConfig struct {
	mu       sync.Mutex // guards every field below
	doc      string
	noConfig string // when set, Settings fails with ErrNoConfiguration for this reason
	files    map[string]string
	fileErrs map[string]error
	reads    map[string]int
	watched  [][]string
	notifies []func()
}

func newMemConfig(doc string, files map[string]string) *memConfig {
	return &memConfig{doc: doc, files: files, fileErrs: map[string]error{}, reads: map[string]int{}}
}

func (c *memConfig) Name() string { return "mem" }

func (c *memConfig) Settings() (settings.Settings, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.noConfig != "" {
		return settings.Settings{}, nil, fmt.Errorf("%w: %s", engine.ErrNoConfiguration, c.noConfig)
	}
	s, err := settings.Parse([]byte(c.doc), "/mem")
	return s, []byte(c.doc), err
}

func (c *memConfig) ReadFile(name string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads[name]++
	if err := c.fileErrs[name]; err != nil {
		return nil, fmt.Errorf("get %s: %w", name, err)
	}
	data, ok := c.files[name]
	if !ok {
		return nil, fmt.Errorf("read %s: %w", name, fs.ErrNotExist)
	}
	return []byte(data), nil
}

func (c *memConfig) Watch(ctx context.Context, files []string, notify func(), opts ...sources.WatchOption) error {
	ready, _ := sources.WatchHooks(opts...)
	c.mu.Lock()
	c.watched = append(c.watched, slices.Clone(files))
	c.notifies = append(c.notifies, notify)
	c.mu.Unlock()
	ready()
	<-ctx.Done()
	return nil
}

func (c *memConfig) set(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f()
}

func (c *memConfig) readCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads[name]
}

func (c *memConfig) readNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.reads))
	for n := range c.reads {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

const memSettings = `cluster: mem
postgres:
  host: 127.0.0.1
  port: 1
  passwordFile: /mem/pw
defaultInterval: 30s
sources:
  - /mem/queries.yaml
exporter:
  endpoint: https://127.0.0.1:1/v1/metrics
  headersFile: /mem/headers
  tls:
    caFile: /mem/ca.pem
    certFile: /mem/tls.crt
    keyFile: /mem/tls.key
`

// selfSigned returns a certificate and its key, PEM encoded.
func selfSigned() (certPEM, keyPEM string) {
	GinkgoHelper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "mem"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	Expect(err).NotTo(HaveOccurred())
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

func memFiles() map[string]string {
	GinkgoHelper()
	cert, key := selfSigned()
	return map[string]string{
		"/mem/queries.yaml": alphaBeta,
		"/mem/pw":           "secret\n",
		"/mem/headers":      "authorization=Bearer x",
		"/mem/ca.pem":       cert,
		"/mem/tls.crt":      cert,
		"/mem/tls.key":      key,
	}
}

var memPrimary = planner.Facts{Version: *semver.MustParse("18.0.7"), Databases: []string{"app", "postgres"}}

var _ = Describe("An in-memory Config", func() {
	Describe("the replanner", func() {
		var (
			cfg   *memConfig
			plans []planner.Plan
			r     *engine.Replanner
		)

		BeforeEach(func() {
			cfg = newMemConfig(strings.ReplaceAll(unitSettings, "queries.yaml", "/mem/queries.yaml"),
				map[string]string{"/mem/queries.yaml": alphaBeta})
			plans = nil
			r = engine.NewReplanner(cfg, selfmetrics.NewStats(), snapshot.New(0), func(p planner.Plan) { plans = append(plans, p) })
		})

		It("runs from an in-memory Config exactly as from files", func(ctx SpecContext) {
			Expect(r.Reload(ctx)).To(Succeed())
			Expect(plans).To(BeEmpty())
			r.SetFacts(ctx, memPrimary)
			Expect(plans).To(HaveLen(1))
			Expect(jobNames(plans[0])).To(ConsistOf("alpha@postgres", "beta@postgres", "collector@app", "collector@postgres"))

			Expect(r.Reload(ctx)).To(Succeed())
			Expect(plans).To(HaveLen(1))

			cfg.set(func() { cfg.files["/mem/queries.yaml"] = alphaBeta + gamma })
			Expect(r.Reload(ctx)).To(Succeed())
			Expect(plans).To(HaveLen(2))
			Expect(jobNames(plans[1])).To(ContainElement("gamma@postgres"))

			cfg.set(func() { cfg.doc = strings.Replace(cfg.doc, "defaultInterval: 30s", "defaultInterval: 45s", 1) })
			Expect(r.Reload(ctx)).To(Succeed())
			Expect(plans).To(HaveLen(3))

			cfg.set(func() { cfg.doc = "cluster: \"\"\n" })
			Expect(r.Reload(ctx)).To(MatchError(ContainSubstring("cluster")))
			Expect(plans).To(HaveLen(3))
		})

		It("reloads when the Config's settings bytes change and not when they are equal", func(ctx SpecContext) {
			Expect(r.Reload(ctx)).To(Succeed())
			Expect(r.Changed()).To(BeFalse())

			cfg.set(func() { cfg.doc += "# same settings, other bytes\n" })
			Expect(r.Changed()).To(BeTrue())
			Expect(r.Reload(ctx)).To(Succeed())
			Expect(r.Changed()).To(BeFalse())

			cfg.set(func() { cfg.doc += "" })
			Expect(r.Changed()).To(BeFalse())

			cfg.set(func() { cfg.files["/mem/queries.yaml"] = alphaBeta + gamma })
			Expect(r.Changed()).To(BeTrue())
		})

		It("marks a denied source OutcomeDenied in its status and keeps its last good version", func(ctx SpecContext) {
			Expect(r.Reload(ctx)).To(Succeed())
			r.SetFacts(ctx, memPrimary)
			Expect(plans).To(HaveLen(1))

			cfg.set(func() { cfg.fileErrs["/mem/queries.yaml"] = sources.ErrDenied })
			Expect(r.Changed()).To(BeTrue())
			Expect(r.Reload(ctx)).To(Succeed())
			Expect(plans).To(HaveLen(1))
			Expect(jobNames(plans[0])).To(ContainElement("alpha@postgres"))

			res := sources.NewLoader(cfg.ReadFile, []string{"/mem/queries.yaml"}).Load()
			Expect(res.Status[1].Outcome).To(Equal(sources.OutcomeDenied))
		})
	})

	Describe("the engine", func() {
		var (
			cfg   *memConfig
			steps *engine.Steps
		)

		BeforeEach(func(ctx SpecContext) {
			cfg = newMemConfig(memSettings, memFiles())
			var err error
			steps, err = engine.NewSteps(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(steps.Close)
		})

		It("reads sources, the password, headers and TLS material through Config.ReadFile only", func(ctx SpecContext) {
			Expect(steps.Apply(ctx)).To(Succeed())
			steps.SetFacts(ctx, memPrimary)
			steps.FactsRound(ctx)

			for _, name := range []string{
				"/mem/queries.yaml", "/mem/pw", "/mem/headers", "/mem/ca.pem", "/mem/tls.crt", "/mem/tls.key",
			} {
				Expect(cfg.readCount(name)).To(BeNumerically(">", 0), name)
			}
			Expect(cfg.readNames()).To(ConsistOf(
				"/mem/queries.yaml", "/mem/pw", "/mem/headers", "/mem/ca.pem", "/mem/tls.crt", "/mem/tls.key"))
			Expect(steps.State().Exporter).To(BeTrue())
		})

		It("reads the password through pg.Config.ReadFile", func() {
			var read []string
			c := pg.Config{
				Host: "/run", Port: 5432, User: "u", PasswordFile: "/mem/pw", QueryTimeout: time.Second, LockTimeout: time.Second,
				ReadFile: func(name string) ([]byte, error) { read = append(read, name); return []byte("pw\n"), nil },
			}
			_, err := pg.ConnConfig(c, "app")
			Expect(err).NotTo(HaveOccurred())
			Expect(read).To(Equal([]string{"/mem/pw"}))
		})

		It("stops the export loop, the scheduler's jobs and the pool on ErrNoConfiguration, and resumes on the next valid settings", func(ctx SpecContext) {
			Expect(steps.Apply(ctx)).To(Succeed())
			steps.SetFacts(ctx, memPrimary)
			steps.SetUp(true)
			Expect(steps.State()).To(Equal(engine.State{Loop: true, Runner: true, Exporter: true, Current: true, Jobs: 4}))
			up, err := steps.UpReported(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(up).To(BeTrue())
			pool := steps.Pool()

			cfg.set(func() { cfg.noConfig = "the Cluster has no telemetry" })
			Expect(steps.Apply(ctx)).To(MatchError(engine.ErrNoConfiguration))
			Expect(steps.State()).To(Equal(engine.State{}))
			steps.Settle()
			_, _, err = pool.Acquire(ctx, "postgres")
			Expect(err).To(MatchError(pg.ErrPoolClosed))
			up, err = steps.UpReported(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(up).To(BeFalse())

			cfg.set(func() { cfg.noConfig = "" })
			Expect(steps.Apply(ctx)).To(Succeed())
			steps.SetFacts(ctx, memPrimary)
			Expect(steps.State()).To(Equal(engine.State{Loop: true, Runner: true, Exporter: true, Current: true, Jobs: 4}))
		})

		It("logs the ErrNoConfiguration reason once per change of reason", func(ctx SpecContext) {
			var mu sync.Mutex
			var lines []string
			logger := funcr.New(func(_, args string) {
				mu.Lock()
				defer mu.Unlock()
				lines = append(lines, args)
			}, funcr.Options{})
			lctx := ctrllog.IntoContext(ctx, logger)
			idleLines := func(reason string) int {
				mu.Lock()
				defer mu.Unlock()
				n := 0
				for _, l := range lines {
					if strings.Contains(l, reason) {
						n++
					}
				}
				return n
			}

			for _, reason := range []string{"reason one", "reason one", "reason two", "reason two", "reason one"} {
				cfg.set(func() { cfg.noConfig = reason })
				steps.Reload(lctx)
			}
			Expect(idleLines("reason one")).To(Equal(2))
			Expect(idleLines("reason two")).To(Equal(1))

			cfg.set(func() { cfg.noConfig = "" })
			steps.Reload(lctx)
			cfg.set(func() { cfg.noConfig = "reason one" })
			steps.Reload(lctx)
			Expect(idleLines("reason one")).To(Equal(3))
		})
	})
})

func jobNames(p planner.Plan) []string {
	out := make([]string, 0, len(p.Jobs))
	for _, j := range p.Jobs {
		out = append(out, j.Query+"@"+j.Database)
	}
	return out
}
