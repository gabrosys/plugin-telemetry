//go:build e2e

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"text/template"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/pgtest"
)

func TestEngine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Engine E2E Suite")
}

const (
	collectorConfig = "/etc/otelcol/e2e.yaml"
	collectorOutput = "/out/metrics.jsonl"
	collectorPort   = "4318/tcp"

	podName   = "e2e-1"
	podUID    = "00000000-0000-4000-8000-0000000000e2"
	nodeName  = "e2e-node"
	namespace = "e2e"

	// engineStopTimeout bounds the engine's shutdown after SIGTERM before it is killed.
	engineStopTimeout = 20 * time.Second
)

var (
	pgServer *pgtest.Postgres
	otelcol  *collector
	proc     *engineProcess
	// configDir holds engine.yaml, its query sources and the password file; configRoot opens it.
	configDir  string
	configRoot *os.Root
)

var _ = BeforeSuite(func(ctx SpecContext) {
	env := map[string]string{}
	for _, name := range []string{"ENGINE_BIN", "POSTGRES_IMAGE", "COLLECTOR_IMAGE"} {
		env[name] = os.Getenv(name)
		Expect(env[name]).NotTo(BeEmpty(), "%s must be set (make e2e-engine sets it)", name)
	}

	var err error
	pgServer, err = pgtest.Start(ctx, env["POSTGRES_IMAGE"])
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) {
		Expect(pgServer.Terminate(ctx)).To(Succeed())
	})

	otelcol, err = startCollector(ctx, env["COLLECTOR_IMAGE"])
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) {
		Expect(otelcol.container.Terminate(ctx)).To(Succeed())
	})

	configDir = GinkgoT().TempDir()
	configRoot, err = os.OpenRoot(configDir)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(configRoot.Close)
	queries, err := os.ReadFile(filepath.Join("testdata", "default-queries.yaml"))
	Expect(err).NotTo(HaveOccurred())
	writeFile("default-queries.yaml", string(queries))
	writeFile("extra.yaml", "")
	writeFile("engine.yaml", renderEngineConfig(engineConfig{
		PostgresHost: pgServer.Host,
		PostgresPort: pgServer.Port,
		PostgresUser: pgtest.MonitorUser,
		PasswordFile: pgServer.MonitorConfig(configDir).PasswordFile,
		Endpoint:     otelcol.endpoint,
	}))

	proc, err = startEngine(ctx, env["ENGINE_BIN"], filepath.Join(configDir, "engine.yaml"))
	Expect(err).NotTo(HaveOccurred())
}, NodeTimeout(5*time.Minute))

var _ = AfterSuite(func(ctx SpecContext) {
	if proc == nil {
		return
	}
	proc.cancel()
	Eventually(proc.exited).WithContext(ctx).WithTimeout(engineStopTimeout + 5*time.Second).Should(BeClosed())
	// Wait reports the context's error when the process exits with status 0 after SIGTERM.
	Expect(proc.err).To(MatchError(context.Canceled), "the engine did not exit cleanly on SIGTERM")
}, NodeTimeout(time.Minute))

// writeFile writes the file called name in configDir.
func writeFile(name, content string) {
	GinkgoHelper()
	Expect(configRoot.WriteFile(name, []byte(content), 0o600)).To(Succeed())
}

// engineConfig fills testdata/engine.yaml.
type engineConfig struct {
	PostgresHost string
	PostgresPort uint16
	PostgresUser string
	PasswordFile string
	Endpoint     string
}

func renderEngineConfig(c engineConfig) string {
	GinkgoHelper()
	tmpl, err := template.New("engine.yaml").Option("missingkey=error").ParseFiles(filepath.Join("testdata", "engine.yaml"))
	Expect(err).NotTo(HaveOccurred())
	var b strings.Builder
	Expect(tmpl.Execute(&b, c)).To(Succeed())
	return b.String()
}

// collector is the OpenTelemetry Collector container and the lines read from its file exporter.
type collector struct {
	container *testcontainers.DockerContainer
	endpoint  string // the OTLP/HTTP metrics URL
	lines     otlp.Lines
}

func startCollector(ctx context.Context, image string) (*collector, error) {
	c, err := testcontainers.Run(ctx, image,
		testcontainers.WithFiles(
			testcontainers.ContainerFile{
				HostFilePath:      filepath.Join("testdata", "collector.yaml"),
				ContainerFilePath: collectorConfig,
				FileMode:          0o644,
			},
			// docker cp cannot read a tmpfs, so the output file lives in the container's filesystem,
			// writable by the collector's user.
			testcontainers.ContainerFile{
				Reader:            strings.NewReader(""),
				ContainerFilePath: collectorOutput,
				FileMode:          0o666,
			},
		),
		testcontainers.WithCmd("--config", collectorConfig),
		testcontainers.WithExposedPorts(collectorPort),
		testcontainers.WithLogConsumers(ginkgoLogs{prefix: "otelcol"}),
		testcontainers.WithWaitStrategy(wait.ForLog("Everything is ready. Begin running and processing data.")),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("start the collector: %w", err), terminate(ctx, c))
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("get the collector host: %w", err), terminate(ctx, c))
	}
	port, err := c.MappedPort(ctx, collectorPort)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("get the collector port: %w", err), terminate(ctx, c))
	}
	return &collector{container: c, endpoint: "http://" + host + ":" + strconv.Itoa(int(port.Num())) + "/v1/metrics"}, nil
}

func terminate(ctx context.Context, c *testcontainers.DockerContainer) error {
	if err := c.Terminate(ctx); err != nil {
		return fmt.Errorf("terminate the container: %w", err)
	}
	return nil
}

// read copies the collector's output file and returns every line decoded so far.
func (c *collector) read(ctx context.Context) ([]otlp.Line, error) {
	rc, err := c.container.CopyFileFromContainer(ctx, collectorOutput)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %w", collectorOutput, err)
	}
	data, readErr := io.ReadAll(rc)
	if err := errors.Join(readErr, rc.Close()); err != nil {
		return nil, fmt.Errorf("read %s: %w", collectorOutput, err)
	}
	if err := c.lines.Update(data); err != nil {
		return nil, fmt.Errorf("decode %s: %w", collectorOutput, err)
	}
	return c.lines.All(), nil
}

// exported returns every line the collector has written, for a polled function; it stops the poll
// at once when the engine has exited.
func exported(ctx context.Context, g Gomega) []otlp.Line {
	select {
	case <-proc.exited:
		StopTrying("the engine exited").Wrap(proc.err).Now()
	default:
	}
	lines, err := otelcol.read(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	return lines
}

// ginkgoLogs writes container logs to GinkgoWriter.
type ginkgoLogs struct{ prefix string }

func (g ginkgoLogs) Accept(l testcontainers.Log) {
	GinkgoWriter.Printf("%s: %s", g.prefix, l.Content)
}

// engineProcess is a running `plugin-telemetry instance --config`.
type engineProcess struct {
	version string // the binary's version, which the instrumentation scope carries
	started time.Time
	cancel  context.CancelFunc
	exited  chan struct{} // closed once the process has been waited for
	err     error         // Wait's result, set before exited is closed
}

// startEngine runs bin with a minimal environment: the downward-API variables, plus a PGPASSWORD and an
// OTLP endpoint variable that must change nothing.
func startEngine(ctx context.Context, bin, config string) (*engineProcess, error) {
	version, err := binaryVersion(ctx, bin)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cmd := exec.CommandContext(runCtx, bin, "instance", "--config", config) //nolint:gosec // ENGINE_BIN is the binary under test
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + GinkgoT().TempDir(),
		"POD_NAME=" + podName,
		"POD_UID=" + podUID,
		"NODE_NAME=" + nodeName,
		"NAMESPACE=" + namespace,
		"PGPASSWORD=junk",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:1",
	}
	cmd.Stdout, cmd.Stderr = GinkgoWriter, GinkgoWriter
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = engineStopTimeout

	p := &engineProcess{version: version, started: time.Now(), cancel: cancel, exited: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	go func() {
		defer close(p.exited)
		p.err = cmd.Wait()
	}()
	return p, nil
}

// binaryVersion returns the version `bin version` prints.
func binaryVersion(ctx context.Context, bin string) (string, error) {
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, "version")
	cmd.Env = []string{}
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("run %s version: %w", bin, err)
	}
	fields := strings.Fields(out.String())
	if len(fields) < 2 {
		return "", fmt.Errorf("parse %s version: unexpected output %q", bin, out.String())
	}
	return fields[1], nil
}
