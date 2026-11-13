package devenv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	script    = "../../hack/dev-env.sh"
	nodeImage = "kindest/node:v1.37.0@sha256:0000"

	certManagerApply = "kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.3/cert-manager.yaml"
	certManagerCheck = "cmctl check api --context kind-pt --wait=2m"
	cnpgReleaseApply = "kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml"
	cnpgMainApply    = "kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/artifacts/main/manifests/operator-manifest.yaml"
)

type result struct {
	stdout, stderr string
	code           int
}

type fakes struct{ state string }

func newFakes() fakes {
	return fakes{state: GinkgoT().TempDir()}
}

func (f fakes) write(name, content string) {
	Expect(os.WriteFile(filepath.Join(f.state, name), []byte(content), 0o600)).To(Succeed())
}

func (f fakes) read(name string) string {
	b, err := fs.ReadFile(os.DirFS(f.state), name)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	Expect(err).NotTo(HaveOccurred())

	return string(b)
}

func (f fakes) marker(cnpgVersion, complete string) {
	f.write("marker", fmt.Sprintf("certManagerVersion=v1.21.3\ncnpgVersion=%s\ncomplete=%s\nkindNodeImage=%s\n",
		cnpgVersion, complete, nodeImage))
}

func (f fakes) up(ctx context.Context, cnpgVersion string, extraEnv ...string) result {
	kind, err := filepath.Abs("testdata/kind")
	Expect(err).NotTo(HaveOccurred())
	kubectl, err := filepath.Abs("testdata/kubectl")
	Expect(err).NotTo(HaveOccurred())
	cmctl, err := filepath.Abs("testdata/cmctl")
	Expect(err).NotTo(HaveOccurred())

	cmd := exec.CommandContext(ctx, "bash", script, "up")
	cmd.Env = append(os.Environ(),
		"KIND="+kind,
		"KUBECTL="+kubectl,
		"CMCTL="+cmctl,
		"KIND_CLUSTER=pt",
		"KIND_NODE_IMAGE="+nodeImage,
		"CNPG_VERSION="+cnpgVersion,
		"CERT_MANAGER_VERSION=v1.21.3",
		"FAKE_STATE="+f.state,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	code := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "run dev-env.sh: %v", err)
		code = exitErr.ExitCode()
	}

	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

var _ = Describe("dev-env.sh up", func() {
	var f fakes

	BeforeEach(func() {
		f = newFakes()
	})

	It("installs a fresh environment and marks it complete", func(ctx SpecContext) {
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(0), r.stderr)
		Expect(r.stdout).To(ContainSubstring("dev-env ready:"))
		Expect(f.read("calls")).To(ContainSubstring("kind create cluster --name pt"))
		Expect(f.read("marker")).To(And(ContainSubstring("cnpgVersion=v1.30.1"), ContainSubstring("complete=true")))
	})

	It("installs cert-manager and a CNPG release as their docs describe", func(ctx SpecContext) {
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(0), r.stderr)
		Expect(strings.Split(strings.TrimSpace(f.read("calls")), "\n")).To(ContainElements(
			certManagerApply, certManagerCheck, cnpgReleaseApply,
		))
		Expect(f.read("calls")).To(MatchRegexp(
			regexp.QuoteMeta(certManagerApply) + `(?s).*` + regexp.QuoteMeta(certManagerCheck) + `.*` + regexp.QuoteMeta(cnpgReleaseApply),
		))
	})

	It("installs CNPG main from the artifacts manifest", func(ctx SpecContext) {
		r := f.up(ctx, "main")
		Expect(r.code).To(Equal(0), r.stderr)
		Expect(f.read("calls")).To(ContainSubstring(cnpgMainApply))
	})

	It("stops when the cert-manager API never becomes ready", func(ctx SpecContext) {
		f.write("cert-manager-api-down", "")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).NotTo(Equal(0))
		Expect(f.read("calls")).NotTo(ContainSubstring("cloudnative-pg"))
		Expect(f.read("marker")).To(ContainSubstring("complete=false"))
	})

	It("does nothing when the environment is complete and current", func(ctx SpecContext) {
		f.write("clusters", "pt\n")
		f.marker("v1.30.1", "true")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(0), r.stderr)
		Expect(r.stdout).To(ContainSubstring("dev-env up to date:"))
		Expect(f.read("calls")).NotTo(ContainSubstring("apply --server-side"))
	})

	It("refuses a different version", func(ctx SpecContext) {
		f.write("clusters", "pt\n")
		f.marker("main", "true")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(2))
		Expect(r.stderr).To(ContainSubstring("environment has cnpgVersion=main, requested v1.30.1"))
	})

	It("refuses a listed cluster whose kubeconfig cannot be exported", func(ctx SpecContext) {
		f.write("clusters", "pt\n")
		f.write("no-kubeconfig", "")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(1))
		Expect(r.stderr).To(ContainSubstring("cluster pt exists but is not reachable; run 'make dev-down' first"))
		Expect(f.read("calls")).NotTo(ContainSubstring("apply"))
	})

	It("refuses a cluster whose node never becomes ready", func(ctx SpecContext) {
		f.write("clusters", "pt\n")
		f.write("node-not-ready", "")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(1))
		Expect(r.stderr).To(ContainSubstring("cluster pt node is not ready; run 'make dev-down' first"))
		Expect(f.read("calls")).NotTo(ContainSubstring("apply"))
	})

	It("records the requested versions before installing", func(ctx SpecContext) {
		r := f.up(ctx, "main", "FAKE_FAIL_APPLY=artifacts")
		Expect(r.code).NotTo(Equal(0))
		Expect(f.read("marker")).To(And(ContainSubstring("cnpgVersion=main"), ContainSubstring("complete=false")))

		r = f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(2))
		Expect(r.stderr).To(ContainSubstring("environment has cnpgVersion=main, requested v1.30.1"))
	})

	It("finishes an interrupted install of the same versions", func(ctx SpecContext) {
		f.write("clusters", "pt\n")
		f.marker("v1.30.1", "false")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(0), r.stderr)
		Expect(f.read("calls")).To(ContainSubstring(cnpgReleaseApply))
		Expect(f.read("marker")).To(ContainSubstring("complete=true"))
	})

	It("finds its cluster among many others without recreating it", func(ctx SpecContext) {
		others := make([]string, 20000)
		for i := range others {
			others[i] = fmt.Sprintf("other-%d", i)
		}
		f.write("clusters", "pt\n"+strings.Join(others, "\n")+"\n")
		f.marker("v1.30.1", "true")
		r := f.up(ctx, "v1.30.1")
		Expect(r.code).To(Equal(0), r.stderr)
		Expect(f.read("calls")).NotTo(ContainSubstring("kind create cluster"))
	})
})
