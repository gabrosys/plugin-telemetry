package devenv_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	deployScript = "../../hack/deploy.sh"
	imageRef     = "registry.example/plugin-telemetry@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func deploy(ctx context.Context, f fakes, args ...string) result {
	kubectl, err := filepath.Abs("testdata/kubectl")
	Expect(err).NotTo(HaveOccurred())

	//nolint:gosec // G204: constant script path and test-controlled arguments
	cmd := exec.CommandContext(ctx, "bash", append([]string{deployScript}, args...)...)
	cmd.Env = append(os.Environ(),
		"KUBECTL="+kubectl,
		"KIND_CLUSTER=pt",
		"BIN="+GinkgoT().TempDir(),
		"FAKE_STATE="+f.state,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	code := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "run deploy.sh: %v", err)
		code = exitErr.ExitCode()
	}

	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

var _ = Describe("deploy.sh", func() {
	var f fakes

	BeforeEach(func() {
		f = newFakes()
	})

	It("applies the CRDs first and annotates the Service with the image after the rollout", func(ctx SpecContext) {
		r := deploy(ctx, f, imageRef)
		Expect(r.code).To(Equal(0), r.stderr)

		calls := strings.Split(strings.TrimSpace(f.read("calls")), "\n")
		Expect(calls).To(HaveLen(6), strings.Join(calls, "\n"))
		Expect(calls[0]).To(MatchRegexp(`^kubectl apply --server-side --force-conflicts -k \S+/config/crd$`))
		Expect(calls[1]).To(Equal("kubectl wait --for=condition=Established " +
			"crd/metricquerysets.telemetry.gabrosys.github.io crd/telemetryexporters.telemetry.gabrosys.github.io --timeout=60s"))
		Expect(calls[2]).To(MatchRegexp(`^kubectl apply --server-side --force-conflicts -k \S+/deploy$`))
		Expect(calls[3]).To(HavePrefix("kubectl wait --for=condition=Ready certificate"))
		Expect(calls[4]).To(Equal("kubectl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s"))
		Expect(calls[5]).To(Equal("kubectl annotate service -n cnpg-system plugin-telemetry " +
			"telemetry.gabrosys.github.io/image=" + imageRef + " --overwrite"))
	})

	It("applies nothing else when the CRDs are not established", func(ctx SpecContext) {
		f.write("crd-not-established", "")
		r := deploy(ctx, f, imageRef)
		Expect(r.code).NotTo(Equal(0))

		calls := f.read("calls")
		Expect(calls).NotTo(MatchRegexp(regexp.QuoteMeta("-k ") + `\S+/deploy`))
		Expect(calls).NotTo(ContainSubstring("rollout status"))
		Expect(calls).NotTo(ContainSubstring("annotate"))
	})

	It("rejects a reference without a digest", func(ctx SpecContext) {
		r := deploy(ctx, f, "registry.example/plugin-telemetry:latest")
		Expect(r.code).To(Equal(2))
		Expect(f.read("calls")).To(BeEmpty())
	})
})
