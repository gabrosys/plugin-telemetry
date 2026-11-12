package devenv_test

import (
	"errors"
	"os/exec"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const curlUnsupportedProtocol = 1

var _ = Describe("fetch-tool.sh", func() {
	It("refuses plain HTTP downloads", func(ctx SpecContext) {
		dest := filepath.Join(GinkgoT().TempDir(), "tool")
		//nolint:gosec // G204: constant arguments plus a test temp dir
		cmd := exec.CommandContext(ctx, "bash", "../../hack/fetch-tool.sh",
			"http://127.0.0.1:9/tool", "http://127.0.0.1:9/tool.sha256", dest)
		out, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), string(out))
		Expect(exitErr.ExitCode()).To(Equal(curlUnsupportedProtocol), string(out))
		Expect(string(out)).To(ContainSubstring(`Protocol "http"`))
		Expect(dest).NotTo(BeAnExistingFile())
	})
})
