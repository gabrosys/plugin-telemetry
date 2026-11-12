package devenv_test

import (
	"os/exec"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("fetch-tool.sh", func() {
	It("refuses plain HTTP downloads", func(ctx SpecContext) {
		dest := filepath.Join(GinkgoT().TempDir(), "tool")
		//nolint:gosec // G204: constant arguments plus a test temp dir
		cmd := exec.CommandContext(ctx, "bash", "../../hack/fetch-tool.sh",
			"http://127.0.0.1:9/tool", "http://127.0.0.1:9/tool.sha256", dest)
		out, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(out)).To(ContainSubstring(`Protocol "http" disabled`))
		Expect(dest).NotTo(BeAnExistingFile())
	})
})
