package cli_test

import (
	"io"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/cli"
)

var _ = Describe("operator command", func() {
	It("requires its flags", func() {
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"operator"})
		Expect(cmd.Execute()).To(MatchError(ContainSubstring("server-address")))
	})

	It("fails when the namespace is empty", func() {
		GinkgoT().Setenv("POD_NAMESPACE", "")
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{
			"operator",
			"--server-address=127.0.0.1:0",
			"--server-cert=a", "--server-key=b", "--client-cert=c",
			"--sidecar-image-configmap=cm",
		})
		Expect(cmd.Execute()).To(MatchError(ContainSubstring("namespace")))
	})
})
