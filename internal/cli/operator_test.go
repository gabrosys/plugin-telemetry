package cli_test

import (
	"bytes"
	"io"
	"strings"

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

	It("rejects an invalid --metrics-endpoint at start", func() {
		GinkgoT().Setenv("POD_NAMESPACE", "ns")
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{
			"operator",
			"--server-address=127.0.0.1:0",
			"--server-cert=a", "--server-key=b", "--client-cert=c",
			"--sidecar-image-configmap=cm",
			"--metrics-endpoint=ftp://collector",
		})
		Expect(cmd.Execute()).To(MatchError(ContainSubstring("exporter.endpoint: must be an http or https URL with a host")))
	})

	It("names the namespace default once in its help", func() {
		GinkgoT().Setenv("POD_NAMESPACE", "cnpg-system")
		var out bytes.Buffer
		cmd := cli.NewRootCommand()
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"operator", "--help"})
		Expect(cmd.Execute()).To(Succeed())

		var line string
		for _, l := range strings.Split(out.String(), "\n") {
			if strings.Contains(l, "--namespace") {
				line = l
			}
		}
		Expect(line).NotTo(BeEmpty())
		Expect(strings.Count(line, "cnpg-system")).To(Equal(1))
		Expect(strings.Count(line, "(default")).To(Equal(1))
	})
})
