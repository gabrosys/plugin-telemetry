package cli_test

import (
	"bytes"
	"fmt"
	"io"
	"runtime"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/cli"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

var _ = Describe("root command", func() {
	It("prints the build version, Go version and platform", func() {
		out := new(bytes.Buffer)
		cmd := cli.NewRootCommand()
		cmd.SetOut(out)
		cmd.SetArgs([]string{"version"})
		Expect(cmd.Execute()).To(Succeed())
		Expect(out.String()).To(Equal(fmt.Sprintf("plugin-telemetry %s %s %s/%s\n",
			version.Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)))
	})

	It("rejects an unknown subcommand", func() {
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"bogus"})
		Expect(cmd.Execute()).To(MatchError(ContainSubstring(`unknown command "bogus"`)))
	})
})
