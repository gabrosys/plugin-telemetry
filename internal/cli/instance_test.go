package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/cli"
)

var _ = Describe("instance command", func() {
	It("returns nil when its context is cancelled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"instance"})

		done := make(chan error, 1)
		go func() { done <- cmd.ExecuteContext(ctx) }()

		Consistently(done, 100*time.Millisecond).ShouldNot(Receive())
		cancel()
		Eventually(done, time.Second).Should(Receive(BeNil()))
	})
})

var _ = Describe("instance preflight command", func() {
	It("prints one JSON line with the postgres and kube parts when neither can be reached", func() {
		GinkgoT().Setenv("KUBERNETES_SERVICE_HOST", "")
		var out bytes.Buffer
		cmd := cli.NewRootCommand()
		cmd.SetOut(&out)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"instance", "preflight", "--pg-host=127.0.0.1", "--pg-port=1", "--password-secret=s"})
		Expect(cmd.Execute()).To(Succeed())

		var report map[string]any
		Expect(json.Unmarshal(out.Bytes(), &report)).To(Succeed())
		Expect(report).To(HaveKey("postgres"))
		Expect(report).To(HaveKey("kube"))
		Expect(strings.Count(out.String(), "\n")).To(Equal(1))
	})

	It("clears PG* before it connects", func() {
		GinkgoT().Setenv("PGAPPNAME", "x")
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"instance", "preflight", "--kube=false", "--pg-host=127.0.0.1", "--pg-port=1"})
		Expect(cmd.Execute()).To(Succeed())

		_, set := os.LookupEnv("PGAPPNAME")
		Expect(set).To(BeFalse())
	})

	It("does not print PGPASSWORD", func() {
		GinkgoT().Setenv("PGPASSWORD", "hunter2")
		var out bytes.Buffer
		cmd := cli.NewRootCommand()
		cmd.SetOut(&out)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"instance", "preflight", "--kube=false", "--pg-host=127.0.0.1", "--pg-port=1"})
		Expect(cmd.Execute()).To(Succeed())

		Expect(out.String()).NotTo(ContainSubstring("PGPASSWORD"))
		Expect(out.String()).NotTo(ContainSubstring("hunter2"))
		Expect(out.String()).To(ContainSubstring(`"connected":false`))
	})

	It("rejects unknown flags", func() {
		cmd := cli.NewRootCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"instance", "preflight", "--nope"})
		Expect(cmd.Execute()).To(HaveOccurred())
	})
})
