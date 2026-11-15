package preflight_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/preflight"
)

var _ = Describe("PodTokenPresent", func() {
	It("is true for a dir holding a token", func() {
		dir := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(dir, "token"), []byte("x"), 0o600)).To(Succeed())
		Expect(preflight.PodTokenPresent(dir)).To(BeTrue())
	})

	It("is false for an empty dir", func() {
		Expect(preflight.PodTokenPresent(GinkgoT().TempDir())).To(BeFalse())
	})

	It("is false for a missing dir", func() {
		Expect(preflight.PodTokenPresent(filepath.Join(GinkgoT().TempDir(), "nope"))).To(BeFalse())
	})
})

var _ = Describe("SQLState", func() {
	It("extracts a wrapped PgError", func() {
		err := fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28000", Severity: "FATAL", Message: "m"})
		code, msg := preflight.SQLState(err)
		Expect(code).To(Equal("28000"))
		Expect(msg).To(Equal("FATAL: m"))
	})

	It("returns the text of a plain error", func() {
		code, msg := preflight.SQLState(errors.New("boom"))
		Expect(code).To(BeEmpty())
		Expect(msg).To(Equal("boom"))
	})
})

var _ = Describe("Report JSON", func() {
	It("uses exactly the contract keys", func() {
		r := preflight.Report{
			Kube: &preflight.KubeReport{Error: "e"},
			Postgres: &preflight.PostgresReport{
				CurrentUser: "c", SQLState: "s", Error: "e",
			},
		}
		raw, err := json.Marshal(r)
		Expect(err).NotTo(HaveOccurred())
		var m map[string]any
		Expect(json.Unmarshal(raw, &m)).To(Succeed())
		Expect(m).To(HaveLen(4))
		Expect(m).To(HaveKey("uid"))
		Expect(m).To(HaveKey("podTokenPresent"))
		kube, ok := m["kube"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(kube).To(HaveLen(3))
		for _, k := range []string{"username", "clusterReadable", "error"} {
			Expect(kube).To(HaveKey(k))
		}
		pg, ok := m["postgres"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(pg).To(HaveLen(8))
		for _, k := range []string{"host", "user", "connected", "currentUser", "walFiles", "archiveStatusFiles", "sqlState", "error"} {
			Expect(pg).To(HaveKey(k))
		}
	})
})

var _ = Describe("Run", func() {
	It("reports the host and user when the connection is refused", func(ctx SpecContext) {
		rep := preflight.Run(ctx, preflight.Config{
			PodTokenDir: GinkgoT().TempDir(),
			Postgres:    preflight.PostgresConfig{Host: "127.0.0.1", Port: 1, User: "u", Database: "postgres", Password: "secret"},
		})
		Expect(rep.Postgres).NotTo(BeNil())
		Expect(rep.Postgres.Host).To(Equal("127.0.0.1"))
		Expect(rep.Postgres.User).To(Equal("u"))
		Expect(rep.Postgres.Connected).To(BeFalse())
		Expect(rep.Postgres.Error).To(ContainSubstring("connect"))
		Expect(rep.Postgres.Error).NotTo(ContainSubstring("secret"))
	})

	It("refuses to connect while a PG variable is set", func(ctx SpecContext) {
		GinkgoT().Setenv("PGAPPNAME", "x")
		rep := preflight.Run(ctx, preflight.Config{
			PodTokenDir: GinkgoT().TempDir(),
			Postgres:    preflight.PostgresConfig{Host: "127.0.0.1", Port: 1, User: "u", Database: "postgres"},
		})
		Expect(rep.Postgres.Connected).To(BeFalse())
		Expect(rep.Postgres.Error).To(ContainSubstring("PGAPPNAME"))
	})

	It("reports an empty token directory in kube.error", func(ctx SpecContext) {
		rep := preflight.Run(ctx, preflight.Config{
			PodTokenDir: GinkgoT().TempDir(),
			Kube:        true,
			APIServer:   "https://127.0.0.1:1",
			Postgres:    preflight.PostgresConfig{Host: "127.0.0.1", Port: 1, User: "u", Database: "postgres"},
		})
		Expect(rep.Kube).NotTo(BeNil())
		Expect(rep.Kube.Error).NotTo(BeEmpty())
		Expect(rep.Kube.ClusterReadable).To(BeFalse())
	})
})
