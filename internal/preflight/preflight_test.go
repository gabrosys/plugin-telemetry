package preflight_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/preflight"
)

func jwt(payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(payload)) + ".sig"
}

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

var _ = Describe("TokenExpiry", func() {
	It("reads the exp claim", func() {
		exp, ok := preflight.TokenExpiry(jwt(`{"exp":1900000000}`))
		Expect(ok).To(BeTrue())
		Expect(exp.UTC().Format(time.RFC3339)).To(Equal("2030-03-17T17:46:40Z"))
	})

	It("reports false without exp", func() {
		_, ok := preflight.TokenExpiry(jwt(`{"sub":"a"}`))
		Expect(ok).To(BeFalse())
	})

	It("reports false for a malformed token", func() {
		for _, tok := range []string{"", "abc", "a.b.c", "a.!!!.c"} {
			_, ok := preflight.TokenExpiry(tok)
			Expect(ok).To(BeFalse(), tok)
		}
	})
})

var _ = Describe("ConnConfig", func() {
	It("uses a socket directory without TLS", func() {
		cc, err := preflight.ConnConfig(preflight.PostgresConfig{Host: "/controller/run", Port: 5432, User: "u", Database: "postgres"})
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Config.Host).To(Equal("/controller/run"))
		Expect(cc.Config.TLSConfig).To(BeNil())
	})

	It("uses TCP for an address", func() {
		cc, err := preflight.ConnConfig(preflight.PostgresConfig{Host: "127.0.0.1", Port: 5432, User: "u", Database: "postgres"})
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Config.Host).To(Equal("127.0.0.1"))
		Expect(cc.Config.Port).To(Equal(uint16(5432)))
	})

	It("sets runtime params and timeout", func() {
		cc, err := preflight.ConnConfig(preflight.PostgresConfig{Host: "127.0.0.1", Port: 5432, User: "u", Database: "postgres"})
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Config.RuntimeParams).To(HaveKeyWithValue("default_transaction_read_only", "on"))
		Expect(cc.Config.RuntimeParams).To(HaveKeyWithValue("application_name", "cnpg_otel_preflight"))
		Expect(cc.Config.ConnectTimeout).To(Equal(5 * time.Second))
	})

	It("uses the password and quotes odd values", func() {
		cc, err := preflight.ConnConfig(preflight.PostgresConfig{Host: "127.0.0.1", Port: 5432, User: "u v", Database: "postgres", Password: `p'a \ss`})
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Config.Password).To(Equal(`p'a \ss`))
		Expect(cc.Config.User).To(Equal("u v"))
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
			Groups: []int{1},
			Kube:   &preflight.KubeReport{TokenExpiresAt: "t", Error: "e"},
			Postgres: &preflight.PostgresReport{
				CurrentUser: "c", SQLState: "s", Error: "e",
			},
		}
		raw, err := json.Marshal(r)
		Expect(err).NotTo(HaveOccurred())
		var m map[string]any
		Expect(json.Unmarshal(raw, &m)).To(Succeed())
		Expect(m).To(HaveLen(6))
		Expect(m).To(HaveKey("uid"))
		Expect(m).To(HaveKey("gid"))
		Expect(m).To(HaveKey("groups"))
		Expect(m).To(HaveKey("podTokenPresent"))
		kube, ok := m["kube"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(kube).To(HaveLen(4))
		for _, k := range []string{"username", "clusterReadable", "tokenExpiresAt", "error"} {
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
