package operator_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

type fakeWaiter struct {
	ready chan struct{}
}

func (*fakeWaiter) Image() string { return "registry.example/sidecar:1" }

func (f *fakeWaiter) Wait(ctx context.Context) error {
	select {
	case <-f.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type testPKI struct {
	dir        string
	serverCert string
	serverKey  string
	clientCert string
	clientPair tls.Certificate
	serverPool *x509.CertPool
}

func writePEM(path, blockType string, der []byte) {
	GinkgoHelper()
	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	Expect(os.WriteFile(path, data, 0o600)).To(Succeed())
}

func selfSigned(serial int64, usage x509.ExtKeyUsage, dnsNames []string) (*ecdsa.PrivateKey, []byte) {
	GinkgoHelper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "plugin-telemetry-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
		DNSNames:              dnsNames,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())

	return key, der
}

func newTestPKI() *testPKI {
	GinkgoHelper()
	p := &testPKI{dir: GinkgoT().TempDir()}

	serverKey, serverDER := selfSigned(1, x509.ExtKeyUsageServerAuth, []string{"plugin-telemetry"})
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	Expect(err).NotTo(HaveOccurred())
	p.serverCert = filepath.Join(p.dir, "server.crt")
	p.serverKey = filepath.Join(p.dir, "server.key")
	writePEM(p.serverCert, "CERTIFICATE", serverDER)
	writePEM(p.serverKey, "EC PRIVATE KEY", serverKeyDER)
	serverParsed, err := x509.ParseCertificate(serverDER)
	Expect(err).NotTo(HaveOccurred())
	p.serverPool = x509.NewCertPool()
	p.serverPool.AddCert(serverParsed)

	clientKey, clientDER := selfSigned(2, x509.ExtKeyUsageClientAuth, nil)
	p.clientCert = filepath.Join(p.dir, "client.crt")
	writePEM(p.clientCert, "CERTIFICATE", clientDER)
	p.clientPair = tls.Certificate{Certificate: [][]byte{clientDER}, PrivateKey: clientKey}

	return p
}

func freeAddress() string {
	GinkgoHelper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	addr := l.Addr().String()
	Expect(l.Close()).To(Succeed())

	return addr
}

func canDial(addr string) bool {
	d := &net.Dialer{Timeout: 100 * time.Millisecond}
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return false
	}
	Expect(conn.Close()).To(Succeed())

	return true
}

var _ = Describe("Run", func() {
	var (
		pki    *testPKI
		addr   string
		waiter *fakeWaiter
		cancel context.CancelFunc
		done   chan error
	)

	start := func() {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		done = make(chan error, 1)
		go func() {
			done <- operator.Run(ctx, operator.Options{
				ServerAddress:  addr,
				ServerCertPath: pki.serverCert,
				ServerKeyPath:  pki.serverKey,
				ClientCAPath:   pki.clientCert,
			}, waiter)
		}()
	}

	dial := func(cfg *tls.Config) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	}

	goodTLS := func() *tls.Config {
		return &tls.Config{
			Certificates: []tls.Certificate{pki.clientPair},
			RootCAs:      pki.serverPool,
			ServerName:   "plugin-telemetry",
			MinVersion:   tls.VersionTLS13,
		}
	}

	BeforeEach(func() {
		cancel, done = nil, nil
		pki = newTestPKI()
		addr = freeAddress()
		waiter = &fakeWaiter{ready: make(chan struct{})}
	})

	AfterEach(func() {
		if cancel == nil {
			return
		}
		cancel()
		Eventually(done, 5*time.Second).Should(Receive())
	})

	It("does not listen before the sidecar image is known", func() {
		start()
		Consistently(func() bool { return canDial(addr) }, 500*time.Millisecond, 50*time.Millisecond).Should(BeFalse())
		close(waiter.ready)
		Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
	})

	Context("once the image is known", func() {
		BeforeEach(func() {
			close(waiter.ready)
			start()
			Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		})

		It("serves identity over mTLS", func() {
			conn, err := dial(goodTLS())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			resp, err := identity.NewIdentityClient(conn).GetPluginMetadata(ctx, &identity.GetPluginMetadataRequest{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.GetName()).To(Equal(metadata.PluginName))
		})

		It("serves the lifecycle service", func() {
			conn, err := dial(goodTLS())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			resp, err := lifecycle.NewOperatorLifecycleClient(conn).
				GetCapabilities(ctx, &lifecycle.OperatorLifecycleCapabilitiesRequest{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.GetLifecycleCapabilities()).To(HaveLen(1))
			Expect(resp.GetLifecycleCapabilities()[0].GetKind()).To(Equal("Pod"))
		})

		It("rejects a client without a certificate", func() {
			cfg := goodTLS()
			cfg.Certificates = nil
			conn, err := dial(cfg)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_, err = identity.NewIdentityClient(conn).GetPluginMetadata(ctx, &identity.GetPluginMetadataRequest{})
			Expect(err).To(HaveOccurred())
		})

		It("rejects TLS 1.2", func() {
			cfg := goodTLS()
			cfg.MinVersion = tls.VersionTLS12
			cfg.MaxVersion = tls.VersionTLS12
			conn, err := dial(cfg)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_, err = identity.NewIdentityClient(conn).GetPluginMetadata(ctx, &identity.GetPluginMetadataRequest{})
			Expect(err).To(HaveOccurred())
		})
	})

	It("stops when the context ends", func() {
		close(waiter.ready)
		start()
		Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		cancel()
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))
		done <- nil
	})

	It("returns nil when the context ends before the image is known", func() {
		start()
		cancel()
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))
		done <- nil
	})

	DescribeTable("refuses to start without TLS material",
		func(mutate func(*operator.Options)) {
			opts := operator.Options{
				ServerAddress:  addr,
				ServerCertPath: pki.serverCert,
				ServerKeyPath:  pki.serverKey,
				ClientCAPath:   pki.clientCert,
			}
			mutate(&opts)
			close(waiter.ready)
			Expect(operator.Run(context.Background(), opts, waiter)).To(MatchError(operator.ErrInvalidOptions))
			Consistently(func() bool { return canDial(addr) }, 200*time.Millisecond, 50*time.Millisecond).Should(BeFalse())
		},
		Entry("empty server address", func(o *operator.Options) { o.ServerAddress = "" }),
		Entry("empty server cert", func(o *operator.Options) { o.ServerCertPath = "" }),
		Entry("empty server key", func(o *operator.Options) { o.ServerKeyPath = "" }),
		Entry("empty client CA", func(o *operator.Options) { o.ClientCAPath = "" }),
	)
})
