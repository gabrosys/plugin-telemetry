package operator_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	cnpgoperator "github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// blockingWaiter is ready at once; Image signals entered, then blocks until release is closed.
type blockingWaiter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingWaiter() *blockingWaiter {
	return &blockingWaiter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingWaiter) Image() string {
	b.once.Do(func() { close(b.entered) })
	<-b.release

	return "registry.example/sidecar:1"
}

func (*blockingWaiter) Wait(context.Context) error { return nil }

type fakeCache struct {
	cache.Cache
	synced chan struct{}

	mu         sync.Mutex // guards registered
	registered []client.Object
}

func (f *fakeCache) GetInformer(_ context.Context, obj client.Object, _ ...cache.InformerGetOption) (cache.Informer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = append(f.registered, obj)

	return nil, nil
}

func (f *fakeCache) kinds() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, o := range f.registered {
		out = append(out, fmt.Sprintf("%T", o))
	}

	return out
}

func (f *fakeCache) WaitForCacheSync(ctx context.Context) bool {
	select {
	case <-f.synced:
		return true
	case <-ctx.Done():
		return false
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
		images operator.ImageWaiter
		caches *fakeCache
		cancel context.CancelFunc
		done   chan error
	)

	deps := func() operator.Deps {
		return operator.Deps{Cache: caches, Status: &operator.Status{}}
	}

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
			}, images, deps())
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
		images = waiter
		caches = &fakeCache{synced: make(chan struct{})}
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
		close(caches.synced)
		Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
	})

	It("registers an informer for all seven cached kinds before it listens", func() {
		close(waiter.ready)
		start()
		Eventually(caches.kinds, 5*time.Second).Should(ConsistOf(
			"*v1.Cluster", "*v1alpha1.TelemetryExporter",
			"*v1.ServiceAccount", "*v1.Role", "*v1.RoleBinding", "*v1.Secret", "*v1.ConfigMap",
		))
		Expect(canDial(addr)).To(BeFalse())
	})

	It("does not listen before the caches have synced", func() {
		close(waiter.ready)
		start()
		Consistently(func() bool { return canDial(addr) }, 500*time.Millisecond, 50*time.Millisecond).Should(BeFalse())
		close(caches.synced)
		Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
	})

	Context("once the image is known", func() {
		BeforeEach(func() {
			close(waiter.ready)
			close(caches.synced)
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

		It("answers Unimplemented to ReconcilerHooks.GetCapabilities", func() {
			conn, err := dial(goodTLS())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_, err = reconciler.NewReconcilerHooksClient(conn).
				GetCapabilities(ctx, &reconciler.ReconcilerHooksCapabilitiesRequest{})
			Expect(status.Code(err)).To(Equal(codes.Unimplemented))
		})

		It("serves Operator.GetCapabilities with only RPC_TYPE_SET_STATUS_IN_CLUSTER", func() {
			conn, err := dial(goodTLS())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			resp, err := cnpgoperator.NewOperatorClient(conn).GetCapabilities(ctx, &cnpgoperator.OperatorCapabilitiesRequest{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.GetCapabilities()).To(HaveLen(1))
			Expect(resp.GetCapabilities()[0].GetRpc().GetType()).
				To(Equal(cnpgoperator.OperatorCapability_RPC_TYPE_SET_STATUS_IN_CLUSTER))
		})

		It("leaves the cluster status unchanged", func() {
			conn, err := dial(goodTLS())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			resp, err := cnpgoperator.NewOperatorClient(conn).SetStatusInCluster(ctx, &cnpgoperator.SetStatusInClusterRequest{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.GetJsonStatus()).To(BeEmpty())
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

		It("rejects a client whose certificate the CA does not trust", func() {
			otherKey, otherDER := selfSigned(3, x509.ExtKeyUsageClientAuth, nil)
			cfg := goodTLS()
			cfg.Certificates = []tls.Certificate{{Certificate: [][]byte{otherDER}, PrivateKey: otherKey}}
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

	Context("with an EVALUATE call in flight", func() {
		var (
			blocking *blockingWaiter
			release  func()
			result   chan error
			patch    chan []byte
		)

		BeforeEach(func() {
			blocking = newBlockingWaiter()
			images = blocking
			release = sync.OnceFunc(func() { close(blocking.release) })
			DeferCleanup(release)
			close(caches.synced)
			start()
			Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())

			conn, err := dial(goodTLS())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			result = make(chan error, 1)
			patch = make(chan []byte, 1)
			go func() {
				ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
				defer c()
				resp, err := lifecycle.NewOperatorLifecycleClient(conn).LifecycleHook(ctx,
					request(lifecycle.OperatorOperationType_TYPE_EVALUATE, clusterJSON(ours(nil)), fixturePod("pod-main")))
				patch <- resp.GetJsonPatch()
				result <- err
			}()
			Eventually(blocking.entered, 5*time.Second).Should(BeClosed())
		})

		It("lets a call in flight finish after its context ends", func() {
			cancel()
			By("accepts no new connection once its context ends")
			Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeFalse())
			By("finishing the call in flight after the dial is refused")
			release()
			Eventually(result, 5*time.Second).Should(Receive(BeNil()))
			Expect(<-patch).NotTo(BeEmpty())
			Eventually(done, 5*time.Second).Should(Receive(BeNil()))
			done <- nil
		})

		It("ends a call still running when the drain timeout has passed", func() {
			DeferCleanup(operator.SetDrainTimeout(200 * time.Millisecond))
			cancel()
			var err error
			Eventually(result, 2*time.Second).Should(Receive(&err))
			Expect(status.Code(err)).To(Equal(codes.Unavailable))
			Eventually(done, 2*time.Second).Should(Receive(BeNil()))
			done <- nil
		})
	})

	It("stops when the context ends", func() {
		close(waiter.ready)
		close(caches.synced)
		start()
		Eventually(func() bool { return canDial(addr) }, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		cancel()
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))
		done <- nil
	})

	It("logs that it waits for the image and that it serves once loaded", func() {
		var mu sync.Mutex // guards msgs
		var msgs []string
		sink := funcr.NewJSON(func(obj string) {
			var entry struct {
				Msg string `json:"msg"`
			}
			mu.Lock()
			defer mu.Unlock()
			if err := json.Unmarshal([]byte(obj), &entry); err != nil {
				msgs = append(msgs, "undecodable log line: "+err.Error())

				return
			}
			msgs = append(msgs, entry.Msg)
		}, funcr.Options{})
		logged := func() []string {
			mu.Lock()
			defer mu.Unlock()

			return append([]string(nil), msgs...)
		}

		ctx, c := context.WithCancel(logr.NewContext(context.Background(), sink))
		cancel = c
		done = make(chan error, 1)
		go func() {
			done <- operator.Run(ctx, operator.Options{
				ServerAddress:  addr,
				ServerCertPath: pki.serverCert,
				ServerKeyPath:  pki.serverKey,
				ClientCAPath:   pki.clientCert,
			}, waiter, deps())
		}()

		Eventually(logged, 5*time.Second).Should(Equal([]string{"waiting for the sidecar image"}))
		close(waiter.ready)
		close(caches.synced)
		Eventually(logged, 5*time.Second).Should(ContainElement("sidecar image loaded, serving"))
	})

	It("returns nil when the context ends before the image is known", func() {
		start()
		cancel()
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))
		done <- nil
	})
})
