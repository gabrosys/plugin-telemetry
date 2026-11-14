package export_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/export/otlptest"
)

func TestExport(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Export Suite")
}

// keyPair is a PEM certificate and key.
type keyPair struct{ cert, key []byte }

// pki holds the test certificates: caA signs the server and both client certificates; caB signs another server.
var pki struct {
	caA, caB         *otlptest.CA
	client1, client2 keyPair
}

var _ = BeforeSuite(func() {
	var err error
	pki.caA, err = otlptest.NewCA("ca-a")
	Expect(err).NotTo(HaveOccurred())
	pki.caB, err = otlptest.NewCA("ca-b")
	Expect(err).NotTo(HaveOccurred())
	pki.client1.cert, pki.client1.key, err = pki.caA.Issue("client-1")
	Expect(err).NotTo(HaveOccurred())
	pki.client2.cert, pki.client2.key, err = pki.caA.Issue("client-2")
	Expect(err).NotTo(HaveOccurred())
})
