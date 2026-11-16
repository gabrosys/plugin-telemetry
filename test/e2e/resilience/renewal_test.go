//go:build e2e

package resilience_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	pluginNamespace = "cnpg-system"
	caCert          = "plugin-telemetry-ca"
	serverCert      = "plugin-telemetry-server"
	clientCert      = "plugin-telemetry-client"
	renewTimeout    = 2 * time.Minute
	afterRenewal    = 150 * time.Second
)

var errNoPEM = errors.New("tls.crt has no PEM block")

// leaf is the identity of a certificate Secret's tls.crt.
type leaf struct {
	Serial string
	PubKey []byte
}

// readLeaf parses the tls.crt of the Secret that cert-manager keeps for the Certificate named name.
func readLeaf(ctx context.Context, c client.Client, name string) (leaf, error) {
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: pluginNamespace, Name: name + "-tls"}, &secret); err != nil {
		return leaf{}, fmt.Errorf("get secret %s-tls: %w", name, err)
	}
	block, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	if block == nil {
		return leaf{}, fmt.Errorf("secret %s-tls: %w", name, errNoPEM)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return leaf{}, fmt.Errorf("parse tls.crt of %s-tls: %w", name, err)
	}

	return leaf{Serial: cert.SerialNumber.String(), PubKey: cert.RawSubjectPublicKeyInfo}, nil
}

// renewalSpecs adds PLG-02: the renewal of the plugin's CA and leaf certificates under traffic.
func renewalSpecs(s *suite) {
	It("PLG-02: renewing the CA, server and client certificates causes no failed hook call", func(ctx SpecContext) {
		s.healthy(ctx)

		names := []string{caCert, serverCert, clientCert}
		before := map[string]leaf{}
		for _, name := range names {
			l, err := readLeaf(ctx, s.c, name)
			Expect(err).NotTo(HaveOccurred())
			before[name] = l
		}

		start := time.Now()
		stop := s.traffic(ctx)
		defer stop()

		Expect(framework.RenewCertificates(ctx, s.env.CMCTL, s.env.KubeContext, pluginNamespace, names...)).To(Succeed())
		for _, name := range names {
			Eventually(readLeaf).WithContext(ctx).WithArguments(s.c, name).
				WithTimeout(renewTimeout).WithPolling(statePoll).
				Should(HaveField("Serial", Not(Equal(before[name].Serial))), "%s was not renewed", name)
		}
		renewed := time.Now()

		Expect(framework.WaitUntil(ctx, renewed.Add(afterRenewal))).To(Succeed())
		stop()

		s.expectNoFailedHookCall(ctx, "PLG-02", start)
		for _, name := range names {
			after, err := readLeaf(ctx, s.c, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(after.PubKey).To(Equal(before[name].PubKey), "%s changed its public key", name)
		}
		s.expectEvaluatedSince(ctx, renewed)
	}, SpecTimeout(8*time.Minute))
}
