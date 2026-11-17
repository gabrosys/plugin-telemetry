package framework

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// ErrNoCMCTL is returned when RenewCertificates has no cmctl binary.
var ErrNoCMCTL = errors.New("cmctl is not set")

// RenewCertificates marks the named cert-manager Certificates for renewal with cmctl.
func RenewCertificates(ctx context.Context, cmctl, kubeContext, namespace string, names ...string) error {
	if cmctl == "" {
		return fmt.Errorf("%w: set CMCTL to the cmctl binary", ErrNoCMCTL)
	}

	args := append([]string{"renew", "--context", kubeContext, "--namespace", namespace}, names...)
	out, err := exec.CommandContext(ctx, cmctl, args...).CombinedOutput() //nolint:gosec // the test environment names the cmctl binary
	if err != nil {
		return fmt.Errorf("cmctl renew %s: %w: %s", strings.Join(names, " "), err, strings.TrimSpace(string(out)))
	}

	return nil
}
