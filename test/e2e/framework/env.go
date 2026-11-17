// Package framework provides helpers shared by the e2e suites.
package framework

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalidEnv is returned when an e2e environment variable is missing or invalid.
var ErrInvalidEnv = errors.New("invalid e2e environment")

const kindContextPrefix = "kind-"

var cnpgVersionPattern = regexp.MustCompile(`^(main|v\d+\.\d+\.\d+)$`)

// Env is the e2e configuration read from the environment.
type Env struct {
	KubeContext   string
	CNPGVersion   string
	PostgresImage string
	CMCTL         string // path of the cmctl binary; empty when unset
}

// LoadEnv reads E2E_KUBE_CONTEXT, which must name a kind context, CNPG_VERSION, POSTGRES_IMAGE and CMCTL.
func LoadEnv(getenv func(string) string) (Env, error) {
	env := Env{
		KubeContext:   getenv("E2E_KUBE_CONTEXT"),
		CNPGVersion:   getenv("CNPG_VERSION"),
		PostgresImage: getenv("POSTGRES_IMAGE"),
		CMCTL:         getenv("CMCTL"),
	}

	switch {
	case !strings.HasPrefix(env.KubeContext, kindContextPrefix) || env.KubeContext == kindContextPrefix:
		return Env{}, fmt.Errorf("%w: E2E_KUBE_CONTEXT must be a kind context (kind-<cluster>), got %q",
			ErrInvalidEnv, env.KubeContext)
	case !cnpgVersionPattern.MatchString(env.CNPGVersion):
		return Env{}, fmt.Errorf("%w: CNPG_VERSION must be main or vX.Y.Z, got %q", ErrInvalidEnv, env.CNPGVersion)
	case env.PostgresImage == "":
		return Env{}, fmt.Errorf("%w: POSTGRES_IMAGE is required", ErrInvalidEnv)
	}

	return env, nil
}

// MatchesCNPGVersion reports whether the operator image tag is the one CNPG
// publishes for cnpgVersion: "main" for main, "X.Y.Z" for vX.Y.Z.
func MatchesCNPGVersion(operatorImage, cnpgVersion string) bool {
	ref, _, _ := strings.Cut(operatorImage, "@")
	i := strings.LastIndex(ref, ":")
	if i < 0 || strings.Contains(ref[i:], "/") {
		return false
	}
	tag := ref[i+1:]

	if cnpgVersion == "main" {
		return tag == "main"
	}

	return strings.HasPrefix(cnpgVersion, "v") && tag == strings.TrimPrefix(cnpgVersion, "v")
}
