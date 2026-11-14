package clusterconfig

import (
	"fmt"
	"slices"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

const (
	placeholderEndpoint = "https://placeholder.invalid:4318"
	placeholderUser     = "placeholder"
	endpointKey         = envPrefix + "ENDPOINT"
	monitorRole         = "pg_monitor"
	loopbackV4          = "127.0.0.1/32"
	loopbackV6          = "::1/128"
)

// Check validates what the plugin can see without reading Secrets: Build with placeholder envFrom data
// and username, an endpoint taken as given when envFrom is set.
func Check(c *apiv1.Cluster, p podpatch.Params, e *v1alpha1.TelemetryExporter) error {
	in := Inputs{Cluster: c, Params: p, Exporter: e}
	if p.Auth == podpatch.AuthPassword {
		in.PasswordUsername = placeholderUser
		if in.Params.PasswordSecret == "" {
			in.Params.PasswordSecret = placeholderUser
		}
	}
	if e != nil && e.Spec.EnvFrom != nil {
		in.EnvConfigMap = map[string]string{endpointKey: placeholderEndpoint}
	}

	if _, _, err := Build(in); err != nil {
		return fmt.Errorf("check exporter: %w", err)
	}

	return nil
}

// SpecProblems lists what the Cluster spec lacks for the connection mode, sorted.
func SpecProblems(c *apiv1.Cluster, p podpatch.Params) []string {
	var problems []string
	switch p.Auth {
	case podpatch.AuthPassword:
		problems = passwordProblems(c, p)
	case podpatch.AuthPeer:
		if !hasIdentLine(c) {
			problems = append(problems, fmt.Sprintf("peer mode needs the pg_ident line %q",
				metadata.MetricsExporterRole+" nobody "+metadata.MetricsExporterRole))
		}
	}
	slices.Sort(problems)

	return problems
}

func hasIdentLine(c *apiv1.Cluster) bool {
	want := []string{metadata.MetricsExporterRole, "nobody", metadata.MetricsExporterRole}
	for _, line := range c.Spec.PostgresConfiguration.PgIdent {
		if slices.Equal(fields(line), want) {
			return true
		}
	}

	return false
}

func passwordProblems(c *apiv1.Cluster, p podpatch.Params) []string {
	if p.PasswordSecret == "" {
		return []string{"password mode needs the passwordSecret parameter"}
	}

	role, ok := monitoringRole(c, p.PasswordSecret)
	if !ok {
		return []string{fmt.Sprintf("no managed role uses passwordSecret %q with login and %s", p.PasswordSecret, monitorRole)}
	}

	lines := c.Spec.PostgresConfiguration.PgHBA
	var problems []string
	last := -1
	for _, addr := range []string{loopbackV4, loopbackV6} {
		i := hbaIndex(lines, role, addr, "", 0)
		if i < 0 {
			problems = append(problems, fmt.Sprintf("pg_hba has no line for role %q from %s", role, addr))
		}
		last = max(last, i)
	}
	if hbaIndex(lines, role, "all", "reject", last+1) < 0 {
		problems = append(problems, fmt.Sprintf("pg_hba has no reject line for role %q", role))
	}

	return problems
}

// monitoringRole returns the first managed role that is present, can log in, is in pg_monitor and uses
// the Secret.
func monitoringRole(c *apiv1.Cluster, secret string) (string, bool) {
	if c.Spec.Managed == nil {
		return "", false
	}
	for _, r := range c.Spec.Managed.Roles {
		if r.PasswordSecret != nil && r.PasswordSecret.Name == secret &&
			(r.Ensure == "" || r.Ensure == apiv1.EnsurePresent) && r.Login && slices.Contains(r.InRoles, monitorRole) {
			return r.Name, true
		}
	}

	return "", false
}

// hbaIndex returns the index, from start on, of the first host* line for role with the address and, when
// method is not empty, the method; -1 when there is none.
func hbaIndex(lines []string, role, addr, method string, start int) int {
	for i := start; i < len(lines); i++ {
		f := fields(lines[i])
		if len(f) < 5 || !strings.HasPrefix(f[0], "host") || !slices.Contains(strings.Split(f[2], ","), role) || f[3] != addr {
			continue
		}
		if method == "" || f[4] == method {
			return i
		}
	}

	return -1
}

// fields splits a pg_hba or pg_ident line on whitespace, dropping a trailing comment.
func fields(line string) []string {
	line, _, _ = strings.Cut(line, "#")

	return strings.Fields(line)
}
