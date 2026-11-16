package clusterconfig

import (
	"fmt"
	"net/netip"
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
	if e.Spec.EnvFrom != nil {
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
	for _, addr := range []string{loopbackV4, loopbackV6} {
		a := netip.MustParsePrefix(addr).Addr()
		if !hasHostLine(lines, role, func(f []string) bool { return couldCover(f[3], a) && !isReject(f) }) {
			problems = append(problems, fmt.Sprintf("pg_hba has no line for role %q from %s", role, addr))
		}
	}
	if !hasHostLine(lines, role, isReject) {
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

// hasHostLine reports whether a host* line with at least five fields could match role and satisfies ok.
// It is advisory and only says no when sure: user forms (all, +group, /regex, @file, quoted) and address
// forms other than a CIDR (all, samehost, samenet, host names, IP plus mask) count as possible matches.
// Line order and the database field are not judged.
func hasHostLine(lines []string, role string, ok func(f []string) bool) bool {
	for _, line := range lines {
		f := fields(line)
		if len(f) >= 5 && strings.HasPrefix(f[0], "host") && couldMatchUser(f[2], role) && ok(f) {
			return true
		}
	}

	return false
}

// isReject reports whether the method of a host* line is reject, after an address or an address and mask.
func isReject(f []string) bool { return slices.Contains(f[4:], "reject") }

// couldMatchUser reports whether a pg_hba user field could include role.
func couldMatchUser(field, role string) bool {
	for item := range strings.SplitSeq(field, ",") {
		if item == role || item == "all" || strings.HasPrefix(item, "+") || strings.HasPrefix(item, "/") ||
			strings.HasPrefix(item, "@") || strings.HasPrefix(item, `"`) {
			return true
		}
	}

	return false
}

// couldCover reports whether a pg_hba address field could include a: false only for a CIDR that does not.
func couldCover(addr string, a netip.Addr) bool {
	p, err := netip.ParsePrefix(addr)

	return err != nil || p.Contains(a)
}

// fields splits a pg_hba or pg_ident line on whitespace, dropping a trailing comment.
func fields(line string) []string {
	line, _, _ = strings.Cut(line, "#")

	return strings.Fields(line)
}
