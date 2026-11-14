package clusterconfig

import (
	"errors"
	"strings"
)

// crdFields maps the settings paths that settings.Validate names to the TelemetryExporter fields they
// come from. More specific paths come first.
var crdFields = []struct{ settings, crd string }{
	{"postgres.maxConnections", "spec.metrics.limits.maxConnections"},
	{"exporter.interval", "spec.metrics.interval"},
	{"exporter.maxPointsPerRequest", "spec.metrics.maxPointsPerRequest"},
	{"exporter", "spec.exporter"},
	{"planInterval", "spec.metrics.planInterval"},
	{"defaultInterval", "spec.metrics.defaultInterval"},
	{"groups", "spec.metrics.groups"},
	{"limits", "spec.metrics.limits"},
	{"exclude", "spec.metrics.exclude"},
}

const (
	settingsHeadersHint = "(headers go in exporter.headersFile)"
	crdHeadersHint      = "(headers go in spec.exporter.headers)"
)

// fieldError is an error whose message names TelemetryExporter fields; it unwraps to the original.
type fieldError struct {
	msg string
	err error
}

func (e *fieldError) Error() string { return e.msg }

func (e *fieldError) Unwrap() error { return e.err }

// withCRDFields renames the settings paths in each of the joined errors of err.
func withCRDFields(err error) error {
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		return renameFields(err)
	}
	parts := joined.Unwrap()
	out := make([]error, 0, len(parts))
	for _, part := range parts {
		out = append(out, renameFields(part))
	}
	return errors.Join(out...)
}

// renameFields replaces the leading settings path of err's message and the headers hint.
func renameFields(err error) error {
	msg := strings.Replace(err.Error(), settingsHeadersHint, crdHeadersHint, 1)
	if path, rest, ok := strings.Cut(msg, ": "); ok {
		msg = crdPath(path) + ": " + rest
	}
	if msg == err.Error() {
		return err
	}
	return &fieldError{msg: msg, err: err}
}

func crdPath(path string) string {
	for _, f := range crdFields {
		if rest, ok := strings.CutPrefix(path, f.settings); ok && (rest == "" || rest[0] == '.' || rest[0] == '[') {
			return f.crd + rest
		}
	}
	return path
}
