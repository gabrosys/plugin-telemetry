// Package settings is the schema, defaults and validation of the engine configuration.
package settings

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	minInterval     = time.Second
	minQueryTimeout = 100 * time.Millisecond

	// ProtocolHTTP is the OTLP/HTTP transport.
	ProtocolHTTP = "http/protobuf"
	// ProtocolGRPC is the OTLP/gRPC transport.
	ProtocolGRPC = "grpc"
)

// Settings is the engine configuration.
type Settings struct {
	Cluster            string            `yaml:"cluster"`
	Namespace          string            `yaml:"namespace"`
	Postgres           Postgres          `yaml:"postgres"`
	PlanInterval       time.Duration     `yaml:"planInterval"`
	DefaultInterval    time.Duration     `yaml:"defaultInterval"`
	Groups             []Group           `yaml:"groups"`
	Limits             Limits            `yaml:"limits"`
	Exclude            Exclude           `yaml:"exclude"`
	Sources            []string          `yaml:"sources"`
	Exporter           Exporter          `yaml:"exporter"`
	ResourceAttributes map[string]string `yaml:"resourceAttributes"`
}

// Postgres describes how to reach the instance.
type Postgres struct {
	Host           string `yaml:"host"` // a leading "/" means a socket directory
	Port           uint16 `yaml:"port"`
	User           string `yaml:"user"`
	PasswordFile   string `yaml:"passwordFile"` // read at every connect
	Database       string `yaml:"database"`     // default target database
	MaxConnections int    `yaml:"maxConnections"`
}

// Group sets the collection interval of the queries it matches.
type Group struct {
	Name     string        `yaml:"name"`
	Interval time.Duration `yaml:"interval"`
	Queries  []string      `yaml:"queries"` // query keys or path.Match patterns
}

// Limits bounds the work and the cardinality of a collection.
type Limits struct {
	QueryTimeout        time.Duration `yaml:"queryTimeout"`
	LockTimeout         time.Duration `yaml:"lockTimeout"`
	MaxRowsPerQuery     int           `yaml:"maxRowsPerQuery"`
	MaxSeriesPerQuery   int           `yaml:"maxSeriesPerQuery"`
	MaxSeriesTotal      int           `yaml:"maxSeriesTotal"`
	MaxDatabases        int           `yaml:"maxDatabases"`
	MaxLabelValueLength int           `yaml:"maxLabelValueLength"` // bytes
	MaxLabelsPerSeries  int           `yaml:"maxLabelsPerSeries"`
}

// Exclude lists names or path.Match patterns to skip.
type Exclude struct {
	Queries   []string `yaml:"queries"`
	Metrics   []string `yaml:"metrics"`
	Databases []string `yaml:"databases"`
}

// Exporter names follow the OTel declarative configuration v1.2.0 otlp_http/otlp_grpc
// exporter (schema/meter_provider.yaml:355-518), in camelCase; serverName and minVersion are ours.
type Exporter struct {
	Protocol            string        `yaml:"protocol"` // "http/protobuf" or "grpc"
	Endpoint            string        `yaml:"endpoint"` // full URL; http:// is plaintext, https:// is TLS
	TLS                 TLS           `yaml:"tls"`
	HeadersFile         string        `yaml:"headersFile"` // OTEL_EXPORTER_OTLP_HEADERS format
	Compression         string        `yaml:"compression"` // "gzip" or "none"
	Interval            time.Duration `yaml:"interval"`
	Timeout             time.Duration `yaml:"timeout"` // per attempt, at most Interval/2
	MaxPointsPerRequest int           `yaml:"maxPointsPerRequest"`
}

// TLS configures the exporter connection.
type TLS struct {
	CAFile     string `yaml:"caFile"` // added to the system roots
	CertFile   string `yaml:"certFile"`
	KeyFile    string `yaml:"keyFile"`
	ServerName string `yaml:"serverName"`
	MinVersion string `yaml:"minVersion"` // "1.2" or "1.3"
}

func (t TLS) isZero() bool {
	return t == TLS{}
}

// Validate checks every rule and returns all violations joined.
func (s Settings) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if s.Cluster == "" {
		add("cluster: must not be empty")
	}
	if s.PlanInterval < minInterval {
		add("planInterval: must be at least %s, got %s", minInterval, s.PlanInterval)
	}
	if s.DefaultInterval < minInterval {
		add("defaultInterval: must be at least %s, got %s", minInterval, s.DefaultInterval)
	}

	errs = append(errs, s.validatePostgres()...)
	errs = append(errs, s.validateGroups()...)
	errs = append(errs, s.validateLimits()...)
	errs = append(errs, s.validateExclude()...)
	errs = append(errs, s.validateExporter()...)

	return errors.Join(errs...)
}

func (s Settings) validatePostgres() []error {
	var errs []error
	p := s.Postgres
	if p.Host == "" {
		errs = append(errs, errors.New("postgres.host: must not be empty"))
	}
	if p.Port == 0 {
		errs = append(errs, errors.New("postgres.port: must not be 0"))
	}
	if p.User == "" {
		errs = append(errs, errors.New("postgres.user: must not be empty"))
	}
	if p.Database == "" {
		errs = append(errs, errors.New("postgres.database: must not be empty"))
	}
	if p.MaxConnections < 1 {
		errs = append(errs, fmt.Errorf("postgres.maxConnections: must be at least 1, got %d", p.MaxConnections))
	}
	return errs
}

func (s Settings) validateGroups() []error {
	var errs []error
	seen := map[string]bool{}
	for i, g := range s.Groups {
		switch {
		case g.Name == "":
			errs = append(errs, fmt.Errorf("groups[%d].name: must not be empty", i))
		case seen[g.Name]:
			errs = append(errs, fmt.Errorf("groups[%d].name: duplicate group name %q", i, g.Name))
		}
		seen[g.Name] = true
		if g.Interval < minInterval {
			errs = append(errs, fmt.Errorf("groups[%d].interval: must be at least %s, got %s", i, minInterval, g.Interval))
		}
		if len(g.Queries) == 0 {
			errs = append(errs, fmt.Errorf("groups[%d].queries: must not be empty", i))
		}
		errs = append(errs, checkPatterns(fmt.Sprintf("groups[%d].queries", i), g.Queries)...)
	}
	return errs
}

func (s Settings) validateLimits() []error {
	var errs []error
	l := s.Limits
	if l.QueryTimeout < minQueryTimeout {
		errs = append(errs, fmt.Errorf("limits.queryTimeout: must be at least %s, got %s", minQueryTimeout, l.QueryTimeout))
	}
	if l.LockTimeout <= 0 {
		errs = append(errs, fmt.Errorf("limits.lockTimeout: must be positive, got %s", l.LockTimeout))
	}
	for _, c := range []struct {
		name string
		v    int
	}{
		{"maxRowsPerQuery", l.MaxRowsPerQuery},
		{"maxSeriesPerQuery", l.MaxSeriesPerQuery},
		{"maxSeriesTotal", l.MaxSeriesTotal},
		{"maxDatabases", l.MaxDatabases},
		{"maxLabelValueLength", l.MaxLabelValueLength},
		{"maxLabelsPerSeries", l.MaxLabelsPerSeries},
	} {
		if c.v < 1 {
			errs = append(errs, fmt.Errorf("limits.%s: must be at least 1, got %d", c.name, c.v))
		}
	}
	return errs
}

func (s Settings) validateExclude() []error {
	var errs []error
	errs = append(errs, checkPatterns("exclude.queries", s.Exclude.Queries)...)
	errs = append(errs, checkPatterns("exclude.metrics", s.Exclude.Metrics)...)
	errs = append(errs, checkPatterns("exclude.databases", s.Exclude.Databases)...)
	return errs
}

func (s Settings) validateExporter() []error {
	var errs []error
	e := s.Exporter

	switch e.Protocol {
	case ProtocolHTTP, ProtocolGRPC:
	default:
		errs = append(errs, fmt.Errorf("exporter.protocol: must be %q or %q, got %q", ProtocolHTTP, ProtocolGRPC, e.Protocol))
	}
	switch e.Compression {
	case "gzip", "none":
	default:
		errs = append(errs, fmt.Errorf(`exporter.compression: must be "gzip" or "none", got %q`, e.Compression))
	}
	if e.Interval < minInterval {
		errs = append(errs, fmt.Errorf("exporter.interval: must be at least %s, got %s", minInterval, e.Interval))
	}
	if err := CheckExporterTimeout(e.Timeout, e.Interval); err != nil {
		errs = append(errs, fmt.Errorf("exporter.timeout: %w, got %s", err, e.Timeout))
	}
	if e.MaxPointsPerRequest < 1 {
		errs = append(errs, fmt.Errorf("exporter.maxPointsPerRequest: must be at least 1, got %d", e.MaxPointsPerRequest))
	}

	switch e.TLS.MinVersion {
	case "1.2", "1.3":
	default:
		errs = append(errs, fmt.Errorf(`exporter.tls.minVersion: must be "1.2" or "1.3", got %q`, e.TLS.MinVersion))
	}
	if (e.TLS.CertFile == "") != (e.TLS.KeyFile == "") {
		missing := "keyFile"
		if e.TLS.CertFile == "" {
			missing = "certFile"
		}
		errs = append(errs, fmt.Errorf("exporter.tls.%s: required when the other of certFile and keyFile is set", missing))
	}

	switch err := CheckEndpoint(e.Endpoint); {
	case e.Endpoint == "":
		errs = append(errs, errors.New("exporter.endpoint: must not be empty"))
	case err != nil:
		errs = append(errs, fmt.Errorf("exporter.endpoint: %w, got %q", err, RedactEndpoint(e.Endpoint)))
	case e.tlsConfigured() && plaintext(e.Endpoint):
		errs = append(errs, errors.New("exporter.tls: cannot be set with an http:// endpoint"))
	}
	return errs
}

// plaintext reports whether a valid endpoint has the http scheme.
func plaintext(endpoint string) bool {
	u, err := url.Parse(endpoint)
	return err == nil && u.Scheme == "http"
}

// CheckExporterTimeout checks the per-attempt timeout against the export interval. The error holds no value.
func CheckExporterTimeout(timeout, interval time.Duration) error {
	if timeout <= 0 || timeout > interval/2 {
		return fmt.Errorf("must be above 0 and at most half of the export interval (%s)", interval/2)
	}
	return nil
}

var (
	errEndpointURL   = errors.New("must be an http or https URL with a host")
	errEndpointParts = errors.New("must not have user info, a query or a fragment (headers go in exporter.headersFile)")
)

// CheckEndpoint checks the shape of an exporter endpoint. The error holds no value.
func CheckEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	switch {
	case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		return errEndpointURL
	case u.User != nil || u.RawQuery != "" || u.Fragment != "":
		return errEndpointParts
	}
	return nil
}

// RedactEndpoint returns endpoint for messages: the URL's Redacted form with the user, the query and
// the fragment also replaced by xxxxx, "(not a URL)" when it does not parse, or "(no host)" when it
// has no host, because user info then parses as a scheme, an opaque part or a path.
func RedactEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "(not a URL)"
	}
	if u.Host == "" {
		return "(no host)"
	}
	if u.User != nil {
		u.User = url.UserPassword("xxxxx", "xxxxx")
	}
	if u.RawQuery != "" {
		u.RawQuery = "xxxxx"
	}
	if u.Fragment != "" {
		u.Fragment, u.RawFragment = "xxxxx", ""
	}
	return u.Redacted()
}

// tlsConfigured reports whether any TLS option other than the default minVersion is set.
func (e Exporter) tlsConfigured() bool {
	t := e.TLS
	t.MinVersion = ""
	return !t.isZero()
}

func checkPatterns(field string, patterns []string) []error {
	var errs []error
	for i, p := range patterns {
		if _, err := path.Match(p, ""); err != nil {
			errs = append(errs, fmt.Errorf("%s[%d]: invalid pattern %q: %w", field, i, p, err))
		}
	}
	return errs
}

func matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, err := path.Match(p, name); err == nil && ok {
			return true
		}
	}
	return false
}

// GroupFor returns the first group whose pattern matches key, else ("", DefaultInterval).
func (s Settings) GroupFor(key string) (group string, interval time.Duration) {
	for _, g := range s.Groups {
		if matchAny(g.Queries, key) {
			return g.Name, g.Interval
		}
	}
	return "", s.DefaultInterval
}

// ExcludedQuery reports whether the query key is excluded.
func (s Settings) ExcludedQuery(key string) bool { return matchAny(s.Exclude.Queries, key) }

// ExcludedMetric reports whether the metric name is excluded.
func (s Settings) ExcludedMetric(name string) bool { return matchAny(s.Exclude.Metrics, name) }

// ExcludedDatabase reports whether the database is excluded.
func (s Settings) ExcludedDatabase(name string) bool { return matchAny(s.Exclude.Databases, name) }

// Warnings lists settings that load but look wrong, such as a query matched by two groups.
func (s Settings) Warnings(queryKeys []string) []string {
	var out []string
	for _, key := range queryKeys {
		var names []string
		for _, g := range s.Groups {
			if matchAny(g.Queries, key) {
				names = append(names, g.Name)
			}
		}
		if len(names) > 1 {
			out = append(out, fmt.Sprintf("query %q is matched by groups %s; the first one wins", key, strings.Join(names, ", ")))
		}
	}
	for _, g := range s.Groups {
		for _, p := range g.Queries {
			if !matchesAnyKey(p, queryKeys) {
				out = append(out, fmt.Sprintf("group %q: pattern %q matches no query", g.Name, p))
			}
		}
	}
	return out
}

func matchesAnyKey(pattern string, keys []string) bool {
	for _, k := range keys {
		if ok, err := path.Match(pattern, k); err == nil && ok {
			return true
		}
	}
	return false
}
