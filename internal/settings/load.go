package settings

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// presence records which defaulted durations the file sets, so an explicit 0s is kept and fails validation.
type presence struct {
	Groups []struct {
		Interval *time.Duration `yaml:"interval"`
	} `yaml:"groups"`
	Exporter struct {
		Timeout *time.Duration `yaml:"timeout"`
	} `yaml:"exporter"`
}

// Defaults returns the settings Parse starts from; Cluster, the endpoint and the exporter timeout are not defaulted here.
func Defaults() Settings {
	return Settings{
		Postgres: Postgres{
			Host:           "/controller/run",
			Port:           5432,
			User:           "cnpg_metrics_exporter",
			Database:       "postgres",
			MaxConnections: 4,
		},
		PlanInterval:    30 * time.Second,
		DefaultInterval: 30 * time.Second,
		Limits: Limits{
			QueryTimeout:        10 * time.Second,
			LockTimeout:         time.Second,
			MaxRowsPerQuery:     10000,
			MaxSeriesPerQuery:   10000,
			MaxSeriesTotal:      50000,
			MaxDatabases:        50,
			MaxLabelValueLength: queries.DefaultMaxLabelValueLength,
			MaxLabelsPerSeries:  32,
		},
		Exporter: Exporter{
			Protocol:            ProtocolHTTP,
			TLS:                 TLS{MinVersion: defaultMinVersion},
			Compression:         "gzip",
			Interval:            30 * time.Second,
			MaxPointsPerRequest: 2000,
		},
	}
}

// DefaultExporterTimeout is the exporter timeout when none is set: the lower of 10s and half the interval.
func DefaultExporterTimeout(interval time.Duration) time.Duration {
	return min(10*time.Second, interval/2)
}

// Parse decodes YAML strictly (unknown keys are errors) over the defaults, resolves relative file paths
// against dir and validates.
func Parse(data []byte, dir string) (Settings, error) {
	s := Defaults()
	var seen presence
	doc, err := composeOne(data)
	if err != nil {
		return Settings{}, fmt.Errorf("parse settings: %w", err)
	}
	if doc != nil {
		if err := doc.Load(&s, yaml.WithKnownFields()); err != nil {
			return Settings{}, fmt.Errorf("parse settings: %w", err)
		}
		if err := doc.Load(&seen); err != nil {
			return Settings{}, fmt.Errorf("parse settings: %w", err)
		}
	}
	if seen.Exporter.Timeout == nil {
		s.Exporter.Timeout = DefaultExporterTimeout(s.Exporter.Interval)
	}
	for i := range s.Groups {
		if i < len(seen.Groups) && seen.Groups[i].Interval == nil {
			s.Groups[i].Interval = s.DefaultInterval
		}
	}

	s.resolvePaths(dir)

	if err := s.Validate(); err != nil {
		return Settings{}, fmt.Errorf("invalid settings: %w", err)
	}
	return s, nil
}

// composeOne reads the single YAML document of data; an empty file gives nil.
func composeOne(data []byte) (*yaml.Node, error) {
	loader, err := yaml.NewLoader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open loader: %w", err)
	}
	var doc yaml.Node
	if err := loader.Load(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("compose document: %w", err)
	}
	switch err := loader.Load(&yaml.Node{}); {
	case err == nil:
		return nil, errors.New("more than one YAML document")
	case !errors.Is(err, io.EOF):
		return nil, fmt.Errorf("compose second document: %w", err)
	}
	return &doc, nil
}

func (s *Settings) resolvePaths(dir string) {
	for i := range s.Sources {
		s.Sources[i] = resolve(dir, s.Sources[i])
	}
	s.Postgres.PasswordFile = resolve(dir, s.Postgres.PasswordFile)
	s.Exporter.HeadersFile = resolve(dir, s.Exporter.HeadersFile)
	s.Exporter.TLS.CAFile = resolve(dir, s.Exporter.TLS.CAFile)
	s.Exporter.TLS.CertFile = resolve(dir, s.Exporter.TLS.CertFile)
	s.Exporter.TLS.KeyFile = resolve(dir, s.Exporter.TLS.KeyFile)
}

func resolve(dir, p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(dir, p)
}
