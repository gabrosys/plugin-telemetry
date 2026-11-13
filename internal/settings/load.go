package settings

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.yaml.in/yaml/v4"
)

// unsetTimeout marks exporter.timeout as absent from the file, so an explicit 0s still fails validation.
const unsetTimeout = time.Duration(-1)

func defaults() Settings {
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
			MaxLabelValueLength: 1024,
			MaxLabelsPerSeries:  32,
		},
		Exporter: Exporter{
			Protocol:            ProtocolHTTP,
			TLS:                 TLS{MinVersion: "1.2"},
			Compression:         "gzip",
			Interval:            30 * time.Second,
			Timeout:             unsetTimeout,
			MaxPointsPerRequest: 2000,
		},
	}
}

// Load reads a YAML file strictly (unknown keys are errors), applies defaults and validates.
// Relative file paths in the file resolve against the file's directory.
func Load(path string) (Settings, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the config path is an operator-supplied flag
	if err != nil {
		return Settings{}, fmt.Errorf("read settings: %w", err)
	}

	s := defaults()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil && !errors.Is(err, io.EOF) {
		return Settings{}, fmt.Errorf("parse settings %s: %w", path, err)
	}

	if s.Exporter.Timeout == unsetTimeout {
		s.Exporter.Timeout = min(10*time.Second, s.Exporter.Interval/2)
	}
	for i := range s.Groups {
		if s.Groups[i].Interval == 0 {
			s.Groups[i].Interval = s.DefaultInterval
		}
	}

	abs, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Settings{}, fmt.Errorf("resolve settings directory: %w", err)
	}
	s.resolvePaths(abs)

	if err := s.Validate(); err != nil {
		return Settings{}, fmt.Errorf("invalid settings %s: %w", path, err)
	}
	return s, nil
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
