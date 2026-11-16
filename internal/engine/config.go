package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// ErrNoConfiguration is what Config.Settings wraps when there is nothing to run.
var ErrNoConfiguration = errors.New("no configuration")

// ErrNotReady is what Config.Settings wraps when an input has not synced or is denied: the engine
// keeps the configuration in force, counts no load and waits.
var ErrNotReady = errors.New("configuration not ready")

// Config is where the engine reads its settings and the files they name.
type Config interface {
	// Name labels the settings in logs and in the source attribute: the file path, or "kubernetes".
	Name() string
	// Settings reads and validates the settings; the bytes identify what they came from and enter
	// the reload digest. An error wrapping ErrNoConfiguration means there is nothing to run;
	// one wrapping ErrNotReady means an input is not available yet.
	Settings() (settings.Settings, []byte, error)
	// ReadFile reads a file the settings name: fs.ErrNotExist when missing, sources.ErrDenied on a 403.
	ReadFile(name string) ([]byte, error)
	// Watch calls notify once it is watching, then after changes to the settings or the files files()
	// lists; it returns nil only when ctx ends.
	Watch(ctx context.Context, files func() []string, notify func()) error
}

// fileConfig reads the settings from a YAML file and every other file from the file system.
type fileConfig struct{ path string }

// FileConfig returns the Config that reads the settings file at path and the files it names from disk.
func FileConfig(path string) Config { return fileConfig{path: path} }

func (c fileConfig) Name() string { return c.path }

// Settings returns the bytes of the file even when they do not parse, so a later edit changes the digest.
func (c fileConfig) Settings() (settings.Settings, []byte, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return settings.Settings{}, nil, fmt.Errorf("read settings: %w", err)
	}
	dir, err := filepath.Abs(filepath.Dir(c.path))
	if err != nil {
		return settings.Settings{}, data, fmt.Errorf("resolve settings directory: %w", err)
	}
	s, err := settings.Parse(data, dir)
	if err != nil {
		return settings.Settings{}, data, fmt.Errorf("settings %s: %w", c.path, err)
	}
	return s, data, nil
}

func (fileConfig) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name) //nolint:gosec,wrapcheck // paths come from the operator's settings; the os error names the file
}

// Watch also watches the settings file.
func (c fileConfig) Watch(ctx context.Context, files func() []string, notify func()) error {
	withSettings := func() []string { return append([]string{c.path}, files()...) }
	if err := sources.Watch(ctx, withSettings, notify); err != nil {
		return fmt.Errorf("watch files: %w", err)
	}
	return nil
}

// ClusterReader is a Config that watches the instance's Cluster.
type ClusterReader interface{ Cluster() (*apiv1.Cluster, bool) }

// sensitiveConfig is implemented by a Config whose files may hold secret data.
type sensitiveConfig interface {
	// Sensitive reports whether the named file holds secret data.
	Sensitive(name string) bool
}

// LoaderOptions returns the sources.Loader options that follow from cfg.
func LoaderOptions(cfg Config) []sources.LoaderOption {
	if s, ok := cfg.(sensitiveConfig); ok {
		return []sources.LoaderOption{sources.WithSensitive(s.Sensitive)}
	}

	return nil
}
