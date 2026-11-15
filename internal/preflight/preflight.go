// Package preflight checks what the sidecar can reach: its identity,
// PostgreSQL and the Kubernetes API.
package preflight

import (
	"context"
	"os"
	"path/filepath"
)

// Report is the JSON document printed by "instance preflight".
type Report struct {
	UID             int             `json:"uid"`
	PodTokenPresent bool            `json:"podTokenPresent"`
	Kube            *KubeReport     `json:"kube,omitempty"`
	Postgres        *PostgresReport `json:"postgres,omitempty"`
}

// KubeReport is the outcome of the Kubernetes checks.
type KubeReport struct {
	Username        string `json:"username"`
	ClusterReadable bool   `json:"clusterReadable"`
	Error           string `json:"error,omitempty"`
}

// PostgresReport is the outcome of the PostgreSQL check.
type PostgresReport struct {
	Host               string `json:"host"`
	User               string `json:"user"`
	Connected          bool   `json:"connected"`
	CurrentUser        string `json:"currentUser,omitempty"`
	WALFiles           int    `json:"walFiles"`
	ArchiveStatusFiles int    `json:"archiveStatusFiles"`
	SQLState           string `json:"sqlState,omitempty"`
	Error              string `json:"error,omitempty"`
}

// Config selects the checks to run.
type Config struct {
	PodTokenDir string
	Kube        bool   // false skips the Kubernetes checks
	TokenDir    string // token and ca.crt
	APIServer   string // https://host:port
	Namespace   string
	Cluster     string
	Postgres    PostgresConfig
}

// PostgresConfig describes the PostgreSQL connection.
type PostgresConfig struct {
	Host           string // a path starting with / means a socket directory
	Port           uint16
	User           string
	Database       string
	PasswordSecret string // read through the API with the sidecar token
	Password       string // used when PasswordSecret is empty
}

// Run performs the configured checks. Failures are reported, never returned.
func Run(ctx context.Context, cfg Config) Report {
	r := Report{
		UID:             os.Getuid(),
		PodTokenPresent: PodTokenPresent(cfg.PodTokenDir),
	}

	var secretErr error
	pgCfg := cfg.Postgres
	if cfg.Kube {
		var password string
		r.Kube, password, secretErr = checkKube(ctx, cfg)
		if pgCfg.PasswordSecret != "" {
			pgCfg.Password = password
		}
	} else if pgCfg.PasswordSecret != "" {
		secretErr = errSecretNeedsKube
	}

	r.Postgres = checkPostgres(ctx, pgCfg, secretErr)

	return r
}

// PodTokenPresent reports whether dir holds a ServiceAccount token file.
func PodTokenPresent(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "token"))

	return err == nil
}
