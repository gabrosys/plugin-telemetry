package queries

import (
	"regexp"
	"sort"
	"strings"
)

// clusterWide are relations that return the same rows from every database: the shared catalogs of
// PostgreSQL 18.7 (relisshared), the statistics views the PostgreSQL 18 docs (monitoring-stats.html)
// don't scope to the current database, and a few more.
var clusterWide = map[string]struct{}{
	"pg_auth_members":            {},
	"pg_authid":                  {},
	"pg_database":                {},
	"pg_db_role_setting":         {},
	"pg_parameter_acl":           {},
	"pg_replication_origin":      {},
	"pg_shdepend":                {},
	"pg_shdescription":           {},
	"pg_shseclabel":              {},
	"pg_subscription":            {},
	"pg_tablespace":              {},
	"pg_stat_activity":           {},
	"pg_stat_archiver":           {},
	"pg_stat_bgwriter":           {},
	"pg_stat_checkpointer":       {},
	"pg_stat_database":           {},
	"pg_stat_database_conflicts": {},
	"pg_stat_gssapi":             {},
	"pg_stat_io":                 {},
	"pg_stat_recovery_prefetch":  {},
	"pg_stat_replication":        {},
	"pg_stat_replication_slots":  {},
	"pg_stat_slru":               {},
	"pg_stat_ssl":                {},
	"pg_stat_subscription":       {},
	"pg_stat_subscription_stats": {},
	"pg_stat_wal":                {},
	"pg_stat_wal_receiver":       {},
	"pg_replication_slots":       {},
	"pg_settings":                {},
	"pg_roles":                   {},
	"pg_locks":                   {},
	"pg_prepared_xacts":          {},
}

const progressPrefix = "pg_stat_progress_"

var (
	identifier = regexp.MustCompile(`[A-Za-z0-9_$]+`)
	globChars  = regexp.MustCompile(`[][*?]`)
	schemaDot  = regexp.MustCompile(`(?i)\bpg_catalog\s*\.\s*$`)
	anyDot     = regexp.MustCompile(`\.\s*$`)
)

func isClusterWide(name string) bool {
	if _, ok := clusterWide[name]; ok {
		return true
	}

	return strings.HasPrefix(name, progressPrefix) && len(name) > len(progressPrefix)
}

// selectsSeveralDatabases reports whether target_databases names a glob or two or more entries.
func selectsSeveralDatabases(targets []string) bool {
	if len(targets) >= 2 {
		return true
	}
	for _, target := range targets {
		if globChars.MatchString(target) {
			return true
		}
	}

	return false
}

// clusterWideRelations returns, sorted and unique, the cluster-wide relations the SQL names as whole
// identifiers, optionally prefixed by pg_catalog.
func clusterWideRelations(sql string) []string {
	return catalogNames(sql, isClusterWide)
}

// readsSettings reports whether the SQL names pg_settings or pg_show_all_settings, optionally prefixed by
// pg_catalog.
func readsSettings(sql string) bool {
	return len(catalogNames(sql, func(name string) bool {
		return name == "pg_settings" || name == "pg_show_all_settings"
	})) > 0
}

// catalogNames returns, sorted and unique, the lower-cased whole identifiers in the SQL that match,
// unqualified or prefixed by pg_catalog.
func catalogNames(sql string, match func(string) bool) []string {
	found := map[string]struct{}{}
	for _, loc := range identifier.FindAllStringIndex(sql, -1) {
		name := strings.ToLower(sql[loc[0]:loc[1]])
		if !match(name) {
			continue
		}

		before := sql[:loc[0]]
		if anyDot.MatchString(before) {
			prefix := schemaDot.FindStringIndex(before)
			if prefix == nil {
				continue
			}
			// A longer qualified name such as db.pg_catalog.pg_locks is not a plain catalog reference.
			if anyDot.MatchString(before[:prefix[0]]) {
				continue
			}
		}
		found[name] = struct{}{}
	}

	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}
