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
	dollarTag  = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z0-9_]*)?\$`)
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
	sql = blankCommentsAndStrings(sql)
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

// blankCommentsAndStrings replaces each comment and string constant with a space, following PostgreSQL 18
// "Lexical Structure" (sql-syntax-lexical.html): -- to the end of the line, nesting /* */ blocks, a
// doubled quote inside '...', backslash escapes inside E'...', and $tag$...$tag$. Quoted names ("...",
// with "" inside) are kept.
func blankCommentsAndStrings(sql string) string {
	var b strings.Builder
	for i := 0; i < len(sql); {
		rest := sql[i:]
		afterIdentifier := i > 0 && isIdentifierChar(sql[i-1])
		switch {
		case strings.HasPrefix(rest, "--"):
			end := strings.IndexAny(rest, "\r\n")
			if end < 0 {
				end = len(rest)
			}
			i += end
		case strings.HasPrefix(rest, "/*"):
			i += blockCommentLen(rest)
		case rest[0] == '\'':
			escapes := afterIdentifier && (sql[i-1] == 'e' || sql[i-1] == 'E') && (i < 2 || !isIdentifierChar(sql[i-2]))
			i += quotedLen(rest, escapes)
		case rest[0] == '"':
			// A quoted name is kept as written, so a quote or dashes inside it start nothing.
			n := quotedNameLen(rest)
			b.WriteString(rest[:n])
			i += n
			continue
		case rest[0] == '$' && !afterIdentifier && dollarTag.MatchString(rest):
			tag := dollarTag.FindString(rest)
			end := strings.Index(rest[len(tag):], tag)
			if end < 0 {
				i = len(sql)
			} else {
				i += len(tag) + end + len(tag)
			}
		default:
			b.WriteByte(sql[i])
			i++
			continue
		}
		b.WriteByte(' ')
	}
	return b.String()
}

// blockCommentLen returns the length of the nesting block comment that starts s, or len(s) if it never ends.
func blockCommentLen(s string) int {
	depth := 0
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(s[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(s)
}

// quotedLen returns the length of the string constant that starts s, or len(s) if it never ends. escapes
// makes a backslash take the next byte, as in E'...'.
func quotedLen(s string, escapes bool) int {
	for i := 1; i < len(s); i++ {
		switch {
		case escapes && s[i] == '\\':
			i++
		case s[i] == '\'' && i+1 < len(s) && s[i+1] == '\'':
			i++
		case s[i] == '\'':
			return i + 1
		}
	}
	return len(s)
}

// quotedNameLen returns the length of the quoted name that starts s, "" counting as one quote, or len(s)
// if it never ends.
func quotedNameLen(s string) int {
	for i := 1; i < len(s); i++ {
		if s[i] != '"' {
			continue
		}
		if i+1 < len(s) && s[i+1] == '"' {
			i++
			continue
		}
		return i + 1
	}
	return len(s)
}

func isIdentifierChar(c byte) bool {
	return c == '_' || c == '$' || '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z'
}
