// Package sources loads CloudNativePG custom-query files in CloudNativePG's order and watches them for changes.
package sources

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.yaml.in/yaml/v4"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// Source names and load outcomes.
const (
	BuiltinSource  = "builtin"
	OutcomeOK      = "ok"
	OutcomeMissing = "missing"
	OutcomeInvalid = "invalid"
	// OutcomeDenied marks a source the reader was not allowed to read; like OutcomeInvalid, it keeps the last good version.
	OutcomeDenied = "denied"
)

// ErrDenied is what a ReadFile wraps when the source forbids the read, such as an HTTP 403.
var ErrDenied = errors.New("permission denied")

// ReadFile returns the content of the named file: an error wrapping fs.ErrNotExist when it is missing,
// one wrapping ErrDenied when the read is forbidden.
type ReadFile func(name string) ([]byte, error)

// Entry is one query after merging.
type Entry struct {
	Key    string
	Source string // "builtin" or the file path
	Query  queries.UserQuery
}

// Status is the outcome of loading one source.
type Status struct {
	Source        string
	Outcome       string // "ok", "missing", "invalid", "denied"
	Err           error
	UsingPrevious bool     // invalid or denied now, its last good version is in force
	Duplicates    []string // keys this source overwrote from an earlier source, sorted
}

// Result is the merged view of all sources.
type Result struct {
	Entries []Entry  // merged, later sources overwrite earlier keys; sorted by Key
	Status  []Status // one per source, in load order
	Hash    string   // hex sha256 of the merged entries
}

// Loader loads the builtin query and the files, remembering each file's last good version.
type Loader struct {
	read      ReadFile
	files     []string
	sensitive func(name string) bool // nil: no source is sensitive

	mu       sync.Mutex // guards previous
	previous map[string]queries.UserQueries
}

// LoaderOption configures a Loader.
type LoaderOption func(*Loader)

// WithSensitive marks the sources for which isSensitive returns true as holding secret data: a parse
// error of such a source names the source and the line, and quotes none of its content.
func WithSensitive(isSensitive func(name string) bool) LoaderOption {
	return func(l *Loader) { l.sensitive = isSensitive }
}

// NewLoader returns a Loader that reads files with read, in order after the builtin query.
func NewLoader(read ReadFile, files []string, opts ...LoaderOption) *Loader {
	l := &Loader{read: read, files: slices.Clone(files), previous: map[string]queries.UserQueries{}}
	for _, o := range opts {
		o(l)
	}

	return l
}

// SetFiles makes files the sources, in order after the builtin query. A file that stays listed keeps its
// last good version; one that is dropped forgets it.
func (l *Loader) SetFiles(files []string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.files = slices.Clone(files)
	for file := range l.previous {
		if !slices.Contains(l.files, file) {
			delete(l.previous, file)
		}
	}
}

// Load reads the builtin query, then each file in order. A later source overwrites an earlier key.
// A missing file, or one that has never parsed or been readable, is skipped; one that stops parsing or
// becomes unreadable keeps its last good version. A file that goes missing keeps its last good version
// too: it serves again if the file comes back invalid.
func (l *Loader) Load() Result {
	l.mu.Lock()
	defer l.mu.Unlock()

	merged := map[string]Entry{}
	statuses := make([]Status, 0, len(l.files)+1)

	merge := func(source string, qs queries.UserQueries, st Status) {
		for key, q := range qs {
			if _, found := merged[key]; found {
				st.Duplicates = append(st.Duplicates, key)
			}
			merged[key] = Entry{Key: key, Source: source, Query: q}
		}
		sort.Strings(st.Duplicates)
		statuses = append(statuses, st)
	}

	merge(BuiltinSource, queries.Builtin(), Status{Source: BuiltinSource, Outcome: OutcomeOK})

	for _, file := range l.files {
		st := Status{Source: file}

		content, err := l.read(file)
		if err != nil {
			st.Err = fmt.Errorf("read %s: %w", file, err)
			switch {
			case errors.Is(err, fs.ErrNotExist):
				st.Outcome = OutcomeMissing
			case errors.Is(err, ErrDenied):
				st.Outcome = OutcomeDenied
			default:
				st.Outcome = OutcomeInvalid
			}
			merge(file, l.previousFor(&st, file), st)

			continue
		}

		parsed, err := queries.Parse(content)
		if err != nil {
			st.Outcome = OutcomeInvalid
			if l.sensitive != nil && l.sensitive(file) {
				err = redactedParseError(err)
			}
			st.Err = fmt.Errorf("load %s: %w", file, err)
			merge(file, l.previousFor(&st, file), st)

			continue
		}

		st.Outcome = OutcomeOK
		l.previous[file] = parsed
		merge(file, parsed, st)
	}

	entries := make([]Entry, 0, len(merged))
	for _, e := range merged {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	return Result{Entries: entries, Status: statuses, Hash: hashEntries(entries)}
}

// previousFor returns the last good queries of an invalid or denied file and flags the status.
func (l *Loader) previousFor(st *Status, file string) queries.UserQueries {
	if st.Outcome != OutcomeInvalid && st.Outcome != OutcomeDenied {
		return nil
	}

	prev, ok := l.previous[file]
	if !ok {
		return nil
	}
	st.UsingPrevious = true

	return prev
}

// hashEntries hashes fmt's %#v form, which prints maps sorted by key and so is deterministic. An empty
// TargetDatabases, Metrics or MetricMapping hashes as an absent one.
func hashEntries(entries []Entry) string {
	norm := make([]Entry, len(entries))
	for i, e := range entries {
		if len(e.Query.TargetDatabases) == 0 {
			e.Query.TargetDatabases = nil
		}
		if len(e.Query.Metrics) == 0 {
			e.Query.Metrics = nil
		} else {
			e.Query.Metrics = normalizeMappings(e.Query.Metrics)
		}
		norm[i] = e
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%#v", norm))

	return hex.EncodeToString(sum[:])
}

// normalizeMappings copies metrics with every empty MetricMapping set to nil.
func normalizeMappings(metrics []queries.Mapping) []queries.Mapping {
	out := make([]queries.Mapping, len(metrics))
	for i, m := range metrics {
		out[i] = make(queries.Mapping, len(m))
		for name, col := range m {
			if len(col.MetricMapping) == 0 {
				col.MetricMapping = nil
			}
			out[i][name] = col
		}
	}

	return out
}

// redactedParseError describes a YAML error by its line numbers only: the library's messages quote the
// content they failed on.
func redactedParseError(err error) error {
	var lines []string
	add := func(l int) {
		if l > 0 {
			lines = append(lines, strconv.Itoa(l))
		}
	}
	var many *yaml.LoadErrors
	var one *yaml.LoadError
	switch {
	case errors.As(err, &many):
		for _, e := range many.Errors {
			add(e.Mark.Line)
		}
	case errors.As(err, &one):
		add(one.Mark.Line)
	}
	if len(lines) == 0 {
		return errors.New("parse user queries: invalid YAML")
	}

	return fmt.Errorf("parse user queries: invalid YAML at line %s", strings.Join(lines, ", "))
}
