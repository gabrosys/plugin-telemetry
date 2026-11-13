// Package sources loads CloudNativePG custom-query files in CloudNativePG's order and watches them for changes.
package sources

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"sort"
	"sync"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// Source names and load outcomes.
const (
	BuiltinSource  = "builtin"
	OutcomeOK      = "ok"
	OutcomeMissing = "missing"
	OutcomeInvalid = "invalid"
)

// Entry is one query after merging.
type Entry struct {
	Key    string
	Source string // "builtin" or the file path
	Query  queries.UserQuery
}

// Status is the outcome of loading one source.
type Status struct {
	Source        string
	Outcome       string // "ok", "missing", "invalid"
	Err           error
	UsingPrevious bool     // invalid now, its last good version is in force
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
	files []string

	mu       sync.Mutex // guards previous
	previous map[string]queries.UserQueries
}

// NewLoader returns a Loader for files, which are read in order after the builtin query.
func NewLoader(files []string) *Loader {
	return &Loader{files: slices.Clone(files), previous: map[string]queries.UserQueries{}}
}

// Load reads the builtin query, then each file in order. A later source overwrites an earlier key.
// A missing file, or one that has never parsed, is skipped; one that stops parsing keeps its last good version.
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

		content, err := os.ReadFile(file) //nolint:gosec // path comes from operator settings
		if err != nil {
			st.Outcome = OutcomeMissing
			st.Err = fmt.Errorf("read %s: %w", file, err)
			if !errors.Is(err, fs.ErrNotExist) {
				st.Outcome = OutcomeInvalid
			}
			merge(file, l.previousFor(&st, file), st)

			continue
		}

		parsed, err := queries.Parse(content)
		if err != nil {
			st.Outcome = OutcomeInvalid
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

// previousFor returns the last good queries of an invalid, non-missing file and flags the status.
func (l *Loader) previousFor(st *Status, file string) queries.UserQueries {
	if st.Outcome != OutcomeInvalid {
		return nil
	}

	prev, ok := l.previous[file]
	if !ok {
		return nil
	}
	st.UsingPrevious = true

	return prev
}

// hashEntries hashes fmt's %#v form, which prints maps sorted by key and so is deterministic.
func hashEntries(entries []Entry) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%#v", entries))

	return hex.EncodeToString(sum[:])
}
