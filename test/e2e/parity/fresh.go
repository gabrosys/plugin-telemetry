package parity

import (
	"fmt"
	"slices"
	"time"

	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

// queryInterval is a query's effective run interval, by query, in seconds.
const queryInterval = "cnpg_otel_query_interval_seconds"

// Wait says which export FreshExport looks for.
type Wait struct {
	After          time.Time     // the export and the last successes it waits for are after After
	Required       []string      // queries whose last success must be after After, however long that takes
	ExportInterval time.Duration // the exporter's interval
	Deadline       time.Time     // when the caller stops waiting
}

// FreshExport returns lines, one Pod's, cut to the first export after w.After that no query holds back, with
// the newest of its time and its last successes. Without one it returns no lines and stale, why the newest
// export didn't qualify.
//
// A required query holds an export back until it has a last success after w.After. Any other query's last
// success at or before w.After holds it back only while that query's next run can still land in the wait: for
// one query interval plus one export interval past w.After, and only when that leaves an export before
// w.Deadline. So a query that keeps failing or runs slower than the wait does not hold the caller up.
func FreshExport(lines []otlp.Line, w Wait) ([]otlp.Line, time.Time, []string) {
	times := otlp.ExportTimes(lines)
	var stale []string
	for i, at := range times {
		stale = nil
		newest := at
		if !at.After(w.After) {
			stale = append(stale, "the export at "+at.UTC().Format(time.RFC3339Nano))
		}
		export := otlp.ExportAt(lines, at)
		intervals := map[string]time.Duration{}
		for _, p := range otlp.Points(export, queryInterval) {
			intervals[p.Attrs["query"]] = time.Duration(p.Value * float64(time.Second))
		}
		successes := otlp.Points(export, lastSuccess)
		for _, query := range w.Required {
			if len(otlp.Matching(successes, map[string]string{"query": query})) == 0 {
				stale = append(stale, query+" has no last success")
			}
		}
		for _, p := range successes {
			query := p.Attrs["query"]
			success := UnixSeconds(p.Value)
			newest = Latest(newest, success)
			if success.After(w.After) {
				continue
			}
			release := w.After.Add(intervals[query] + w.ExportInterval)
			if !slices.Contains(w.Required, query) && (!at.Before(release) || release.Add(w.ExportInterval).After(w.Deadline)) {
				continue
			}
			stale = append(stale, fmt.Sprintf("%s on %s at %s", query, p.Attrs["database"], success.UTC().Format(time.RFC3339Nano)))
		}
		if len(stale) > 0 {
			continue
		}
		var cut []otlp.Line
		for _, l := range lines {
			if i+1 == len(times) || l.Time.Before(times[i+1]) {
				cut = append(cut, l)
			}
		}

		return cut, newest, nil
	}

	return nil, time.Time{}, stale
}

// Latest returns the later of a and b.
func Latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}

	return b
}
