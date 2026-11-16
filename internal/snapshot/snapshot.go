// Package snapshot holds the last successful result of each query and database.
package snapshot

import (
	"cmp"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// Key identifies a query running against a database.
type Key struct{ Query, Database string }

// Point is one series ready to export.
type Point struct {
	Key                 Key // set by Points
	Metric, Description string
	Kind                queries.Kind
	Labels              []queries.Label
	Value               float64
	Histogram           *queries.Histogram
	Start               time.Time // counters and histograms; zero for gauges
	Observed            time.Time
}

// ErrTotalCap is returned by Put when the entry would push the snapshot over maxSeriesTotal.
var ErrTotalCap = errors.New("the snapshot would exceed maxSeriesTotal")

// seriesState is what the start-time rule remembers about a series.
type seriesState struct {
	kind    queries.Kind
	start   time.Time
	last    float64 // last real value, or histogram Count
	hasLast bool
}

type entry struct {
	fingerprint string
	observed    time.Time
	points      []Point // nil after Fail
	size        int     // the series of the last Put; the cap counts it, also after Fail
	state       map[string]seriesState
}

// Store is safe for concurrent use.
type Store struct {
	mu             sync.RWMutex // covers every field below
	maxSeriesTotal int
	series         int // sum of entry sizes
	entries        map[Key]*entry
}

// New returns an empty Store. A maxSeriesTotal of zero or less sets no cap.
func New(maxSeriesTotal int) *Store {
	return &Store{maxSeriesTotal: maxSeriesTotal, entries: make(map[Key]*entry)}
}

// SetMaxSeriesTotal changes the cap; zero or less removes it.
func (s *Store) SetMaxSeriesTotal(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.maxSeriesTotal = n
}

// Series returns the number of series exported: those of the entries that have not failed since their last Put.
func (s *Store) Series() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := 0
	for _, e := range s.entries {
		n += len(e.points)
	}

	return n
}

// Put replaces the entry atomically, or returns ErrTotalCap and leaves it as it was.
func (s *Store) Put(k Key, fingerprint string, samples []queries.Sample, observed time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.entries[k]
	oldSeries := 0
	if old != nil {
		oldSeries = old.size
	}
	if s.maxSeriesTotal > 0 && s.series-oldSeries+len(samples) > s.maxSeriesTotal {
		return ErrTotalCap
	}

	e := &entry{
		fingerprint: fingerprint,
		observed:    observed,
		points:      make([]Point, 0, len(samples)),
		size:        len(samples),
		state:       make(map[string]seriesState, len(samples)),
	}
	for _, sample := range samples {
		p := Point{
			Metric:      sample.Metric,
			Description: sample.Description,
			Kind:        sample.Kind,
			Labels:      slices.Clone(sample.Labels),
			Value:       sample.Value,
			Histogram:   cloneHistogram(sample.Histogram),
			Observed:    observed,
		}
		if sample.Kind != queries.KindGauge {
			p.Start = e.advance(old, seriesID(sample), sample, observed)
		}
		e.points = append(e.points, p)
	}

	s.series += e.size - oldSeries
	s.entries[k] = e

	return nil
}

// advance applies the OTel reset rule to a series and records its new state.
func (e *entry) advance(old *entry, id string, sample queries.Sample, observed time.Time) time.Time {
	value, usable := sample.Value, true
	if sample.Kind == queries.KindHistogram {
		value = 0
		if sample.Histogram != nil {
			value = float64(sample.Histogram.Count)
		} else {
			usable = false
		}
	}
	usable = usable && !math.IsNaN(value)

	st := seriesState{kind: sample.Kind, start: observed}
	if old != nil {
		if prev, ok := old.state[id]; ok && prev.kind == sample.Kind {
			st = prev
		}
	}
	if usable {
		if st.hasLast && value < st.last {
			st.start = old.observed
		}
		st.last, st.hasLast = value, true
	}
	e.state[id] = st

	return st.start
}

// Fail stops exporting k's series and keeps its start times and its share of maxSeriesTotal until its next Put or a Retain that drops it.
func (s *Store) Fail(k Key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[k]; ok {
		e.points = nil
	}
}

// Retain deletes every entry for which keep is false.
func (s *Store) Retain(keep func(k Key, fingerprint string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, e := range s.entries {
		if !keep(k, e.fingerprint) {
			s.series -= e.size
			delete(s.entries, k)
		}
	}
}

// Points returns copies of every series, sorted by Metric then labels; across entries, the first
// by (Query, Database) keeps a duplicate series and the others are counted.
func (s *Store) Points() (points []Point, duplicates map[Key]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]Key, 0, len(s.entries))
	for k := range s.entries {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b Key) int {
		return cmp.Or(strings.Compare(a.Query, b.Query), strings.Compare(a.Database, b.Database))
	})

	duplicates = make(map[Key]int)
	exported := 0
	for _, e := range s.entries {
		exported += len(e.points)
	}
	seen := make(map[string]struct{}, exported)
	points = make([]Point, 0, exported)
	for _, k := range keys {
		for _, p := range s.entries[k].points {
			id := seriesID(queries.Sample{Metric: p.Metric, Labels: p.Labels})
			if _, dup := seen[id]; dup {
				duplicates[k]++

				continue
			}
			seen[id] = struct{}{}
			p.Key = k
			p.Labels = slices.Clone(p.Labels)
			p.Histogram = cloneHistogram(p.Histogram)
			points = append(points, p)
		}
	}

	slices.SortStableFunc(points, func(a, b Point) int {
		return cmp.Or(strings.Compare(a.Metric, b.Metric), slices.CompareFunc(a.Labels, b.Labels, queries.CompareLabels))
	})

	return points, duplicates
}

// seriesID joins the metric and labels with NUL and SOH separators.
func seriesID(sample queries.Sample) string {
	var b strings.Builder
	b.WriteString(sample.Metric)
	for _, l := range sample.Labels {
		b.WriteString("\x00")
		b.WriteString(l.Key)
		b.WriteString("\x01")
		b.WriteString(l.Value)
	}

	return b.String()
}

func cloneHistogram(h *queries.Histogram) *queries.Histogram {
	if h == nil {
		return nil
	}
	c := *h
	c.Bounds = slices.Clone(h.Bounds)
	c.Cumulative = slices.Clone(h.Cumulative)

	return &c
}
