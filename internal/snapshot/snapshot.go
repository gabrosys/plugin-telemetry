// Package snapshot holds the last successful result of each query and database.
package snapshot

import (
	"errors"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// Key identifies a query running against a database.
type Key struct{ Query, Database string }

// Point is one series ready to export.
type Point struct {
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
	points      []Point
	state       map[string]seriesState
}

// Store is safe for concurrent use.
type Store struct {
	mu             sync.RWMutex // covers every field below
	maxSeriesTotal int
	series         int
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

// Series returns the number of series held.
func (s *Store) Series() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.series
}

// Put replaces the entry atomically, or returns ErrTotalCap and leaves it as it was.
func (s *Store) Put(k Key, fingerprint string, samples []queries.Sample, observed time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.entries[k]
	oldSeries := 0
	if old != nil {
		oldSeries = len(old.points)
	}
	if s.maxSeriesTotal > 0 && s.series-oldSeries+len(samples) > s.maxSeriesTotal {
		return ErrTotalCap
	}

	e := &entry{
		fingerprint: fingerprint,
		observed:    observed,
		points:      make([]Point, 0, len(samples)),
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

	s.series += len(e.points) - oldSeries
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

// Delete removes the entry and its start times.
func (s *Store) Delete(k Key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[k]; ok {
		s.series -= len(e.points)
		delete(s.entries, k)
	}
}

// Retain deletes every entry for which keep is false.
func (s *Store) Retain(keep func(k Key, fingerprint string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, e := range s.entries {
		if !keep(k, e.fingerprint) {
			s.series -= len(e.points)
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
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Query != keys[j].Query {
			return keys[i].Query < keys[j].Query
		}

		return keys[i].Database < keys[j].Database
	})

	duplicates = make(map[Key]int)
	seen := make(map[string]struct{}, s.series)
	points = make([]Point, 0, s.series)
	for _, k := range keys {
		for _, p := range s.entries[k].points {
			id := seriesID(queries.Sample{Metric: p.Metric, Labels: p.Labels})
			if _, dup := seen[id]; dup {
				duplicates[k]++

				continue
			}
			seen[id] = struct{}{}
			p.Labels = slices.Clone(p.Labels)
			p.Histogram = cloneHistogram(p.Histogram)
			points = append(points, p)
		}
	}

	sort.SliceStable(points, func(i, j int) bool {
		if points[i].Metric != points[j].Metric {
			return points[i].Metric < points[j].Metric
		}

		return compareLabels(points[i].Labels, points[j].Labels) < 0
	})

	return points, duplicates
}

func compareLabels(a, b []queries.Label) int {
	return slices.CompareFunc(a, b, func(x, y queries.Label) int {
		if c := strings.Compare(x.Key, y.Key); c != 0 {
			return c
		}

		return strings.Compare(x.Value, y.Value)
	})
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
