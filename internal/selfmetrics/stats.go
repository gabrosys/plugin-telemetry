package selfmetrics

import (
	"maps"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Outcome is the result of a query run, config load or export.
type Outcome string

// Outcomes recorded by the engine.
const (
	OutcomeOK      Outcome = "ok"
	OutcomeError   Outcome = "error"
	OutcomeTimeout Outcome = "timeout"
	OutcomeSkipped Outcome = "skipped"
	OutcomeCapped  Outcome = "capped"
)

// errorTypeRejected is the error.type of data points the backend rejected.
const errorTypeRejected = "rejected"

type queryDB struct{ query, database string }

// QueryDatabase is a query planned on a database.
type QueryDatabase struct{ Query, Database string }

type runKey struct {
	query, database string
	outcome         Outcome
}

type dropKey struct{ query, database, reason string }

type loadKey struct {
	source  string
	outcome Outcome
}

type pointKey struct {
	errorType, componentType, address string
	port                              int
}

// ExportRecord describes one export operation after its last retry.
type ExportRecord struct {
	Outcome                      Outcome // ok, error, skipped
	Duration                     time.Duration
	At                           time.Time
	Points, Rejected             int
	ErrorType                    string // semconv error.type; empty when ok
	ComponentType, ServerAddress string
	ServerPort                   int
	HTTPStatusCode               int // 0 when there was no HTTP response
}

// Stats holds the self-metric values; Register exposes them through observable instruments.
type Stats struct {
	// mu guards every field below.
	mu sync.RWMutex

	runs        map[runKey]int64
	lastDur     map[queryDB]float64
	lastSuccess map[queryDB]float64
	interval    map[string]float64
	capped      map[queryDB]int64
	dropped     map[dropKey]int64
	series      int
	seriesSet   bool

	loads map[loadKey]int64
	dups  map[string]int64

	generation uint64
	hash       string
	configSet  bool

	exports       map[Outcome]int64
	exported      map[pointKey]int64
	lastExportOK  float64
	lastExportSet bool
	duration      metric.Float64Histogram
}

// NewStats returns an empty store.
func NewStats() *Stats {
	return &Stats{
		runs:        map[runKey]int64{},
		lastDur:     map[queryDB]float64{},
		lastSuccess: map[queryDB]float64{},
		interval:    map[string]float64{},
		capped:      map[queryDB]int64{},
		dropped:     map[dropKey]int64{},
		loads:       map[loadKey]int64{},
		dups:        map[string]int64{},
		exports:     map[Outcome]int64{},
		exported:    map[pointKey]int64{},
	}
}

func seconds(d time.Duration) float64 { return d.Seconds() }

func timestamp(t time.Time) float64 { return float64(t.UnixNano()) / float64(time.Second) }

// QueryRun records one run of a query. A run that was not capped clears the capped series;
// a skipped run is only counted.
func (s *Stats) QueryRun(query, database string, o Outcome, d time.Duration, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[runKey{query, database, o}]++
	if o == OutcomeSkipped {
		return
	}
	k := queryDB{query, database}
	s.lastDur[k] = seconds(d)
	if o == OutcomeOK {
		s.lastSuccess[k] = timestamp(at)
	}
	if o != OutcomeCapped {
		delete(s.capped, k)
	}
}

// QueryCapped records how many series a capped query would have produced.
func (s *Stats) QueryCapped(query, database string, wouldBe int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capped[queryDB{query, database}] = int64(wouldBe)
}

// SeriesDropped adds n to the series dropped from a query for a reason.
func (s *Stats) SeriesDropped(query, database, reason string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped[dropKey{query, database, reason}] += int64(n)
}

// QueryInterval records the effective run interval of a query.
func (s *Stats) QueryInterval(query string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.interval[query] = seconds(d)
}

// Series records the number of series currently held.
func (s *Stats) Series(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.series, s.seriesSet = n, true
}

// ConfigLoad records one configuration load and the duplicate queries it found.
func (s *Stats) ConfigLoad(source string, ok bool, duplicates int) {
	outcome := OutcomeOK
	if !ok {
		outcome = OutcomeError
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads[loadKey{source, outcome}]++
	s.dups[source] += int64(duplicates)
}

// ConfigGeneration records the active configuration generation and hash.
func (s *Stats) ConfigGeneration(generation uint64, hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation, s.hash, s.configSet = generation, hash, true
}

// Export records one export operation. Skipped exports only count in the export total. The last
// success time moves only when the backend accepted at least one point.
func (s *Stats) Export(r ExportRecord) {
	s.mu.Lock()
	s.exports[r.Outcome]++
	if r.Outcome == OutcomeSkipped {
		s.mu.Unlock()
		return
	}
	key := pointKey{r.ErrorType, r.ComponentType, r.ServerAddress, r.ServerPort}
	if r.Outcome == OutcomeOK {
		if r.Rejected < r.Points {
			s.lastExportOK, s.lastExportSet = timestamp(r.At), true
		}
		key.errorType = ""
		s.addPoints(key, r.Points-r.Rejected)
	} else {
		s.addPoints(key, r.Points)
	}
	if r.Rejected > 0 {
		key.errorType = errorTypeRejected
		s.addPoints(key, r.Rejected)
	}
	hist := s.duration
	s.mu.Unlock()

	if hist != nil {
		hist.Record(contextForRecord, seconds(r.Duration), metric.WithAttributes(exportAttributes(r)...))
	}
}

// addPoints must be called with s.mu held.
func (s *Stats) addPoints(k pointKey, n int) {
	if n > 0 {
		s.exported[k] += int64(n)
	}
}

// Retain drops the series of every query and database not in keep, and the interval of every
// query no entry of keep has, so queries and databases that leave the plan stop reporting.
func (s *Stats) Retain(keep map[QueryDatabase]bool) {
	planned := make(map[string]bool, len(keep))
	for k := range keep {
		planned[k.Query] = true
	}
	gone := func(query, database string) bool { return !keep[QueryDatabase{query, database}] }

	s.mu.Lock()
	defer s.mu.Unlock()
	maps.DeleteFunc(s.runs, func(k runKey, _ int64) bool { return gone(k.query, k.database) })
	maps.DeleteFunc(s.lastDur, func(k queryDB, _ float64) bool { return gone(k.query, k.database) })
	maps.DeleteFunc(s.lastSuccess, func(k queryDB, _ float64) bool { return gone(k.query, k.database) })
	maps.DeleteFunc(s.capped, func(k queryDB, _ int64) bool { return gone(k.query, k.database) })
	maps.DeleteFunc(s.dropped, func(k dropKey, _ int64) bool { return gone(k.query, k.database) })
	maps.DeleteFunc(s.interval, func(query string, _ float64) bool { return !planned[query] })
}

type point struct {
	value float64
	attrs []attribute.KeyValue
}

func exportAttrs(errorType, componentType, address string, port int) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	if errorType != "" {
		attrs = append(attrs, attribute.String("error.type", errorType))
	}
	return append(attrs,
		attribute.String("otel.component.type", componentType),
		attribute.String("otel.component.name", componentType+"/0"),
		attribute.String("server.address", address),
		attribute.Int("server.port", port),
	)
}

func exportAttributes(r ExportRecord) []attribute.KeyValue {
	errorType := ""
	if r.Outcome != OutcomeOK {
		errorType = r.ErrorType
	}
	attrs := exportAttrs(errorType, r.ComponentType, r.ServerAddress, r.ServerPort)
	if r.HTTPStatusCode != 0 {
		attrs = append(attrs, attribute.Int("http.response.status_code", r.HTTPStatusCode))
	}
	return attrs
}

// points returns the current points of the catalogue metric called name.
func (s *Stats) points(name string) ([]point, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []point
	qd := func(k queryDB) []attribute.KeyValue {
		return []attribute.KeyValue{attribute.String("query", k.query), attribute.String("database", k.database)}
	}
	switch name {
	case "cnpg_otel_query_runs_total":
		for k, v := range s.runs {
			out = append(out, point{float64(v), []attribute.KeyValue{
				attribute.String("query", k.query), attribute.String("database", k.database),
				attribute.String("outcome", string(k.outcome)),
			}})
		}
	case "cnpg_otel_query_last_duration_seconds":
		for k, v := range s.lastDur {
			out = append(out, point{v, qd(k)})
		}
	case "cnpg_otel_query_last_success_timestamp_seconds":
		for k, v := range s.lastSuccess {
			out = append(out, point{v, qd(k)})
		}
	case "cnpg_otel_query_interval_seconds":
		for k, v := range s.interval {
			out = append(out, point{v, []attribute.KeyValue{attribute.String("query", k)}})
		}
	case "cnpg_otel_query_capped_series":
		for k, v := range s.capped {
			out = append(out, point{float64(v), qd(k)})
		}
	case "cnpg_otel_query_dropped_series_total":
		for k, v := range s.dropped {
			out = append(out, point{float64(v), []attribute.KeyValue{
				attribute.String("query", k.query), attribute.String("database", k.database),
				attribute.String("reason", k.reason),
			}})
		}
	case "cnpg_otel_series":
		if s.seriesSet {
			out = append(out, point{value: float64(s.series)})
		}
	case "cnpg_otel_config_loads_total":
		for k, v := range s.loads {
			out = append(out, point{float64(v), []attribute.KeyValue{
				attribute.String("source", k.source), attribute.String("outcome", string(k.outcome)),
			}})
		}
	case "cnpg_otel_config_duplicate_queries_total":
		for k, v := range s.dups {
			out = append(out, point{float64(v), []attribute.KeyValue{attribute.String("source", k)}})
		}
	case "cnpg_otel_config_info":
		if s.configSet {
			out = append(out, point{1, []attribute.KeyValue{
				attribute.String("generation", strconv.FormatUint(s.generation, 10)),
				attribute.String("hash", s.hash),
			}})
		}
	case "cnpg_otel_exports_total":
		for k, v := range s.exports {
			out = append(out, point{float64(v), []attribute.KeyValue{attribute.String("outcome", string(k))}})
		}
	case "cnpg_otel_last_export_success_timestamp_seconds":
		if s.lastExportSet {
			out = append(out, point{value: s.lastExportOK})
		}
	case "otel.sdk.exporter.metric_data_point.exported":
		for k, v := range s.exported {
			out = append(out, point{float64(v), exportAttrs(k.errorType, k.componentType, k.address, k.port)})
		}
	default:
		return nil, false
	}
	return out, true
}
