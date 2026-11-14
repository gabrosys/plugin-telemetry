// Package otlp reads the OTLP/JSON lines the collector's file exporter writes, for the e2e suites.
package otlp

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Data types of a Shape.
const (
	TypeGauge                = "gauge"
	TypeSum                  = "sum"
	TypeHistogram            = "histogram"
	TypeExponentialHistogram = "exponential_histogram"
	TypeSummary              = "summary"
)

// ErrShrank is returned by Lines.Update when the file is shorter than the bytes already decoded.
var ErrShrank = errors.New("the file shrank")

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// Line is one request the file exporter wrote.
type Line struct {
	// Time is the latest data point time in the request: the collection time of its export.
	Time    time.Time
	Request *colmetricpb.ExportMetricsServiceRequest
}

// Lines decodes a file that only grows, one OTLP/JSON ExportMetricsServiceRequest per line.
type Lines struct {
	read  int // bytes decoded so far
	lines []Line
}

// Update decodes the complete lines of file past those decoded before; file is the whole file.
// A trailing line without a newline is left for the next call.
func (l *Lines) Update(file []byte) error {
	if len(file) < l.read {
		return fmt.Errorf("%w: %d bytes decoded, %d now", ErrShrank, l.read, len(file))
	}
	for {
		i := bytes.IndexByte(file[l.read:], '\n')
		if i < 0 {
			return nil
		}
		raw := file[l.read : l.read+i]
		if len(bytes.TrimSpace(raw)) > 0 {
			req := &colmetricpb.ExportMetricsServiceRequest{}
			if err := unmarshal.Unmarshal(raw, req); err != nil {
				return fmt.Errorf("decode the line at byte %d: %w", l.read, err)
			}
			l.lines = append(l.lines, Line{Time: latest(req), Request: req})
		}
		l.read += i + 1
	}
}

// All returns the lines decoded so far, in file order.
func (l *Lines) All() []Line {
	return slices.Clone(l.lines)
}

// Since returns the lines whose Time is at or after t.
func Since(lines []Line, t time.Time) []Line {
	var out []Line
	for _, l := range lines {
		if !l.Time.Before(t) {
			out = append(out, l)
		}
	}
	return out
}

// Point is one number data point of a gauge or a sum.
type Point struct {
	Time  time.Time
	Attrs map[string]string
	Value float64
}

// Points returns the number data points of the metric called name, in file order.
func Points(lines []Line, name string) []Point {
	var out []Point
	eachMetric(lines, func(_ *commonpb.InstrumentationScope, m *metricpb.Metric) {
		if m.GetName() != name {
			return
		}
		dps := append(slices.Clone(m.GetGauge().GetDataPoints()), m.GetSum().GetDataPoints()...)
		for _, dp := range dps {
			out = append(out, Point{Time: unixNano(dp.GetTimeUnixNano()), Attrs: Attributes(dp.GetAttributes()), Value: value(dp)})
		}
	})
	return out
}

// ResourcePoint is a Point and the attributes of the resource that reported it.
type ResourcePoint struct {
	Resource map[string]string
	Point
}

// ResourcePoints returns the number data points of the metric called name with their resource, in file order.
func ResourcePoints(lines []Line, name string) []ResourcePoint {
	var out []ResourcePoint
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			resource := Attributes(rm.GetResource().GetAttributes())
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() != name {
						continue
					}
					dps := append(slices.Clone(m.GetGauge().GetDataPoints()), m.GetSum().GetDataPoints()...)
					for _, dp := range dps {
						out = append(out, ResourcePoint{
							Resource: resource,
							Point:    Point{Time: unixNano(dp.GetTimeUnixNano()), Attrs: Attributes(dp.GetAttributes()), Value: value(dp)},
						})
					}
				}
			}
		}
	}
	return out
}

// Export is the part of a request that one resource sent: its resource, the time of the request, and
// its series as `name{key="value",...}`, attributes sorted by key, in file order, and its scopes.
type Export struct {
	Resource map[string]string
	Time     time.Time
	Series   []string
	Scopes   []Scope // distinct, in file order
}

// Exports returns one Export per ResourceMetrics of every line, in file order.
func Exports(lines []Line) []Export {
	var out []Export
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			e := Export{Resource: Attributes(rm.GetResource().GetAttributes()), Time: l.Time}
			for _, sm := range rm.GetScopeMetrics() {
				scope := Scope{Name: sm.GetScope().GetName(), Version: sm.GetScope().GetVersion()}
				if !slices.Contains(e.Scopes, scope) {
					e.Scopes = append(e.Scopes, scope)
				}
				for _, m := range sm.GetMetrics() {
					for _, attrs := range pointAttributes(m) {
						e.Series = append(e.Series, seriesKey(m.GetName(), attrs))
					}
				}
			}
			out = append(out, e)
		}
	}
	return out
}

func seriesKey(name string, attrs map[string]string) string {
	keys := slices.Sorted(maps.Keys(attrs))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Quote(attrs[k]))
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func pointAttributes(m *metricpb.Metric) []map[string]string {
	var out []map[string]string
	add := func(kvs []*commonpb.KeyValue) { out = append(out, Attributes(kvs)) }
	for _, dp := range m.GetGauge().GetDataPoints() {
		add(dp.GetAttributes())
	}
	for _, dp := range m.GetSum().GetDataPoints() {
		add(dp.GetAttributes())
	}
	for _, dp := range m.GetHistogram().GetDataPoints() {
		add(dp.GetAttributes())
	}
	for _, dp := range m.GetExponentialHistogram().GetDataPoints() {
		add(dp.GetAttributes())
	}
	for _, dp := range m.GetSummary().GetDataPoints() {
		add(dp.GetAttributes())
	}
	return out
}

// Last returns the latest point of the metric called name whose attributes include want.
func Last(lines []Line, name string, want map[string]string) (Point, bool) {
	var (
		last  Point
		found bool
	)
	for _, p := range Points(lines, name) {
		if includes(p.Attrs, want) && (!found || !p.Time.Before(last.Time)) {
			last, found = p, true
		}
	}
	return last, found
}

// Matching returns the points whose attributes include want.
func Matching(points []Point, want map[string]string) []Point {
	var out []Point
	for _, p := range points {
		if includes(p.Attrs, want) {
			out = append(out, p)
		}
	}
	return out
}

func includes(attrs, want map[string]string) bool {
	for k, v := range want {
		if got, ok := attrs[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// Shape is what the golden file pins of a metric: name, data type, monotonicity for sums and attribute keys.
type Shape struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Monotonic  *bool    `json:"monotonic,omitempty"`
	Attributes []string `json:"attributes"`
}

// Shapes returns the shape of every metric under the instrumentation scope called scope, sorted by name.
// The attribute keys are those of all the metric's data points. A metric seen with two data types or
// two monotonicities is an error.
func Shapes(lines []Line, scope string) ([]Shape, error) {
	type acc struct {
		shape Shape
		keys  map[string]bool
	}
	seen := map[string]*acc{}
	var errs []error
	eachMetric(lines, func(s *commonpb.InstrumentationScope, m *metricpb.Metric) {
		if s.GetName() != scope {
			return
		}
		shape, keys := shapeOf(m)
		a, ok := seen[m.GetName()]
		if !ok {
			seen[m.GetName()] = &acc{shape: shape, keys: keys}
			return
		}
		if a.shape.Type != shape.Type || !equalPtr(a.shape.Monotonic, shape.Monotonic) {
			errs = append(errs, fmt.Errorf("metric %s: seen as %s and as %s", m.GetName(), describe(a.shape), describe(shape)))
		}
		maps.Copy(a.keys, keys)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	out := make([]Shape, 0, len(seen))
	for _, name := range slices.Sorted(maps.Keys(seen)) {
		a := seen[name]
		a.shape.Attributes = append([]string{}, slices.Sorted(maps.Keys(a.keys))...)
		out = append(out, a.shape)
	}
	return out, nil
}

func shapeOf(m *metricpb.Metric) (Shape, map[string]bool) {
	s := Shape{Name: m.GetName()}
	var attrs [][]*commonpb.KeyValue
	switch d := m.GetData().(type) {
	case *metricpb.Metric_Gauge:
		s.Type = TypeGauge
		for _, dp := range d.Gauge.GetDataPoints() {
			attrs = append(attrs, dp.GetAttributes())
		}
	case *metricpb.Metric_Sum:
		s.Type = TypeSum
		monotonic := d.Sum.GetIsMonotonic()
		s.Monotonic = &monotonic
		for _, dp := range d.Sum.GetDataPoints() {
			attrs = append(attrs, dp.GetAttributes())
		}
	case *metricpb.Metric_Histogram:
		s.Type = TypeHistogram
		for _, dp := range d.Histogram.GetDataPoints() {
			attrs = append(attrs, dp.GetAttributes())
		}
	case *metricpb.Metric_ExponentialHistogram:
		s.Type = TypeExponentialHistogram
		for _, dp := range d.ExponentialHistogram.GetDataPoints() {
			attrs = append(attrs, dp.GetAttributes())
		}
	case *metricpb.Metric_Summary:
		s.Type = TypeSummary
		for _, dp := range d.Summary.GetDataPoints() {
			attrs = append(attrs, dp.GetAttributes())
		}
	}
	keys := map[string]bool{}
	for _, kvs := range attrs {
		for _, kv := range kvs {
			keys[kv.GetKey()] = true
		}
	}
	return s, keys
}

func describe(s Shape) string {
	if s.Monotonic == nil {
		return s.Type
	}
	return fmt.Sprintf("%s (monotonic %t)", s.Type, *s.Monotonic)
}

func equalPtr(a, b *bool) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// Names returns the names of the metrics under the instrumentation scope called scope, sorted.
func Names(lines []Line, scope string) []string {
	seen := map[string]bool{}
	eachMetric(lines, func(s *commonpb.InstrumentationScope, m *metricpb.Metric) {
		if s.GetName() == scope {
			seen[m.GetName()] = true
		}
	})
	return slices.Sorted(maps.Keys(seen))
}

// Scope is an instrumentation scope.
type Scope struct {
	Name    string
	Version string
}

// Scopes returns the distinct instrumentation scopes, sorted by name and version.
func Scopes(lines []Line) []Scope {
	seen := map[Scope]bool{}
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				seen[Scope{Name: sm.GetScope().GetName(), Version: sm.GetScope().GetVersion()}] = true
			}
		}
	}
	return slices.SortedFunc(maps.Keys(seen), func(a, b Scope) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Version, b.Version))
	})
}

// Resources returns the resource attributes of every ResourceMetrics, in file order.
func Resources(lines []Line) []map[string]string {
	var out []map[string]string
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			out = append(out, Attributes(rm.GetResource().GetAttributes()))
		}
	}
	return out
}

// Attributes returns the attributes as strings: integers and doubles in decimal, booleans as true or false.
func Attributes(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[kv.GetKey()] = anyString(kv.GetValue())
	}
	return out
}

func anyString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	default:
		return v.String()
	}
}

func value(dp *metricpb.NumberDataPoint) float64 {
	if v, ok := dp.GetValue().(*metricpb.NumberDataPoint_AsInt); ok {
		return float64(v.AsInt)
	}
	return dp.GetAsDouble()
}

func eachMetric(lines []Line, fn func(*commonpb.InstrumentationScope, *metricpb.Metric)) {
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					fn(sm.GetScope(), m)
				}
			}
		}
	}
}

// latest returns the latest data point time in req.
func latest(req *colmetricpb.ExportMetricsServiceRequest) time.Time {
	var t uint64
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, ts := range pointTimes(m) {
					t = max(t, ts)
				}
			}
		}
	}
	return unixNano(t)
}

func pointTimes(m *metricpb.Metric) []uint64 {
	var out []uint64
	for _, dp := range m.GetGauge().GetDataPoints() {
		out = append(out, dp.GetTimeUnixNano())
	}
	for _, dp := range m.GetSum().GetDataPoints() {
		out = append(out, dp.GetTimeUnixNano())
	}
	for _, dp := range m.GetHistogram().GetDataPoints() {
		out = append(out, dp.GetTimeUnixNano())
	}
	for _, dp := range m.GetExponentialHistogram().GetDataPoints() {
		out = append(out, dp.GetTimeUnixNano())
	}
	for _, dp := range m.GetSummary().GetDataPoints() {
		out = append(out, dp.GetTimeUnixNano())
	}
	return out
}

func unixNano(ns uint64) time.Time {
	return time.Unix(0, int64(ns)) //nolint:gosec // OTLP times are nanoseconds since the epoch, far below MaxInt64
}
