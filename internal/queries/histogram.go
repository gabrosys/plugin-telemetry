package queries

import (
	"maps"
	"math"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
)

// typeMaps holds the pgtype.Map values scanArray uses; a Map is not safe for concurrent use.
var typeMaps = sync.Pool{New: func() any { return pgtype.NewMap() }}

// histogramFrom builds a histogram as CNPG's histogram.NewFromRawData does (pkg/management/postgres/metrics/histogram/histogram.go:50-110
// at v1.30.1). The arrays arrive as text and are parsed with pgtype. It reports false when the bounds or
// the _bucket column is missing, when either array is empty, malformed or multi-dimensional, when a
// bound is NaN, or when a present _sum or _count does not convert.
func histogramFrom(name string, byName map[string]any) (*Histogram, bool) {
	rawBounds, ok := byName[name]
	if !ok {
		return nil, false
	}
	rawCounts, ok := byName[name+"_bucket"]
	if !ok {
		return nil, false
	}

	h := &Histogram{}
	if raw, present := byName[name+"_sum"]; present {
		if h.Sum, ok = toFloat(raw); !ok {
			return nil, false
		}
	}
	if raw, present := byName[name+"_count"]; present {
		if h.Count, ok = toUint(raw); !ok {
			return nil, false
		}
	}

	var boundArray pgtype.Array[float64]
	var countArray pgtype.Array[int64]
	if !scanArray(pgtype.Float8ArrayOID, rawBounds, &boundArray) || !scanArray(pgtype.Int8ArrayOID, rawCounts, &countArray) {
		return nil, false
	}
	// lib/pq rejects arrays of more than one dimension.
	if len(boundArray.Dims) > 1 || len(countArray.Dims) > 1 {
		return nil, false
	}
	bounds, counts := boundArray.Elements, countArray.Elements
	if len(bounds) == 0 || len(counts) == 0 || slices.ContainsFunc(bounds, math.IsNaN) {
		return nil, false
	}

	// A bound without a count is ignored; a repeated bound keeps the last count.
	buckets := make(map[float64]uint64, len(bounds))
	for i, bound := range bounds {
		if i >= len(counts) {
			break
		}
		buckets[bound] = uint64(max(counts[i], 0))
	}

	h.Bounds = slices.Sorted(maps.Keys(buckets))
	h.Cumulative = make([]uint64, len(h.Bounds))
	for i, bound := range h.Bounds {
		h.Cumulative[i] = buckets[bound]
	}

	return h, true
}

// scanArray parses the text form of a Postgres array into dst and reports whether it could; NULL leaves
// dst empty.
func scanArray(oid uint32, raw any, dst any) bool {
	var text []byte
	switch v := raw.(type) {
	case nil:
		return true
	case string:
		text = []byte(v)
	case []byte:
		text = v
	default:
		return false
	}

	m, ok := typeMaps.Get().(*pgtype.Map)
	if !ok {
		m = pgtype.NewMap()
	}
	defer typeMaps.Put(m)

	return m.Scan(oid, pgtype.TextFormatCode, text, dst) == nil
}
