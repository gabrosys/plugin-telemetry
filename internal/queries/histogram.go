package queries

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	errHistogramPart = errors.New("histogram column is missing")
	errHistogramDims = errors.New("histogram array has more than one dimension")
	errHistogramNaN  = errors.New("histogram bound is NaN")
)

// typeMaps holds the pgtype.Map values scanArray uses; a Map is not safe for concurrent use.
var typeMaps = sync.Pool{New: func() any { return pgtype.NewMap() }}

// histogramFrom builds a histogram as CNPG's histogram.NewFromRawData does (pkg/management/postgres/metrics/histogram/histogram.go:46-105
// at v1.30.1). The arrays arrive as text and are parsed with pgtype.
func histogramFrom(name string, byName map[string]any) (*Histogram, error) {
	rawBounds, ok := byName[name]
	if !ok {
		return nil, fmt.Errorf("%s: %w", name, errHistogramPart)
	}
	rawCounts, ok := byName[name+"_bucket"]
	if !ok {
		return nil, fmt.Errorf("%s_bucket: %w", name, errHistogramPart)
	}

	h := &Histogram{}
	if raw, present := byName[name+"_sum"]; present {
		if h.Sum, ok = toFloat(raw); !ok {
			return nil, fmt.Errorf("convert %s_sum", name)
		}
	}
	if raw, present := byName[name+"_count"]; present {
		if h.Count, ok = toUint(raw); !ok {
			return nil, fmt.Errorf("convert %s_count", name)
		}
	}

	var boundArray pgtype.Array[float64]
	if err := scanArray(pgtype.Float8ArrayOID, rawBounds, &boundArray); err != nil {
		return nil, fmt.Errorf("load %s: %w", name, err)
	}
	var countArray pgtype.Array[int64]
	if err := scanArray(pgtype.Int8ArrayOID, rawCounts, &countArray); err != nil {
		return nil, fmt.Errorf("load %s_bucket: %w", name, err)
	}
	// lib/pq rejects arrays of more than one dimension.
	if len(boundArray.Dims) > 1 || len(countArray.Dims) > 1 {
		return nil, fmt.Errorf("%s: %w", name, errHistogramDims)
	}
	bounds, counts := boundArray.Elements, countArray.Elements
	for _, bound := range bounds {
		if math.IsNaN(bound) {
			return nil, fmt.Errorf("%s: %w", name, errHistogramNaN)
		}
	}
	if bounds == nil {
		return nil, fmt.Errorf("%s: %w", name, errHistogramPart)
	}
	if counts == nil {
		return nil, fmt.Errorf("%s_bucket: %w", name, errHistogramPart)
	}

	// A bound without a count is ignored; a repeated bound keeps the last count.
	buckets := make(map[float64]uint64, len(bounds))
	for i, bound := range bounds {
		if i >= len(counts) {
			break
		}
		if counts[i] >= 0 {
			buckets[bound] = uint64(counts[i])
		} else {
			buckets[bound] = 0
		}
	}

	h.Bounds = make([]float64, 0, len(buckets))
	for bound := range buckets {
		h.Bounds = append(h.Bounds, bound)
	}
	sort.Float64s(h.Bounds)
	h.Cumulative = make([]uint64, len(h.Bounds))
	for i, bound := range h.Bounds {
		h.Cumulative[i] = buckets[bound]
	}

	return h, nil
}

// scanArray parses the text form of a Postgres array; NULL leaves dst nil.
func scanArray(oid uint32, raw any, dst any) error {
	var text []byte
	switch v := raw.(type) {
	case nil:
		return nil
	case string:
		text = []byte(v)
	case []byte:
		text = v
	default:
		return fmt.Errorf("unsupported array value %T", raw)
	}

	m, ok := typeMaps.Get().(*pgtype.Map)
	if !ok {
		m = pgtype.NewMap()
	}
	defer typeMaps.Put(m)

	if err := m.Scan(oid, pgtype.TextFormatCode, text, dst); err != nil {
		return fmt.Errorf("scan array: %w", err)
	}

	return nil
}
