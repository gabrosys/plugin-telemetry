package queries

import (
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgtype"
)

var errHistogramPart = errors.New("histogram column is missing")

// histogramFrom builds a histogram as CNPG's histogram.NewFromRawData does (pkg/management/postgres/metrics/histogram/histogram.go:46-105
// at v1.30.1). The arrays arrive as text and are parsed with pgtype in place of pq.Array.
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

	var bounds []float64
	if err := scanArray(pgtype.Float8ArrayOID, rawBounds, &bounds); err != nil {
		return nil, fmt.Errorf("load %s: %w", name, err)
	}
	var counts []int64
	if err := scanArray(pgtype.Int8ArrayOID, rawCounts, &counts); err != nil {
		return nil, fmt.Errorf("load %s_bucket: %w", name, err)
	}
	if bounds == nil {
		return nil, fmt.Errorf("%s: %w", name, errHistogramPart)
	}
	if counts == nil {
		return nil, fmt.Errorf("%s_bucket: %w", name, errHistogramPart)
	}

	// A bound without a count is ignored; a repeated bound keeps the last count, as CNPG's map does.
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

	if err := pgtype.NewMap().Scan(oid, pgtype.TextFormatCode, text, dst); err != nil {
		return fmt.Errorf("scan array: %w", err)
	}

	return nil
}
