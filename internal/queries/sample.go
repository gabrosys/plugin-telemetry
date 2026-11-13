package queries

import (
	"errors"
	"fmt"
	"sort"

	"github.com/Masterminds/semver/v3"
)

// Label is one label of a sample.
type Label struct{ Key, Value string }

// Histogram is the value of a KindHistogram sample.
type Histogram struct {
	Bounds     []float64 // ascending upper bounds
	Cumulative []uint64  // cumulative count per bound, CNPG's bucket values
	Count      uint64
	Sum        float64
}

// Sample is one metric data point of a result row.
type Sample struct {
	Metric      string
	Description string
	Kind        Kind
	Labels      []Label // sorted by Key
	Value       float64 // unused for KindHistogram
	Histogram   *Histogram
}

// DropReason says why a row or a sample was not turned into a data point.
type DropReason string

// The reasons. The executor counts DropDuplicate; RowSamples never returns it.
const (
	DropConversion   DropReason = "conversion"
	DropLabel        DropReason = "label"
	DropLabelTooLong DropReason = "label_too_long"
	DropDuplicate    DropReason = "duplicate"
)

// ErrColumnMismatch is returned when the result has another number of columns than the mapping.
var ErrColumnMismatch = errors.New("result columns don't match the query mapping")

// CheckColumns applies CNPG's rule: the result must have exactly len(Columns) columns.
func (d *Definition) CheckColumns(columns []string) error {
	if len(columns) != len(d.Columns) {
		return fmt.Errorf("%w: %d in the result, %d in the mapping", ErrColumnMismatch, len(columns), len(d.Columns))
	}

	return nil
}

// RowSamples converts one row of database/sql-style values. A label failure drops the whole row;
// a value failure drops only that sample. A nil server runs every column; a maxLabelValueLength
// of zero or less sets no limit.
func (d *Definition) RowSamples(
	columns []string, values []any, server *semver.Version, maxLabelValueLength int,
) (samples []Sample, dropped map[DropReason]int) {
	dropped = make(map[DropReason]int)

	byName := make(map[string]any, len(columns))
	for i, name := range columns {
		if i < len(values) {
			byName[name] = values[i]
		}
	}

	labels, reason := d.rowLabels(byName, maxLabelValueLength)
	if reason != "" {
		dropped[reason]++

		return nil, dropped
	}

	for _, name := range sortedKeys(d.Columns) {
		column := d.Columns[name]
		if column.Role != RoleValue {
			continue
		}
		if server != nil && column.Versions != nil && !column.Versions.Check(server) {
			continue
		}

		sample := Sample{
			Metric:      column.Metric,
			Description: column.Description,
			Kind:        column.Kind,
			Labels:      labels,
		}
		if column.Kind == KindHistogram {
			h, err := histogramFrom(name, byName)
			if err != nil {
				dropped[DropConversion]++

				continue
			}
			sample.Histogram = h
		} else {
			raw, present := byName[name]
			value, ok := columnValue(column, raw)
			if !present || !ok {
				dropped[DropConversion]++

				continue
			}
			sample.Value = value
		}
		samples = append(samples, sample)
	}

	return samples, dropped
}

// rowLabels reads the label columns by name, sorted by key.
func (d *Definition) rowLabels(byName map[string]any, maxLength int) ([]Label, DropReason) {
	var labels []Label
	for _, name := range d.Labels {
		raw, present := byName[name]
		if !present {
			return nil, DropLabel
		}
		text, ok := toString(raw)
		if !ok {
			return nil, DropLabel
		}
		if maxLength > 0 && len(text) > maxLength {
			return nil, DropLabelTooLong
		}
		labels = append(labels, Label{Key: name, Value: text})
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].Key < labels[j].Key })

	return labels, ""
}

func columnValue(column Column, raw any) (float64, bool) {
	switch column.Usage {
	case UsageMappedMetric:
		return mappedValue(column.Mapping, raw)
	case UsageDuration:
		return durationMillis(raw)
	case UsageCounter, UsageGauge, UsageHistogram, UsageLabel, UsageDiscard:
		return toFloat(raw)
	default:
		return toFloat(raw)
	}
}
