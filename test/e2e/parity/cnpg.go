package parity

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// ErrNoUpdateTime is returned when the CNPG text lacks cnpg_last_update_timestamp.
var ErrNoUpdateTime = errors.New("the CNPG text has no cnpg_last_update_timestamp")

const (
	updateMetric  = "cnpg_last_update_timestamp"
	builtinPrefix = "cnpg_collector_"
	// loPages is CNPG's one cnpg_collector_* metric from a query (pg_collector.go:639-660 at v1.30.1).
	loPages = "cnpg_collector_lo_pages"
)

// ParseCNPG reads CNPG's Prometheus text scraped at at. At is at for the built-ins, cnpg_collector_* but
// lo_pages, which CNPG recomputes on every scrape (pg_collector.go:320-343 at v1.30.1); for the others it is
// the text's cnpg_last_update_timestamp, when CNPG last ran its queries. The series are sorted by Name.
func ParseCNPG(text []byte, at time.Time) ([]Series, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(text))
	if err != nil {
		return nil, fmt.Errorf("parse the CNPG text: %w", err)
	}
	update := families[updateMetric].GetMetric()
	if len(update) == 0 {
		return nil, ErrNoUpdateTime
	}
	updated := UnixSeconds(update[0].GetGauge().GetValue())

	var out []Series
	for metric, mf := range families {
		when := updated
		if strings.HasPrefix(metric, builtinPrefix) && metric != loPages {
			when = at
		}
		for _, m := range mf.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			s := Series{Name: seriesName(metric, labels), Metric: metric, Labels: labels, At: when}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				s.Kind, s.Value = kindCounter, m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				s.Kind, s.Value = kindGauge, m.GetGauge().GetValue()
			case dto.MetricType_UNTYPED:
				s.Kind, s.Value = kindGauge, m.GetUntyped().GetValue()
			case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
				h := m.GetHistogram()
				s.Kind, s.Value, s.Sum = kindHistogram, float64(h.GetSampleCount()), h.GetSampleSum()
				if h.SampleCountFloat != nil {
					s.Value = h.GetSampleCountFloat()
				}
			case dto.MetricType_SUMMARY:
				// CNPG's one summary, go_gc_duration_seconds, is expected on its side only.
				s.Kind, s.Value, s.Sum = kindHistogram, float64(m.GetSummary().GetSampleCount()), m.GetSummary().GetSampleSum()
			}
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b Series) int { return strings.Compare(a.Name, b.Name) })

	return out, nil
}

// seriesName writes metric{k="v",...} with the labels sorted by key, or metric alone without labels.
func seriesName(metric string, labels map[string]string) string {
	if len(labels) == 0 {
		return metric
	}
	keys := slices.Sorted(maps.Keys(labels))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Quote(labels[k]))
	}

	return metric + "{" + strings.Join(parts, ",") + "}"
}

// UnixSeconds converts Unix seconds, as CNPG and our timestamp gauges write them, to a time.
func UnixSeconds(seconds float64) time.Time {
	whole := math.Floor(seconds)
	return time.Unix(int64(whole), int64((seconds-whole)*1e9))
}
