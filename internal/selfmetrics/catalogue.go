// Package selfmetrics defines the engine's self-metrics and the store that feeds them.
package selfmetrics

import (
	_ "embed"
	"fmt"
	"io"
	"strings"
	"sync"

	"go.yaml.in/yaml/v4"
)

// Instrument is the kind of OpenTelemetry instrument a metric uses.
type Instrument string

// Instrument kinds used in the catalogue.
const (
	InstrumentCounter   Instrument = "counter"
	InstrumentGauge     Instrument = "gauge"
	InstrumentHistogram Instrument = "histogram"
)

// Metric describes one self-metric.
type Metric struct {
	Name        string   `yaml:"name"`
	Instrument  string   `yaml:"instrument"`
	Unit        string   `yaml:"unit"`
	Description string   `yaml:"description"`
	Attributes  []string `yaml:"attributes"`
}

//go:embed catalogue.yaml
var catalogueYAML []byte

var catalogue = sync.OnceValues(func() ([]Metric, error) {
	var ms []Metric
	if err := yaml.Unmarshal(catalogueYAML, &ms); err != nil {
		return nil, fmt.Errorf("parse catalogue.yaml: %w", err)
	}
	return ms, nil
})

// Catalogue returns the metrics parsed from the embedded catalogue.yaml.
// The embedded file is validated by the package tests; a parse failure is a programming error.
func Catalogue() []Metric {
	ms, err := catalogue()
	if err != nil {
		panic(err)
	}
	out := make([]Metric, len(ms))
	copy(out, ms)
	return out
}

// Render writes the Markdown catalogue table to w.
func Render(w io.Writer, ms []Metric) error {
	var b strings.Builder
	b.WriteString("# Self-metrics\n\nMetrics the engine reports about itself.\n\n")
	b.WriteString("| Name | Instrument | Unit | Attributes | Description |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, m := range ms {
		unit, attrs := "", ""
		if m.Unit != "" {
			unit = "`" + m.Unit + "`"
		}
		if len(m.Attributes) > 0 {
			attrs = "`" + strings.Join(m.Attributes, "`, `") + "`"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s |\n", m.Name, m.Instrument, unit, attrs, m.Description)
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("write catalogue: %w", err)
	}
	return nil
}
