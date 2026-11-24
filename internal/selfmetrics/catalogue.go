// Package selfmetrics defines the self-metrics of the engine and the plugin and the store that feeds them.
package selfmetrics

import (
	_ "embed"
	"fmt"
	"io"
	"slices"
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

// Component is the process that reports a metric.
type Component string

// Components in the catalogue.
const (
	ComponentEngine Component = "engine"
	ComponentPlugin Component = "plugin"
)

// Metric describes one self-metric.
type Metric struct {
	Component   Component  `yaml:"component"`
	Name        string     `yaml:"name"`
	Instrument  Instrument `yaml:"instrument"`
	Unit        string     `yaml:"unit"`
	Description string     `yaml:"description"`
	Attributes  []string   `yaml:"attributes"`
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

// ByComponent returns the catalogue metrics of one component, in catalogue order.
func ByComponent(c Component) []Metric {
	var out []Metric
	for _, m := range Catalogue() {
		if m.Component == c {
			out = append(out, m)
		}
	}
	return out
}

type section struct {
	component Component
	heading   string
	intro     string
}

var sections = []section{
	{ComponentEngine, "Engine", "Metrics the sidecar in each instance Pod reports about itself."},
	{ComponentPlugin, "Plugin", "Metrics the plugin process reports about itself. They are exported only when `operator` gets `--metrics-endpoint`."},
}

// Render writes the Markdown catalogue to w, one section with a table per component.
func Render(w io.Writer, ms []Metric) error {
	for _, m := range ms {
		if !slices.ContainsFunc(sections, func(sec section) bool { return sec.component == m.Component }) {
			return fmt.Errorf("render catalogue: metric %q has component %q, which has no section", m.Name, m.Component)
		}
	}

	var b strings.Builder
	b.WriteString("# Self-metrics\n")
	for _, sec := range sections {
		var rows []Metric
		for _, m := range ms {
			if m.Component == sec.component {
				rows = append(rows, m)
			}
		}
		if len(rows) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n## %s\n\n%s\n\n", sec.heading, sec.intro)
		renderTable(&b, rows)
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("write catalogue: %w", err)
	}
	return nil
}

func renderTable(b *strings.Builder, ms []Metric) {
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
		fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s |\n", m.Name, m.Instrument, unit, attrs, m.Description)
	}
}
