package queries_test

import (
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

func FuzzParse(f *testing.F) {
	for _, name := range []string{
		"default-monitoring-v1.30.1.yaml",
		"default-monitoring-main.yaml",
		"cnpg-pg-exporter.yaml",
	} {
		data := fixture.Read(f, "testdata", name)
		f.Add(data)

		var cm struct {
			Data map[string]string `yaml:"data"`
		}
		if yaml.Unmarshal(data, &cm) == nil && cm.Data["queries"] != "" {
			f.Add([]byte(cm.Data["queries"]))
		}
	}

	f.Fuzz(func(t *testing.T, content []byte) {
		parsed, err := queries.Parse(content)
		if err != nil {
			return
		}

		for key, q := range parsed {
			first := queries.Define(key, q, queries.Options{MaxLabelsPerSeries: 32})
			second := queries.Define(key, q, queries.Options{MaxLabelsPerSeries: 32})
			if first.Fingerprint != second.Fingerprint {
				t.Fatalf("fingerprint of %q differs between calls: %s, %s", key, first.Fingerprint, second.Fingerprint)
			}
		}
	})
}
