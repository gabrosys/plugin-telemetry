package queries_test

import (
	"bytes"
	"encoding/json"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.yaml.in/yaml/v4"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

// configMapQueries returns data.queries of a CNPG default-monitoring ConfigMap.
func configMapQueries(configMap []byte) string {
	GinkgoHelper()

	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	Expect(yaml.Unmarshal(configMap, &cm)).To(Succeed())
	Expect(cm.Data).To(HaveKey("queries"))

	return cm.Data["queries"]
}

// defineOne parses a document holding the single query key and defines it.
func defineOne(key, document string, opts queries.Options) queries.Definition {
	GinkgoHelper()

	parsed, err := queries.Parse([]byte(document))
	Expect(err).ToNot(HaveOccurred())
	Expect(parsed).To(HaveKey(key))

	return queries.Define(key, parsed[key], opts)
}

// defineAll defines every query of the content plus the built-in one.
func defineAll(content string) map[string]queries.Definition {
	GinkgoHelper()

	parsed, err := queries.Parse([]byte(content))
	Expect(err).ToNot(HaveOccurred())

	for key, q := range queries.Builtin() {
		parsed[key] = q
	}

	result := make(map[string]queries.Definition, len(parsed))
	for key, q := range parsed {
		result[key] = queries.Define(key, q, queries.Options{MaxLabelsPerSeries: 32})
	}

	return result
}

type summaryMetric struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Usage string `json:"usage"`
}

type summary struct {
	Key             string          `json:"key"`
	Namespace       string          `json:"namespace"`
	Metrics         []summaryMetric `json:"metrics"`
	Labels          []string        `json:"labels"`
	PrimaryOnly     bool            `json:"primaryOnly"`
	RunOnServer     string          `json:"runOnServer"`
	TargetDatabases []string        `json:"targetDatabases"`
	CacheSeconds    uint64          `json:"cacheSeconds"`
	Warnings        []string        `json:"warnings"`
	Disabled        string          `json:"disabled"`
}

// marshalSummary renders the definitions sorted by key, with sorted object keys, two-space indent and a final newline.
func marshalSummary(defs map[string]queries.Definition) []byte {
	GinkgoHelper()

	keys := make([]string, 0, len(defs))
	for key := range defs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	list := make([]summary, 0, len(keys))
	for _, key := range keys {
		def := defs[key]
		s := summary{
			Key:             def.Key,
			Namespace:       def.Namespace,
			Metrics:         []summaryMetric{},
			Labels:          def.Labels,
			PrimaryOnly:     def.PrimaryOnly,
			TargetDatabases: def.TargetDatabases,
			CacheSeconds:    def.CacheSeconds,
			Warnings:        def.Warnings,
			Disabled:        def.Disabled,
		}
		if def.RunOnServer != nil {
			s.RunOnServer = def.RunOnServer.String()
		}
		for _, col := range def.Columns {
			if col.Role == queries.RoleValue {
				s.Metrics = append(s.Metrics, summaryMetric{Name: col.Metric, Kind: col.Kind.String(), Usage: string(col.Usage)})
			}
		}
		sort.Slice(s.Metrics, func(i, j int) bool { return s.Metrics[i].Name < s.Metrics[j].Name })
		list = append(list, s)
	}

	// A round trip through a generic value makes encoding/json sort the object keys.
	raw, err := json.Marshal(list)
	Expect(err).NotTo(HaveOccurred())

	var generic any
	Expect(json.Unmarshal(raw, &generic)).To(Succeed())

	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	Expect(enc.Encode(generic)).To(Succeed())

	return out.Bytes()
}
