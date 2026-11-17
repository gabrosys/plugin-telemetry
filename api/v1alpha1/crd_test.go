package v1alpha1_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsinstall "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

const (
	crdDir        = "../../config/crd"
	exporterCRD   = "telemetry.gabrosys.github.io_telemetryexporters.yaml"
	querySetCRD   = "telemetry.gabrosys.github.io_metricquerysets.yaml"
	cnpgQueryData = "../../internal/queries/testdata"
	wantPrefix    = "# want: "
)

// crd is a generated CRD loaded the way the API server builds its validators.
type crd struct {
	structural *structuralschema.Structural
	openapi    apiservervalidation.SchemaValidator
	cel        *cel.Validator
}

func readFile(dir, name string) []byte {
	GinkgoHelper()

	root, err := os.OpenRoot(dir)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)

	data, err := root.ReadFile(name)
	Expect(err).NotTo(HaveOccurred())

	return data
}

func loadCRD(name string) *crd {
	GinkgoHelper()

	scheme := runtime.NewScheme()
	apiextensionsinstall.Install(scheme)

	var external apiextensionsv1.CustomResourceDefinition
	Expect(yaml.UnmarshalStrict(readFile(crdDir, name), &external)).To(Succeed())
	scheme.Default(&external)

	var internal apiextensions.CustomResourceDefinition
	Expect(scheme.Convert(&external, &internal, nil)).To(Succeed())
	internal.Status.StoredVersions = []string{v1alpha1.GroupVersion.Version}

	Expect(apiextensionsvalidation.ValidateCustomResourceDefinition(context.Background(), &internal)).
		To(BeEmpty(), "the apiserver would reject the CRD (structural schema, CEL compilation, cost limits)")

	props, err := apiextensions.GetSchemaForVersion(&internal, v1alpha1.GroupVersion.Version)
	Expect(err).NotTo(HaveOccurred())
	Expect(props).NotTo(BeNil())

	structural, err := structuralschema.NewStructural(props.OpenAPIV3Schema)
	Expect(err).NotTo(HaveOccurred())
	Expect(structuralschema.ValidateStructural(nil, structural)).To(BeEmpty())

	openapi, _, err := apiservervalidation.NewSchemaValidator(props.OpenAPIV3Schema)
	Expect(err).NotTo(HaveOccurred())

	return &crd{
		structural: structural,
		openapi:    openapi,
		cel:        cel.NewValidator(structural, true, celconfig.PerCallLimit),
	}
}

// create runs what the API server runs on create: pruning, OpenAPI validation, list-type checks, then CEL.
func (c *crd) create(object []byte) []string {
	GinkgoHelper()

	var obj map[string]any
	jsonBytes, err := yaml.YAMLToJSON(object)
	Expect(err).NotTo(HaveOccurred())
	Expect(utiljson.Unmarshal(jsonBytes, &obj)).To(Succeed())

	var problems []string
	pruned := pruning.PruneWithOptions(obj, c.structural, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: true})
	for _, path := range pruned {
		problems = append(problems, fmt.Sprintf("unknown field %q", path))
	}

	errs := apiservervalidation.ValidateCustomResource(nil, obj, c.openapi)
	errs = append(errs, listtype.ValidateListSetsAndMaps(nil, c.structural, obj)...)
	if len(errs) == 0 {
		celErrs, _ := c.cel.Validate(context.Background(), nil, c.structural, obj, nil, celconfig.RuntimeCELCostBudget)
		errs = append(errs, celErrs...)
	}

	return append(problems, errorStrings(errs)...)
}

func errorStrings(errs field.ErrorList) []string {
	out := make([]string, 0, len(errs))
	for _, err := range errs {
		out = append(out, err.Error())
	}

	return out
}

func crdFor(kind string) string {
	if kind == "MetricQuerySet" {
		return querySetCRD
	}
	Expect(kind).To(Equal("TelemetryExporter"), "no CRD for kind %s", kind)

	return exporterCRD
}

func fixtures(dir string) []string {
	GinkgoHelper()

	files, err := fs.Glob(os.DirFS("testdata"), path.Join(dir, "*.yaml"))
	Expect(err).NotTo(HaveOccurred())
	Expect(files).NotTo(BeEmpty())

	return files
}

func kindOf(object []byte) string {
	GinkgoHelper()

	var meta struct {
		Kind string `json:"kind"`
	}
	Expect(yaml.Unmarshal(object, &meta)).To(Succeed())

	return meta.Kind
}

func wantOf(object []byte) string {
	GinkgoHelper()

	first, _, _ := strings.Cut(string(object), "\n")
	Expect(first).To(HavePrefix(wantPrefix), "an invalid fixture starts with a want line")

	return strings.TrimPrefix(first, wantPrefix)
}

var _ = Describe("the generated CRDs", func() {
	It("generates CRDs that are structural and whose CEL rules fit the cost limits", func() {
		Expect(loadCRD(exporterCRD)).NotTo(BeNil())
		Expect(loadCRD(querySetCRD)).NotTo(BeNil())
	})

	DescribeTable("accepts every valid fixture",
		func(file string) {
			object := readFile("testdata", file)

			Expect(loadCRD(crdFor(kindOf(object))).create(object)).To(BeEmpty())
		},
		func() []TableEntry {
			var entries []TableEntry
			for _, file := range fixtures("valid") {
				entries = append(entries, Entry(filepath.Base(file), file))
			}

			return entries
		}(),
	)

	DescribeTable("rejects every invalid fixture with its message",
		func(file string) {
			object := readFile("testdata", file)

			problems := loadCRD(crdFor(kindOf(object))).create(object)
			Expect(problems).To(HaveExactElements(ContainSubstring(wantOf(object))))
		},
		func() []TableEntry {
			var entries []TableEntry
			for _, file := range fixtures("invalid") {
				entries = append(entries, Entry(filepath.Base(file), file))
			}

			return entries
		}(),
	)

	It("fails on a kind it has no CRD for", func() {
		Expect(InterceptGomegaFailure(func() { crdFor("Pooler") })).NotTo(Succeed())
	})

	It("reports a rejected endpoint at spec.exporter.endpoint without its value", func() {
		problems := loadCRD(exporterCRD).create(readFile("testdata/invalid", "exporter-endpoint-user-info.yaml"))

		Expect(problems).To(ContainElement(HavePrefix("spec.exporter.endpoint: ")))
		Expect(strings.Join(problems, "\n")).NotTo(ContainSubstring("secret"))
	})

	DescribeTable("accepts CNPG's default queries wrapped as a MetricQuerySet",
		func(version string) {
			var configMap struct {
				Data map[string]string `json:"data"`
			}
			Expect(yaml.Unmarshal(readFile(cnpgQueryData, "default-monitoring-"+version+".yaml"), &configMap)).To(Succeed())
			cnpgYAML := configMap.Data["queries"]
			Expect(cnpgYAML).NotTo(BeEmpty())

			var rawQueries map[string]any
			Expect(yaml.Unmarshal([]byte(cnpgYAML), &rawQueries)).To(Succeed())
			object, err := json.Marshal(map[string]any{
				"apiVersion": v1alpha1.GroupVersion.String(),
				"kind":       "MetricQuerySet",
				"metadata":   map[string]any{"name": "cnpg-defaults", "namespace": "app"},
				"spec":       map[string]any{"queries": rawQueries},
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(loadCRD(querySetCRD).create(object)).To(BeEmpty())

			var typed map[string]v1alpha1.Query
			Expect(yaml.UnmarshalStrict([]byte(cnpgYAML), &typed)).To(Succeed())
			roundTripped, err := json.Marshal(typed)
			Expect(err).NotTo(HaveOccurred())

			Expect(definitions([]byte(cnpgYAML))).To(Equal(definitions(roundTripped)))
		},
		Entry("v1.30.1", "v1.30.1"),
		Entry("main", "main"),
	)

	It("round-trips a TelemetryExporter through JSON without losing fields", func() {
		original := readFile("testdata/valid", "exporter-full.yaml")

		var typed v1alpha1.TelemetryExporter
		Expect(yaml.UnmarshalStrict(original, &typed)).To(Succeed())
		marshalled, err := json.Marshal(typed)
		Expect(err).NotTo(HaveOccurred())

		Expect(specOf(marshalled)).To(Equal(specOf(original)))
	})

	It("round-trips a query set that spells the mapping metrics_mapping", func() {
		var object struct {
			Spec struct {
				Queries map[string]any `json:"queries"`
			} `json:"spec"`
		}
		Expect(yaml.Unmarshal(readFile("testdata", "valid/query-set-metrics-mapping.yaml"), &object)).To(Succeed())
		original, err := yaml.Marshal(object.Spec.Queries)
		Expect(err).NotTo(HaveOccurred())

		var typed map[string]v1alpha1.Query
		Expect(yaml.UnmarshalStrict(original, &typed)).To(Succeed())
		roundTripped, err := json.Marshal(typed)
		Expect(err).NotTo(HaveOccurred())

		Expect(definitions(roundTripped)).To(Equal(definitions(original)))
		Expect(definitions(original)[0].Columns["state"].Mapping).NotTo(BeEmpty())
	})

	It("every CRD property has a description", func() {
		files, err := fs.Glob(os.DirFS(crdDir), "telemetry.gabrosys.github.io_*.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(files).NotTo(BeEmpty())

		var missing []string
		for _, name := range files {
			var external apiextensionsv1.CustomResourceDefinition
			Expect(yaml.UnmarshalStrict(readFile(crdDir, name), &external)).To(Succeed())

			for _, version := range external.Spec.Versions {
				Expect(version.Schema).NotTo(BeNil(), "%s %s has no schema", name, version.Name)
				missing = append(missing, undescribed(name+" "+version.Name, version.Schema.OpenAPIV3Schema, true)...)
			}
		}

		Expect(missing).To(BeEmpty(), "properties without a description")
	})

	It("registers its kinds in a scheme", func() {
		scheme := runtime.NewScheme()
		Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())
		Expect(scheme.Recognizes(v1alpha1.GroupVersion.WithKind("TelemetryExporter"))).To(BeTrue())
		Expect(scheme.Recognizes(v1alpha1.GroupVersion.WithKind("MetricQuerySetList"))).To(BeTrue())
	})
})

// undescribed returns the JSON path of every named property under schema that has no description. It
// walks into properties, items and additionalProperties. Only a named property is checked: the schema of
// an array item or a map value is described by the field that holds it. The root skips apiVersion, kind
// and metadata, and everything under status.
func undescribed(where string, schema *apiextensionsv1.JSONSchemaProps, root bool) []string {
	var missing []string

	for _, name := range sortedKeys(schema.Properties) {
		if root && (name == "apiVersion" || name == "kind" || name == "metadata" || name == "status") {
			continue
		}

		child := schema.Properties[name]
		path := where + "." + name
		if strings.TrimSpace(child.Description) == "" {
			missing = append(missing, path)
		}
		missing = append(missing, undescribed(path, &child, false)...)
	}

	if schema.Items != nil && schema.Items.Schema != nil {
		missing = append(missing, undescribed(where+"[]", schema.Items.Schema, false)...)
	}

	if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
		missing = append(missing, undescribed(where+".*", schema.AdditionalProperties.Schema, false)...)
	}

	return missing
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

func specOf(object []byte) any {
	GinkgoHelper()

	jsonBytes, err := yaml.YAMLToJSON(object)
	Expect(err).NotTo(HaveOccurred())

	var parsed map[string]any
	Expect(json.Unmarshal(jsonBytes, &parsed)).To(Succeed())

	return parsed["spec"]
}

// definitions parses queries and returns their Definitions ordered by key.
func definitions(content []byte) []queries.Definition {
	GinkgoHelper()

	parsed, err := queries.Parse(content)
	Expect(err).NotTo(HaveOccurred())

	out := make([]queries.Definition, 0, len(parsed))
	for key, query := range parsed {
		out = append(out, queries.Define(key, query, queries.Options{}))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}
