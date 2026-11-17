//go:build e2e

package plugin_test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// strictFieldValidation is fieldValidation=Strict, which kubectl applies by default.
const strictFieldValidation = client.FieldValidation(metav1.FieldValidationStrict)

const (
	fixturesDir = "../../../api/v1alpha1/testdata"
	wantPrefix  = "# want: "
)

func fixtureEntries(dir string, run func(file string) TableEntry) []TableEntry {
	files, err := fs.Glob(os.DirFS(fixturesDir), path.Join(dir, "*.yaml"))
	Expect(err).NotTo(HaveOccurred())
	Expect(files).NotTo(BeEmpty())

	entries := make([]TableEntry, 0, len(files))
	for _, file := range files {
		entries = append(entries, run(file))
	}

	return entries
}

var _ = Describe("AC1: the CRDs", Ordered, func() {
	var strict client.Client

	// dryRun applies the fixture the way kubectl does: server-side dry-run with strict field validation.
	dryRun := func(ctx SpecContext, ns string, raw []byte) (*unstructured.Unstructured, error) {
		obj := &unstructured.Unstructured{}
		Expect(yaml.Unmarshal(raw, &obj.Object)).To(Succeed())
		obj.SetNamespace(ns)

		return obj, strict.Create(ctx, obj, client.DryRunAll)
	}

	var ns string

	BeforeAll(func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, _, err := framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		strict = client.WithFieldValidation(c, strictFieldValidation)

		namespace, err := framework.CreateNamespace(ctx, c, "plugin-crd")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
		})
		ns = namespace.Name
	})

	DescribeTable("the API server accepts every valid fixture",
		func(ctx SpecContext, file string) {
			raw, err := fs.ReadFile(os.DirFS(fixturesDir), file)
			Expect(err).NotTo(HaveOccurred())

			_, err = dryRun(ctx, ns, raw)
			Expect(err).NotTo(HaveOccurred())
		},
		func() []TableEntry {
			return fixtureEntries("valid", func(file string) TableEntry {
				return Entry(filepath.Base(file), file, SpecTimeout(time.Minute))
			})
		}(),
	)

	DescribeTable("the API server rejects every invalid fixture with its message",
		func(ctx SpecContext, file string) {
			raw, err := fs.ReadFile(os.DirFS(fixturesDir), file)
			Expect(err).NotTo(HaveOccurred())
			first, _, _ := strings.Cut(string(raw), "\n")
			Expect(first).To(HavePrefix(wantPrefix), "an invalid fixture starts with a want line")

			_, err = dryRun(ctx, ns, raw)
			Expect(err).To(MatchError(ContainSubstring(strings.TrimPrefix(first, wantPrefix))))
		},
		func() []TableEntry {
			return fixtureEntries("invalid", func(file string) TableEntry {
				return Entry(filepath.Base(file), file, SpecTimeout(time.Minute))
			})
		}(),
	)

	It("reports a rejected endpoint at spec.exporter.endpoint without its value", func(ctx SpecContext) {
		raw, err := fs.ReadFile(os.DirFS(fixturesDir), "invalid/exporter-endpoint-user-info.yaml")
		Expect(err).NotTo(HaveOccurred())

		_, err = dryRun(ctx, ns, raw)
		Expect(err).To(MatchError(ContainSubstring("spec.exporter.endpoint: ")))
		Expect(err.Error()).NotTo(ContainSubstring("secret"))
	}, SpecTimeout(time.Minute))

	It("keeps metric_mapping: {} in a MetricQuerySet", func(ctx SpecContext) {
		obj, err := dryRun(ctx, ns, fixture.Read(GinkgoTB(), "testdata", "metricqueryset-empty-mapping.yaml"))
		Expect(err).NotTo(HaveOccurred())

		metrics, found, err := unstructured.NestedSlice(obj.Object, "spec", "queries", "e2e_mapped", "metrics")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(metrics).To(HaveLen(1))
		column, ok := metrics[0].(map[string]any)["state"].(map[string]any)
		Expect(ok).To(BeTrue(), "metrics[0] = %v", metrics[0])
		Expect(column).To(HaveKeyWithValue("metric_mapping", BeEmpty()))
	}, SpecTimeout(time.Minute))
})
