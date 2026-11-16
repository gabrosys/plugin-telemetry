//go:build e2e

package plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/e2e/parity"
)

const (
	// cnpgQueriesTTL is CNPG's default metricsQueriesTTL (api/v1/cluster_types.go:2460-2466 at v1.30.1).
	cnpgQueriesTTL = 30 * time.Second
	// cnpgDefaultMonitoring is the ConfigMap of CNPG's default queries in the Cluster's namespace, key queries
	// (api/v1/cluster_defaults.go:36-44 at v1.30.1).
	cnpgDefaultMonitoring = "cnpg-default-monitoring"
	cnpgUpdated           = "cnpg_last_update_timestamp"
	parityPoll            = 500 * time.Millisecond
	reportDir             = "../../../bin/e2e"
)

// parityTarget is what the parity spec reads from its mode's container once BeforeAll has run.
type parityTarget struct {
	env       framework.Env
	c         client.Client
	cfg       *rest.Config
	reader    *framework.CollectorReader
	ns        string
	cluster   string
	mode      string
	sentinels []string // the values seeded in the mode's Secrets
}

// itParity adds AC7 to the mode's Ordered container; target is called when the spec runs.
func itParity(target func() parityTarget) {
	It("AC7: the parity report is produced", func(ctx SpecContext) {
		t := target()

		var cl apiv1.Cluster
		Expect(t.c.Get(ctx, client.ObjectKey{Namespace: t.ns, Name: t.cluster}, &cl)).To(Succeed())
		Expect(cl.Status.CurrentPrimary).NotTo(BeEmpty())
		primary := types.NamespacedName{Namespace: t.ns, Name: cl.Status.CurrentPrimary}

		var exporter v1alpha1.TelemetryExporter
		Expect(t.c.Get(ctx, client.ObjectKey{Namespace: t.ns, Name: exporterName}, &exporter)).To(Succeed())
		Expect(exporter.Spec.Metrics.Interval).NotTo(BeNil(), "spec.metrics.interval")

		o := parity.Options{LooseWALCount: t.env.CNPGVersion == "v1.30.1"}
		if t.mode == "password" {
			o.Skip = map[string]string{}
			for _, name := range []string{"idle", "interval", "count"} {
				o.Skip[`cnpg_pg_settings_setting{name="tcp_keepalives_`+name+`"}`] = "password-tcp-keepalives"
			}
		}

		checks := parityRound(ctx, t, primary, exporter.Spec.Metrics.Interval.Duration, parityIndex(ctx, t), o)

		report := parity.Markdown(fmt.Sprintf("Parity: CNPG %s, %s mode, primary %s", t.env.CNPGVersion, t.mode, primary.Name), checks)
		for i, s := range t.sentinels {
			// Not ContainSubstring: its failure message would print the value.
			Expect(strings.Contains(report, s)).To(BeFalse(), "the report holds seeded Secret value %d", i)
		}
		Expect(os.MkdirAll(reportDir, 0o750)).To(Succeed())
		path := filepath.Join(reportDir, fmt.Sprintf("parity-%s-%s.md", t.env.CNPGVersion, t.mode))
		Expect(os.WriteFile(path, []byte(report), 0o600)).To(Succeed())
		AddReportEntry("parity report", path)
	}, SpecTimeout(5*time.Minute))
}

// parityRound scrapes pod's CNPG endpoint, waits for an export of pod newer than that scrape by Margin in every
// query's last success, waits until the next scrape brackets that export, scrapes again and brackets it.
func parityRound(
	ctx SpecContext, t parityTarget, pod types.NamespacedName, interval time.Duration, index map[string][]string, o parity.Options,
) []parity.Check {
	GinkgoHelper()

	text, beforeAt, err := framework.ScrapeCNPG(ctx, t.cfg, pod)
	Expect(err).NotTo(HaveOccurred())
	before, err := parity.ParseCNPG(text, beforeAt)
	Expect(err).NotTo(HaveOccurred())

	var (
		lines  []otlp.Line
		newest time.Time
		window = 3*interval + cnpgQueriesTTL
	)
	Eventually(func(g Gomega, ctx SpecContext) {
		all, err := t.reader.Since(ctx, beforeAt)
		g.Expect(err).NotTo(HaveOccurred())
		podLines := otlp.ForResource(otlp.ForResource(all, "service.namespace", t.ns), "k8s.pod.name", pod.Name)
		var stale []string
		lines, newest, stale = parity.FreshExport(podLines, parity.Wait{
			After: beforeAt.Add(parity.Margin), ExportInterval: interval, Deadline: beforeAt.Add(window),
		})
		g.Expect(lines).NotTo(BeEmpty(), "no export of %s newer than %s; the newest one's stale: %v", pod.Name, beforeAt, stale)
	}).WithContext(ctx).WithTimeout(window).WithPolling(parityPoll).Should(Succeed())

	// CNPG writes its update time in whole seconds, so the after scrape starts at a whole second Margin past ours;
	// its cache is rerun on a scrape more than the TTL after the last update.
	updated := parity.UnixSeconds(seriesValue(before, cnpgUpdated))
	Expect(framework.WaitUntil(ctx, parity.Latest(
		newest.Add(parity.Margin).Truncate(time.Second).Add(time.Second), updated.Add(cnpgQueriesTTL+time.Second)))).To(Succeed())

	text, afterAt, err := framework.ScrapeCNPG(ctx, t.cfg, pod)
	Expect(err).NotTo(HaveOccurred())
	after, err := parity.ParseCNPG(text, afterAt)
	Expect(err).NotTo(HaveOccurred())

	AddReportEntry("parity round", fmt.Sprintf(
		"before scrape %s (cnpg_last_update_timestamp %s); our newest sample %s; after scrape %s (cnpg_last_update_timestamp %s)",
		beforeAt.UTC().Format(time.RFC3339Nano), updated.UTC().Format(time.RFC3339), newest.UTC().Format(time.RFC3339Nano),
		afterAt.UTC().Format(time.RFC3339Nano), parity.UnixSeconds(seriesValue(after, cnpgUpdated)).UTC().Format(time.RFC3339)))

	return parity.Bracket(before, after, parity.Ours(lines, pod.Name, index), o)
}

// parityIndex indexes the queries behind the metrics both sides export: the namespace's CNPG default
// monitoring, the suite's ConfigMaps and MetricQuerySet, and the built-ins.
func parityIndex(ctx context.Context, t parityTarget) map[string][]string {
	GinkgoHelper()

	parse := func(content []byte, what string) queries.UserQueries {
		qs, err := queries.Parse(content)
		Expect(err).NotTo(HaveOccurred(), what)
		return qs
	}

	var defaults corev1.ConfigMap
	Expect(t.c.Get(ctx, client.ObjectKey{Namespace: t.ns, Name: cnpgDefaultMonitoring}, &defaults)).To(Succeed())
	Expect(defaults.Data).To(HaveKey(queriesKey))
	sets := []queries.UserQueries{queries.Builtin(), parse([]byte(defaults.Data[queriesKey]), cnpgDefaultMonitoring)}

	for _, file := range []string{"configmap-base.yaml", "configmap-base-edited.yaml", "configmap-added.yaml", "configmap-added-edited.yaml"} {
		cm := configMap(t.ns, file)
		Expect(cm.Data).To(HaveKey(queriesKey), file)
		sets = append(sets, parse([]byte(cm.Data[queriesKey]), file))
	}
	set, err := json.Marshal(querySet(t.ns, "metricqueryset.yaml").Spec.Queries)
	Expect(err).NotTo(HaveOccurred())
	sets = append(sets, parse(set, "metricqueryset.yaml"))

	return parity.QueryIndex(sets...)
}

func seriesValue(series []parity.Series, name string) float64 {
	GinkgoHelper()

	for _, s := range series {
		if s.Name == name {
			return s.Value
		}
	}
	Fail("no series " + name)

	return 0
}
