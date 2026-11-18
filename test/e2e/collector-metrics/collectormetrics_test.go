//go:build e2e

package collectormetrics_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/e2e/parity"
)

const (
	exporterName = "cm"
	mainName     = "cm-main"
	replicaName  = "cm-replica"
	firstPod     = mainName + "-1"
	secondPod    = mainName + "-2"
	replicaPod   = replicaName + "-1"

	// postgresContainer is CNPG's PostgreSQL container (pkg/specs/pods.go:78 at v1.30.1).
	postgresContainer = "postgres"
	// fencedAnnotation is CNPG's fencing annotation (pkg/utils/labels_annotations.go:238 at v1.30.1).
	fencedAnnotation = "cnpg.io/fencedInstances"

	builtinPrefix  = "cnpg_collector_"
	exportInterval = 5 * time.Second
	reportDir      = "../../../bin/e2e"
	clusterTimeout = 10 * time.Minute
	freshTimeout   = 3 * time.Minute
	statePoll      = 2 * time.Second
	freshPoll      = 500 * time.Millisecond

	fencedFrozen = "fenced-frozen"
	// primaryOnlyOnStandby: CNPG sets sync_replicas{observed} and the backup timestamps only on a primary
	// (v1.30.1 pkg/management/postgres/webserver/metricserver/pg_collector.go:445-457) and
	// manual_switchover_required only on the current primary (internal/management/controller/instance_controller.go:1014-1020).
	primaryOnlyOnStandby = "primary-only-on-standby"
)

// backupTimestamps stay 0 in e2e: no state takes a backup.
var backupTimestamps = []string{
	"first_recoverability_point", "last_available_backup_timestamp", "last_failed_backup_timestamp",
}

// selfMeasuring are CNPG's metrics that measure its collector (pg_collector.go:112-141 at v1.30.1).
var selfMeasuring = []string{
	"collections_total", "collection_errors_total", "last_collection_error", "collection_duration_seconds",
}

// tallied is CNPG's list of built-in series on PostgreSQL 18 (pg_collector.go:112-272, 320-393 and wal.go:36-61,
// 151-222 at v1.30.1), by metric without the cnpg_collector_ prefix and, for the three metrics with a value label,
// by that label.
var tallied = slices.Concat([]string{
	"up", "postgres_version",
	"pg_wal{count}", "pg_wal{size}", "pg_wal{min}", "pg_wal{max}", "pg_wal{keep}", "pg_wal{slots_max}",
	"pg_wal{volume_size}", "pg_wal{volume_max}",
	"pg_wal_archive_status{ready}", "pg_wal_archive_status{done}",
	"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
	"sync_replicas{min}", "sync_replicas{max}", "sync_replicas{expected}", "sync_replicas{observed}",
	"replica_mode", "fencing_on", "manual_switchover_required", "nodes_used", "lo_pages",
}, backupTimestamps, selfMeasuring)

// frozenMetrics are the PostgreSQL-derived metrics CNPG keeps at their last value on a fenced instance
// (pg_collector.go:402-409 at v1.30.1); sync_replicas{observed} is checked by series (observedSeries).
var frozenMetrics = []string{
	"up", "pg_wal", "pg_wal_archive_status", "wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
	"postgres_version", "lo_pages",
}

const observedSeries = builtinPrefix + `sync_replicas{value="observed"}`

// setupSQL fills cm-main so that wal_fpi, wal_buffers_full, lo_pages and pg_wal_archive_status{done} are non-zero.
// The last CHECKPOINT leaves few dirty buffers for the spread checkpoint cm-replica's pg_basebackup waits for
// (pkg/management/postgres/join.go:56-61 at v1.30.1).
var setupSQL = []string{
	"CREATE TABLE cm (i int)",
	"INSERT INTO cm SELECT generate_series(1, 200000)",
	"CHECKPOINT",
	"UPDATE cm SET i = i + 1",
	"SELECT lo_from_bytea(0, decode(repeat('ab', 100000), 'hex'))",
	"VACUUM ANALYZE pg_largeobject",
	"CHECKPOINT",
	"SELECT pg_switch_wal()",
}

// tally records, per entry of tallied, whether each side had it non-zero in some check.
type tally struct{ ours, cnpg map[string]bool }

func (t tally) add(side map[string]bool, series []parity.Series, skip map[string]string) {
	for _, s := range series {
		if skip[s.Name] != "" || skip[s.Metric] != "" {
			continue
		}
		key := strings.TrimPrefix(s.Metric, builtinPrefix)
		if v, ok := s.Labels["value"]; ok && key != s.Metric {
			key += "{" + v + "}"
		}
		if slices.Contains(tallied, key) && s.Value != 0 && !math.IsNaN(s.Value) {
			side[key] = true
		}
	}
}

// zero returns the tallied entries side never had non-zero, sorted.
func zero(side map[string]bool) []string {
	var out []string
	for _, key := range tallied {
		if !side[key] {
			out = append(out, key)
		}
	}
	slices.Sort(out)

	return out
}

func monitoring() *apiv1.MonitoringConfiguration {
	return &apiv1.MonitoringConfiguration{DisableDefaultQueries: ptr.To(true), MetricsQueriesTTL: &metav1.Duration{}}
}

func secretKey(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key}
}

// valueOf returns the value of the series called name; g fails without one.
func valueOf(g Gomega, series []parity.Series, name string) float64 {
	GinkgoHelper()

	i := slices.IndexFunc(series, func(s parity.Series) bool { return s.Name == name })
	g.Expect(i).To(BeNumerically(">=", 0), "no series %s", name)
	if i < 0 {
		return 0
	}

	return series[i].Value
}

var _ = Describe("collector-metrics", Ordered, func() {
	var (
		env    framework.Env
		c      client.Client
		cfg    *rest.Config
		reader *framework.CollectorReader
		ns     string
		index  = parity.QueryIndex(queries.Builtin())
		counts = tally{ours: map[string]bool{}, cnpg: map[string]bool{}}
	)

	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }
	podKey := func(name string) types.NamespacedName { return types.NamespacedName{Namespace: ns, Name: name} }

	getCluster := func(g Gomega, ctx context.Context, name string) *apiv1.Cluster {
		GinkgoHelper()

		var cl apiv1.Cluster
		g.Expect(c.Get(ctx, key(name), &cl)).To(Succeed())

		return &cl
	}

	clusterReady := func(ctx SpecContext, name string, timeout time.Duration) {
		GinkgoHelper()

		Eventually(func(ctx context.Context) (bool, error) {
			return framework.ClusterReady(ctx, c, key(name))
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(statePoll).Should(BeTrue(), "cluster %s ready", name)
	}

	podReady := func(g Gomega, ctx context.Context, name string) bool {
		var pod corev1.Pod
		g.Expect(c.Get(ctx, key(name), &pod)).To(Succeed())
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady {
				return cond.Status == corev1.ConditionTrue
			}
		}

		return false
	}

	patchCluster := func(ctx SpecContext, name string, patch map[string]any) {
		GinkgoHelper()

		body, err := json.Marshal(patch)
		Expect(err).NotTo(HaveOccurred())
		cl := &apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
		Eventually(func(ctx context.Context) error {
			return c.Patch(ctx, cl, client.RawPatch(types.MergePatchType, body))
		}).WithContext(ctx).WithTimeout(30*time.Second).WithPolling(time.Second).Should(Succeed(), "patch cluster %s", name)
	}

	psql := func(ctx SpecContext, pod string, statements ...string) {
		GinkgoHelper()

		command := []string{"psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"}
		for _, s := range statements {
			command = append(command, "-c", s)
		}
		_, stderr, err := framework.Exec(ctx, cfg, podKey(pod), postgresContainer, command)
		Expect(err).NotTo(HaveOccurred(), "psql in %s: %s", pod, stderr)
	}

	scrape := func(ctx context.Context, pod string) ([]parity.Series, time.Time) {
		GinkgoHelper()

		text, at, err := framework.ScrapeCNPG(ctx, cfg, podKey(pod))
		Expect(err).NotTo(HaveOccurred())
		series, err := parity.ParseCNPG(text, at)
		Expect(err).NotTo(HaveOccurred())

		return series, at
	}

	podLines := func(g Gomega, ctx context.Context, pod string, since time.Time) []otlp.Line {
		all, err := reader.Since(ctx, since)
		g.Expect(err).NotTo(HaveOccurred())

		return otlp.ForResource(otlp.ForResource(all, "service.namespace", ns), "k8s.pod.name", pod)
	}

	// round scrapes pod's CNPG endpoint, waits for an export of pod whose named queries last succeeded Margin
	// after that scrape, scrapes again past the newest of those times and brackets our samples.
	round := func(ctx SpecContext, pod string, named []string, o parity.Options) ([]parity.Check, []parity.Series) {
		GinkgoHelper()

		before, beforeAt := scrape(ctx, pod)
		var (
			lines  []otlp.Line
			newest time.Time
		)
		Eventually(func(g Gomega, ctx SpecContext) {
			var stale []string
			lines, newest, stale = parity.FreshExport(podLines(g, ctx, pod, beforeAt), parity.Wait{
				After: beforeAt.Add(parity.Margin), Required: named, ExportInterval: exportInterval, Deadline: beforeAt.Add(freshTimeout),
			})
			g.Expect(lines).NotTo(BeEmpty(), "no export of %s newer than %s; the newest one's stale: %v", pod, beforeAt, stale)
		}).WithContext(ctx).WithTimeout(freshTimeout).WithPolling(freshPoll).Should(Succeed())

		// CNPG writes lo_pages' update time in whole seconds, so the after scrape starts at a whole second.
		Expect(framework.WaitUntil(ctx, newest.Add(parity.Margin).Truncate(time.Second).Add(time.Second))).To(Succeed())
		after, _ := scrape(ctx, pod)

		ours := parity.Ours(lines, pod, index)
		counts.add(counts.ours, ours, o.Skip)
		counts.add(counts.cnpg, slices.Concat(before, after), o.Skip)

		return parity.Bracket(before, after, ours, o), ours
	}

	// judge runs a round on pod of cluster in state, and one more when a gauge fails, writes the report and fails
	// on a gated check that does not pass. It returns our samples of the last round.
	judge := func(ctx SpecContext, state, cluster, pod string, fenced bool) []parity.Series {
		GinkgoHelper()

		cl := getCluster(Default, ctx, cluster)
		replica := cl.Spec.ReplicaCluster != nil && ptr.Deref(cl.Spec.ReplicaCluster.Enabled, false)
		primary := cl.Status.CurrentPrimary == pod && !replica
		skip := map[string]string{}
		named := []string{queries.KeyLOPages, queries.KeyPGWAL, queries.KeyArchiveStatus, queries.KeyStatWAL}
		if primary {
			named = append(named, queries.KeySyncObserved)
		} else {
			skip[observedSeries] = primaryOnlyOnStandby
			for _, m := range backupTimestamps {
				skip[builtinPrefix+m] = primaryOnlyOnStandby
			}
		}
		if cl.Status.CurrentPrimary != pod {
			skip[builtinPrefix+"manual_switchover_required"] = primaryOnlyOnStandby
		}
		if fenced {
			named = nil
			skip[observedSeries] = fencedFrozen
			for _, m := range frozenMetrics {
				skip[builtinPrefix+m] = fencedFrozen
			}
		}
		o := parity.Options{
			Gate:          func(metric string) bool { return strings.HasPrefix(metric, builtinPrefix) },
			Skip:          skip,
			LooseWALCount: env.CNPGVersion == "v1.30.1",
		}

		title := fmt.Sprintf("Collector metrics: CNPG %s, %s, %s", env.CNPGVersion, state, pod)
		checks, ours := round(ctx, pod, named, o)
		failures := parity.Failures(checks)
		report := parity.Markdown(title, checks)
		if slices.ContainsFunc(failures, func(c parity.Check) bool { return c.Kind == "gauge" }) {
			var again []parity.Check
			again, ours = round(ctx, pod, named, o)
			failures = parity.Retry(checks, again)
			report += "\n" + parity.Markdown(title+", second round", again)
		}

		Expect(os.MkdirAll(reportDir, 0o750)).To(Succeed())
		path := filepath.Join(reportDir, fmt.Sprintf("collector-metrics-%s-%s-%s.md", env.CNPGVersion, state, pod))
		Expect(os.WriteFile(path, []byte(report), 0o600)).To(Succeed())
		AddReportEntry("collector-metrics report", path)
		Expect(failures).To(BeEmpty(), report)

		return ours
	}

	BeforeAll(func(ctx SpecContext) {
		var err error
		env, err = framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		reader = framework.NewCollectorReader(cfg)

		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())
		Expect(framework.CollectorReady(ctx, c)).To(Succeed())

		namespace, err := framework.CreateNamespace(ctx, c, "cm")
		Expect(err).NotTo(HaveOccurred())
		ns = namespace.Name
		DeferCleanup(func(ctx SpecContext) {
			Expect(framework.DeleteNamespace(ctx, c, ns, statePoll)).To(Succeed())
		}, NodeTimeout(4*time.Minute))

		exporter := framework.ExporterFor(ns, exporterName)
		exporter.Spec.Metrics.Interval = &metav1.Duration{Duration: exportInterval}
		exporter.Spec.Metrics.DefaultInterval = &metav1.Duration{Duration: 5 * time.Second}
		exporter.Spec.Metrics.PlanInterval = &metav1.Duration{Duration: 10 * time.Second}
		exporter.Spec.Metrics.UseClusterMonitoring = ptr.To(false)
		Expect(c.Create(ctx, exporter)).To(Succeed())

		create := func(cl *apiv1.Cluster) {
			Expect(c.Create(ctx, cl)).To(Succeed())
		}

		primary := framework.WithExporter(framework.PeerCluster(ns, mainName, env.PostgresImage), exporterName)
		primary.Spec.Instances = 2
		primary.Spec.MinSyncReplicas, primary.Spec.MaxSyncReplicas = 1, 1
		primary.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "1Gi"}
		primary.Spec.PostgresConfiguration.Parameters = map[string]string{"wal_buffers": "32kB", "max_slot_wal_keep_size": "1GB"}
		primary.Spec.Monitoring = monitoring()
		create(primary)
		clusterReady(ctx, mainName, clusterTimeout)
		psql(ctx, getCluster(Default, ctx, mainName).Status.CurrentPrimary, setupSQL...)

		replica := framework.WithExporter(framework.PeerCluster(ns, replicaName, env.PostgresImage), exporterName)
		replica.Spec.Bootstrap = &apiv1.BootstrapConfiguration{PgBaseBackup: &apiv1.BootstrapPgBaseBackup{Source: mainName}}
		replica.Spec.ReplicaCluster = &apiv1.ReplicaClusterConfiguration{Enabled: ptr.To(true), Source: mainName}
		replica.Spec.ExternalClusters = []apiv1.ExternalCluster{{
			Name: mainName,
			ConnectionParameters: map[string]string{
				"host": mainName + "-rw", "user": "streaming_replica", "sslmode": "verify-full", "dbname": "postgres",
			},
			SSLKey:      secretKey(mainName+apiv1.ReplicationSecretSuffix, corev1.TLSPrivateKeyKey),
			SSLCert:     secretKey(mainName+apiv1.ReplicationSecretSuffix, corev1.TLSCertKey),
			SSLRootCert: secretKey(mainName+apiv1.DefaultServerCaSecretSuffix, "ca.crt"),
		}}
		replica.Spec.Monitoring = monitoring()
		create(replica)
	}, NodeTimeout(clusterTimeout+3*time.Minute))

	// A failed spec reports what the sidecars logged.
	JustAfterEach(func(ctx SpecContext) {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, cluster := range []string{mainName, replicaName} {
			pods, err := framework.InstancePods(ctx, c, key(cluster))
			if err != nil {
				AddReportEntry("instance pods of "+cluster, err.Error())
				continue
			}
			for _, pod := range pods {
				logs, err := framework.ContainerLogs(ctx, cfg, podKey(pod.Name), metadata.SidecarContainerName,
					&corev1.PodLogOptions{TailLines: ptr.To[int64](200)})
				if err != nil {
					logs = err.Error()
				}
				AddReportEntry("sidecar logs of "+pod.Name, logs)
			}
		}
	}, NodeTimeout(time.Minute))

	It("plain, synchronous: every built-in of the primary and the standby matches CNPG", func(ctx SpecContext) {
		cl := getCluster(Default, ctx, mainName)
		Expect(cl.Status.CurrentPrimary).To(Equal(firstPod))
		judge(ctx, "plain", mainName, firstPod, false)
		judge(ctx, "plain", mainName, secondPod, false)
	}, SpecTimeout(3*time.Minute))

	It("replica cluster: replica_mode is 1 and every built-in of the designated primary matches CNPG", func(ctx SpecContext) {
		clusterReady(ctx, replicaName, 2*time.Minute)
		ours := judge(ctx, "replica-cluster", replicaName, replicaPod, false)
		Expect(valueOf(Default, ours, builtinPrefix+"replica_mode")).To(Equal(1.0))
	}, SpecTimeout(3*time.Minute))

	It("fencing: fencing_on is 1 and the fenced instance's PostgreSQL metrics stop", func(ctx SpecContext) {
		fencedAt := time.Now()
		patchCluster(ctx, mainName, map[string]any{"metadata": map[string]any{
			"annotations": map[string]any{fencedAnnotation: `["` + secondPod + `"]`},
		}})
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(podReady(g, ctx, secondPod)).To(BeFalse(), "%s ready", secondPod)
			up, ok := otlp.Last(podLines(g, ctx, secondPod, fencedAt), builtinPrefix+"up", nil)
			g.Expect(ok).To(BeTrue(), "no up from %s since the fencing", secondPod)
			g.Expect(up.Value).To(Equal(0.0))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(statePoll).Should(Succeed())

		ours := judge(ctx, "fencing", mainName, secondPod, true)
		Expect(valueOf(Default, ours, builtinPrefix+`up{cluster="`+mainName+`"}`)).To(Equal(0.0))
		Expect(valueOf(Default, ours, builtinPrefix+"fencing_on")).To(Equal(1.0))
		var frozen []string
		for _, s := range ours {
			if s.Name == observedSeries ||
				(s.Metric != builtinPrefix+"up" && slices.Contains(frozenMetrics, strings.TrimPrefix(s.Metric, builtinPrefix))) {
				frozen = append(frozen, s.Name)
			}
		}
		Expect(frozen).To(BeEmpty(), "PostgreSQL-derived series from the fenced instance")

		patchCluster(ctx, mainName, map[string]any{"metadata": map[string]any{
			"annotations": map[string]any{fencedAnnotation: nil},
		}})
		clusterReady(ctx, mainName, 3*time.Minute)
	}, SpecTimeout(6*time.Minute))

	It("supervised update: manual_switchover_required is 1 on the primary waiting for a switchover", func(ctx SpecContext) {
		// A hot standby needs max_connections at least the primary's, or its recovery pauses.
		patchCluster(ctx, replicaName, map[string]any{"spec": map[string]any{
			"postgresql": map[string]any{"parameters": map[string]any{"max_connections": "110"}},
		}})
		patchCluster(ctx, mainName, map[string]any{"spec": map[string]any{
			"primaryUpdateStrategy": string(apiv1.PrimaryUpdateStrategySupervised),
			"postgresql":            map[string]any{"parameters": map[string]any{"max_connections": "110"}},
		}})
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(getCluster(g, ctx, mainName).Status.Phase).To(Equal(apiv1.PhaseWaitingForUser))
			g.Expect(podReady(g, ctx, secondPod)).To(BeTrue(), "%s ready", secondPod)
		}).WithContext(ctx).WithTimeout(4 * time.Minute).WithPolling(statePoll).Should(Succeed())

		ours := judge(ctx, "supervised-update", mainName, firstPod, false)
		Expect(valueOf(Default, ours, builtinPrefix+"manual_switchover_required")).To(Equal(1.0))
	}, SpecTimeout(6*time.Minute))

	It("switchover: the new primary and the demoted one match CNPG", func(ctx SpecContext) {
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var cl apiv1.Cluster
			if err := c.Get(ctx, key(mainName), &cl); err != nil {
				return fmt.Errorf("get cluster %s: %w", mainName, err)
			}
			return framework.Switchover(ctx, c, &cl, secondPod)
		})).To(Succeed())
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(getCluster(g, ctx, mainName).Status.CurrentPrimary).To(Equal(secondPod))
			ready, err := framework.ClusterReady(ctx, c, key(mainName))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(BeTrue(), "cluster %s ready", mainName)
		}).WithContext(ctx).WithTimeout(4 * time.Minute).WithPolling(statePoll).Should(Succeed())

		judge(ctx, "switchover", mainName, firstPod, false)
		judge(ctx, "switchover", mainName, secondPod, false)
	}, SpecTimeout(6*time.Minute))

	It("failing archive: pg_wal_archive_status{ready} counts the segments waiting for the archive", func(ctx SpecContext) {
		patchCluster(ctx, mainName, map[string]any{"spec": map[string]any{"backup": map[string]any{
			"barmanObjectStore": map[string]any{
				"destinationPath": "s3://cm-unreachable/",
				"endpointURL":     "http://127.0.0.1:9",
				"s3Credentials":   map[string]any{"inheritFromIAMRole": true},
			},
		}}})
		psql(ctx, secondPod, "INSERT INTO cm VALUES (0)", "SELECT pg_switch_wal()")
		Eventually(func(g Gomega, ctx SpecContext) {
			text, at, err := framework.ScrapeCNPG(ctx, cfg, podKey(secondPod))
			g.Expect(err).NotTo(HaveOccurred())
			series, err := parity.ParseCNPG(text, at)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(valueOf(g, series, builtinPrefix+`pg_wal_archive_status{value="ready"}`)).To(BeNumerically(">=", 1))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(statePoll).Should(Succeed())

		judge(ctx, "failing-archive", mainName, secondPod, false)
	}, SpecTimeout(3*time.Minute))

	It("tally: every built-in was non-zero on our side in some check, but the three backup timestamps", func() {
		backups := slices.Sorted(slices.Values(backupTimestamps))
		Expect(zero(counts.ours)).To(Equal(backups), "never non-zero on our side")

		var cnpg []string
		for _, key := range zero(counts.cnpg) {
			if !slices.Contains(selfMeasuring, key) {
				cnpg = append(cnpg, key)
			}
		}
		Expect(cnpg).To(Equal(backups), "never non-zero on CNPG's side, the self-measuring metrics aside")
		AddReportEntry("tally", fmt.Sprintf("ours: %v; CNPG: %v", slices.Sorted(maps.Keys(counts.ours)), slices.Sorted(maps.Keys(counts.cnpg))))
	})
})
