package builtin_test

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Masterminds/semver/v3"
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/builtin"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

const (
	podName     = "c-1"
	clusterName = "c"
	prefix      = "cnpg_collector_"
	fenceKey    = "cnpg.io/fencedInstances"
)

func facts(primary bool) *builtin.Facts {
	return &builtin.Facts{Version: *semver.MustParse("18.0.7"), Primary: primary, WALSegmentSize: 16777216}
}

// find returns the value of the one sample with the metric name and the given key=value label pairs.
func find(samples []queries.Sample, name string, kv ...string) (float64, bool) {
	for _, s := range samples {
		if s.Metric != prefix+name || len(s.Labels) != len(kv)/2 {
			continue
		}
		match := true
		for i := 0; i < len(kv); i += 2 {
			if s.Labels[i/2].Key != kv[i] || s.Labels[i/2].Value != kv[i+1] {
				match = false
			}
		}
		if match {
			return s.Value, true
		}
	}
	return 0, false
}

func value(samples []queries.Sample, name string, kv ...string) float64 {
	GinkgoHelper()
	v, ok := find(samples, name, kv...)
	Expect(ok).To(BeTrue(), "no sample %s %v", name, kv)
	return v
}

func absent(samples []queries.Sample, name string, kv ...string) {
	GinkgoHelper()
	_, ok := find(samples, name, kv...)
	Expect(ok).To(BeFalse(), "unexpected sample %s %v", name, kv)
}

// legacyCluster is CNPG's createFakeCluster (pkg/postgres/replication/suite_test.go:37-53, v1.30.1).
func legacyCluster(name string) *apiv1.Cluster {
	primary := fmt.Sprintf("%s-1", name)
	c := &apiv1.Cluster{}
	c.Name = name
	c.Spec.Instances = 3
	c.Spec.MaxSyncReplicas = 2
	c.Spec.MinSyncReplicas = 1
	c.Status = apiv1.ClusterStatus{
		CurrentPrimary: primary,
		InstancesStatus: map[apiv1.PodStatus][]string{
			apiv1.PodHealthy: {primary, fmt.Sprintf("%s-2", name), fmt.Sprintf("%s-3", name)},
			apiv1.PodFailed:  {},
		},
	}
	return c
}

func expected(c *apiv1.Cluster) float64 {
	GinkgoHelper()
	return value(builtin.Samples(c, podName, clusterName, nil), "sync_replicas", "value", "expected")
}

var _ = Describe("Samples", func() {
	It("returns only postgres_version without a Cluster", func() {
		samples := builtin.Samples(nil, podName, clusterName, facts(true))

		Expect(samples).To(HaveLen(1))
		Expect(samples[0]).To(Equal(queries.Sample{
			Metric:      prefix + "postgres_version",
			Description: "Postgres version",
			Kind:        queries.KindGauge,
			Labels:      []queries.Label{{Key: "cluster", Value: "c"}, {Key: "full", Value: "18.7"}},
			Value:       18.7,
		}))
	})

	It("returns nothing without a Cluster and without facts", func() {
		Expect(builtin.Samples(nil, podName, clusterName, nil)).To(BeEmpty())
	})

	It("returns no facts-derived sample with a Cluster and without facts", func() {
		c := legacyCluster(clusterName)
		c.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "1Gi"}
		samples := builtin.Samples(c, podName, clusterName, nil)

		absent(samples, "postgres_version", "cluster", "c", "full", "18.7")
		absent(samples, "pg_wal", "value", "volume_size")
		absent(samples, "pg_wal", "value", "volume_max")
		for _, name := range []string{"first_recoverability_point", "last_available_backup_timestamp", "last_failed_backup_timestamp"} {
			absent(samples, name)
		}
		for _, s := range samples {
			Expect(s.Metric).NotTo(Equal(prefix + "postgres_version"))
		}
	})

	It("returns sorted gauges with CNPG's help strings", func() {
		c := legacyCluster(clusterName)
		c.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "1Gi"}
		samples := builtin.Samples(c, podName, clusterName, facts(true))

		Expect(sort.SliceIsSorted(samples, func(i, j int) bool { return samples[i].Metric < samples[j].Metric })).To(BeTrue())
		var order []string
		for _, s := range samples {
			if s.Metric == prefix+"sync_replicas" || s.Metric == prefix+"pg_wal" {
				order = append(order, s.Metric[len(prefix):]+"="+s.Labels[0].Value)
			}
		}
		Expect(order).To(Equal([]string{
			"pg_wal=volume_max", "pg_wal=volume_size", "sync_replicas=expected", "sync_replicas=max", "sync_replicas=min",
		}))
		help := map[string]string{
			"replica_mode":                    "1 if the cluster is in replica mode, 0 otherwise",
			"fencing_on":                      "1 if the instance is fenced, 0 otherwise",
			"manual_switchover_required":      "1 if a manual switchover is required, 0 otherwise",
			"sync_replicas":                   "Number of requested synchronous replicas (synchronous_standby_names)",
			"first_recoverability_point":      "The first point of recoverability for the cluster as a unix timestamp (Deprecated)",
			"last_available_backup_timestamp": "The last available backup as a unix timestamp (Deprecated)",
			"last_failed_backup_timestamp":    "The last failed backup as a unix timestamp (Deprecated)",
			"postgres_version":                "Postgres version",
			"pg_wal": "Total size in bytes of WAL segments in the '/var/lib/postgresql/data/pgdata/pg_wal' directory " +
				" computed as (wal_segment_size * count)",
			"nodes_used": "NodesUsed represents the count of distinct nodes accommodating the instances. " +
				"A value of '-1' suggests that the metric is not available. " +
				"A value of '1' suggests that all instances are hosted on a single node, " +
				"implying the absence of High Availability (HA). Ideally this value " +
				"should match the number of instances in the cluster.",
		}
		seen := map[string]bool{}
		for _, s := range samples {
			Expect(s.Kind).To(Equal(queries.KindGauge), s.Metric)
			name := s.Metric[len(prefix):]
			Expect(help).To(HaveKey(name))
			Expect(s.Description).To(Equal(help[name]), name)
			seen[name] = true
		}
		Expect(seen).To(HaveLen(len(help)))
	})

	// Ported from CloudNativePG v1.30.1 api/v1/cluster_funcs.go:1087-1112 (IsReplica); the table is ours.
	Describe("replica_mode", func() {
		enabled, disabled := true, false
		DescribeTable("IsReplica",
			func(r *apiv1.ReplicaClusterConfiguration, want float64) {
				c := legacyCluster(clusterName)
				c.Spec.ReplicaCluster = r
				Expect(value(builtin.Samples(c, podName, clusterName, nil), "replica_mode")).To(Equal(want))
			},
			Entry("no replica stanza", nil, 0.0),
			Entry("enabled true", &apiv1.ReplicaClusterConfiguration{Enabled: &enabled}, 1.0),
			Entry("enabled false", &apiv1.ReplicaClusterConfiguration{Enabled: &disabled, Self: "a", Primary: "b"}, 0.0),
			Entry("self differs from primary", &apiv1.ReplicaClusterConfiguration{Self: "a", Primary: "b"}, 1.0),
			Entry("self equals primary", &apiv1.ReplicaClusterConfiguration{Self: "a", Primary: "a"}, 0.0),
			Entry("empty self uses the Cluster name, differing", &apiv1.ReplicaClusterConfiguration{Primary: "b"}, 1.0),
			Entry("empty self uses the Cluster name, equal", &apiv1.ReplicaClusterConfiguration{Primary: clusterName}, 0.0),
		)
	})

	// Ported from CloudNativePG v1.30.1 api/v1/cluster_funcs_test.go:921-967, plus "not json" (pkg/utils/fencing.go:46-64).
	Describe("fencing_on", func() {
		DescribeTable("the annotation",
			func(annotations map[string]string, pod string, want float64) {
				c := legacyCluster(clusterName)
				c.Annotations = annotations
				Expect(value(builtin.Samples(c, pod, clusterName, nil), "fencing_on")).To(Equal(want))
			},
			Entry("pod listed", map[string]string{fenceKey: `["one"]`}, "one", 1.0),
			Entry("another pod listed", map[string]string{fenceKey: `["one"]`}, "two", 0.0),
			Entry("whole cluster", map[string]string{fenceKey: `["*"]`}, "three", 1.0),
			Entry("no annotation", map[string]string{}, "one", 0.0),
			Entry("nil annotations", nil, "one", 0.0),
			Entry("not json", map[string]string{fenceKey: "not json"}, "one", 0.0),
		)
	})

	// CloudNativePG v1.30.1 pg_collector.go:533-551 (collectNodesUsed).
	Describe("nodes_used", func() {
		It("is the topology count when extracted", func() {
			c := legacyCluster(clusterName)
			c.Status.Topology = apiv1.Topology{SuccessfullyExtracted: true, NodesUsed: 3}
			Expect(value(builtin.Samples(c, podName, clusterName, nil), "nodes_used")).To(Equal(3.0))
		})
		It("is -1 when not extracted", func() {
			c := legacyCluster(clusterName)
			c.Status.Topology = apiv1.Topology{NodesUsed: 3}
			Expect(value(builtin.Samples(c, podName, clusterName, nil), "nodes_used")).To(Equal(-1.0))
		})
	})

	// Ported from CloudNativePG v1.30.1 pkg/postgres/replication/legacy_test.go:30-100 and legacy.go:55-169, replication.go:30-37.
	Describe("sync_replicas", func() {
		It("reports min, max and expected", func() {
			c := legacyCluster(clusterName)
			c.Spec.MinSyncReplicas, c.Spec.MaxSyncReplicas = 1, 2
			samples := builtin.Samples(c, podName, clusterName, nil)
			Expect(value(samples, "sync_replicas", "value", "min")).To(Equal(1.0))
			Expect(value(samples, "sync_replicas", "value", "max")).To(Equal(2.0))
			Expect(value(samples, "sync_replicas", "value", "expected")).To(Equal(2.0))
			absent(samples, "sync_replicas", "value", "observed")
		})

		It("counts all non primary pods as electable", func() {
			Expect(expected(legacyCluster("example"))).To(Equal(2.0))
		})

		It("keeps only the pod in a different zone", func() {
			c := legacyCluster("exampleAntiAffinity")
			c.Spec.PostgresConfiguration.SyncReplicaElectionConstraint = apiv1.SyncReplicaElectionConstraints{
				Enabled:                true,
				NodeLabelsAntiAffinity: []string{"az"},
			}
			c.Status.Topology = apiv1.Topology{
				SuccessfullyExtracted: true,
				Instances: map[apiv1.PodName]apiv1.PodTopologyLabels{
					"exampleAntiAffinity-1": {"az": "one"},
					"exampleAntiAffinity-2": {"az": "one"},
					"exampleAntiAffinity-3": {"az": "three"},
				},
			}
			Expect(expected(c)).To(Equal(1.0))
		})

		It("lowers the number to enforce self-healing", func() {
			c := legacyCluster("exampleOnePod")
			c.Status = apiv1.ClusterStatus{
				CurrentPrimary: "exampleOnePod-1",
				InstancesStatus: map[apiv1.PodStatus][]string{
					apiv1.PodHealthy: {"exampleOnePod-1"},
					apiv1.PodFailed:  {"exampleOnePod-2", "exampleOnePod-3"},
				},
			}
			Expect(expected(c)).To(BeZero())
		})

		It("is zero when no pod is ready", func() {
			c := legacyCluster("exampleNoPods")
			c.Status = apiv1.ClusterStatus{
				CurrentPrimary: "example-1",
				InstancesStatus: map[apiv1.PodStatus][]string{
					apiv1.PodFailed: {"exampleNoPods-1", "exampleNoPods-2", "exampleNoPods-3"},
				},
			}
			Expect(expected(c)).To(BeZero())
		})

		It("falls back to all replicas while the topology is not extracted", func() {
			c := legacyCluster(clusterName)
			c.Spec.PostgresConfiguration.SyncReplicaElectionConstraint = apiv1.SyncReplicaElectionConstraints{
				Enabled: true, NodeLabelsAntiAffinity: []string{"az"},
			}
			Expect(expected(c)).To(Equal(2.0))
		})

		It("elects nothing without a current primary under constraints", func() {
			c := legacyCluster(clusterName)
			c.Spec.PostgresConfiguration.SyncReplicaElectionConstraint = apiv1.SyncReplicaElectionConstraints{
				Enabled: true, NodeLabelsAntiAffinity: []string{"az"},
			}
			c.Status.Topology.SuccessfullyExtracted = true
			c.Status.CurrentPrimary = ""
			Expect(expected(c)).To(BeZero())
		})

		It("elects nothing while the primary topology is unknown", func() {
			c := legacyCluster(clusterName)
			c.Spec.PostgresConfiguration.SyncReplicaElectionConstraint = apiv1.SyncReplicaElectionConstraints{
				Enabled: true, NodeLabelsAntiAffinity: []string{"az"},
			}
			c.Status.Topology = apiv1.Topology{
				SuccessfullyExtracted: true,
				Instances:             map[apiv1.PodName]apiv1.PodTopologyLabels{"c-2": {"az": "two"}},
			}
			Expect(expected(c)).To(BeZero())
		})

		It("skips replicas without topology data", func() {
			c := legacyCluster(clusterName)
			c.Spec.PostgresConfiguration.SyncReplicaElectionConstraint = apiv1.SyncReplicaElectionConstraints{
				Enabled: true, NodeLabelsAntiAffinity: []string{"az"},
			}
			c.Status.Topology = apiv1.Topology{
				SuccessfullyExtracted: true,
				Instances: map[apiv1.PodName]apiv1.PodTopologyLabels{
					"c-1": {"az": "one"},
					"c-2": {"az": "two"},
				},
			}
			Expect(expected(c)).To(Equal(1.0))
		})

		It("uses synchronous.number when set", func() {
			c := legacyCluster(clusterName)
			c.Spec.PostgresConfiguration.Synchronous = &apiv1.SynchronousReplicaConfiguration{Number: 2}
			c.Spec.MaxSyncReplicas = 0
			Expect(expected(c)).To(Equal(2.0))
		})
	})

	// CloudNativePG v1.30.1 internal/management/controller/instance_controller.go:1007-1020 (reconcileMetrics).
	Describe("manual_switchover_required", func() {
		DescribeTable("on the current primary",
			func(primary, phase string, want float64, present bool) {
				c := legacyCluster(clusterName)
				c.Status.CurrentPrimary = primary
				c.Status.Phase = phase
				samples := builtin.Samples(c, podName, clusterName, nil)
				if !present {
					absent(samples, "manual_switchover_required")
					return
				}
				Expect(value(samples, "manual_switchover_required")).To(Equal(want))
			},
			Entry("waiting for the user", podName, apiv1.PhaseWaitingForUser, 1.0, true),
			Entry("healthy", podName, apiv1.PhaseHealthy, 0.0, true),
			Entry("another pod is the primary", "c-2", apiv1.PhaseWaitingForUser, 0.0, false),
		)
	})

	// CloudNativePG v1.30.1 pg_collector.go:489-532 (setTimestampMetric).
	Describe("the backup timestamps", func() {
		const when = "2026-11-15T10:00:00Z"
		unix := float64(time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC).Unix())
		names := []string{"first_recoverability_point", "last_available_backup_timestamp", "last_failed_backup_timestamp"}

		set := func(c *apiv1.Cluster, name, v string) {
			switch name {
			case "first_recoverability_point":
				c.Status.FirstRecoverabilityPoint = v //nolint:staticcheck // the field CNPG reports
			case "last_available_backup_timestamp":
				c.Status.LastSuccessfulBackup = v //nolint:staticcheck // the field CNPG reports
			case "last_failed_backup_timestamp":
				c.Status.LastFailedBackup = v //nolint:staticcheck // the field CNPG reports
			}
		}

		for _, name := range names {
			DescribeTable(name,
				func(v string, want float64) {
					c := legacyCluster(clusterName)
					set(c, name, v)
					Expect(value(builtin.Samples(c, podName, clusterName, facts(true)), name)).To(Equal(want))
				},
				Entry("RFC 3339", when, unix),
				Entry("empty", "", 0.0),
				Entry("unparsable", "yesterday", 0.0),
			)

			It(name+" is absent on a standby and without facts", func() {
				c := legacyCluster(clusterName)
				set(c, name, when)
				absent(builtin.Samples(c, podName, clusterName, facts(false)), name)
				absent(builtin.Samples(c, podName, clusterName, nil), name)
			})
		}
	})

	// CloudNativePG v1.30.1 metricserver/wal.go:205-239 (volume_size, volume_max, getWalVolumeSize).
	Describe("pg_wal volume", func() {
		It("is the volume size in bytes and in segments", func() {
			c := legacyCluster(clusterName)
			c.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "1Gi"}
			samples := builtin.Samples(c, podName, clusterName, facts(true))
			Expect(value(samples, "pg_wal", "value", "volume_size")).To(Equal(1073741824.0))
			Expect(value(samples, "pg_wal", "value", "volume_max")).To(Equal(64.0))
		})

		It("is NaN without walStorage", func() {
			samples := builtin.Samples(legacyCluster(clusterName), podName, clusterName, facts(true))
			Expect(math.IsNaN(value(samples, "pg_wal", "value", "volume_size"))).To(BeTrue())
			Expect(math.IsNaN(value(samples, "pg_wal", "value", "volume_max"))).To(BeTrue())
		})

		It("is NaN for an unparsable size", func() {
			c := legacyCluster(clusterName)
			c.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "lots"}
			samples := builtin.Samples(c, podName, clusterName, facts(true))
			Expect(math.IsNaN(value(samples, "pg_wal", "value", "volume_size"))).To(BeTrue())
		})

		It("is absent without facts", func() {
			c := legacyCluster(clusterName)
			c.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "1Gi"}
			samples := builtin.Samples(c, podName, clusterName, nil)
			absent(samples, "pg_wal", "value", "volume_size")
			absent(samples, "pg_wal", "value", "volume_max")
		})
	})
})
