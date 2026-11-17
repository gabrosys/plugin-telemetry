// Package builtin computes the built-in metrics that come from the Cluster rather than from SQL.
package builtin

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/Masterminds/semver/v3"
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

const (
	prefix         = "cnpg_collector_"
	fencedKey      = "cnpg.io/fencedInstances"
	fenceAll       = "*"
	notExtracted   = -1
	switchoverHelp = "1 if a manual switchover is required, 0 otherwise"
)

// Facts are the values from the newest successful facts round.
type Facts struct {
	Version        semver.Version
	Primary        bool    // not in recovery
	WALSegmentSize float64 // bytes
}

// Samples returns the built-ins computed outside SQL, sorted: those of c for pod (none when c is nil), and
// postgres_version{full, cluster}. f is nil while the newest facts round failed.
// Ported from CloudNativePG v1.30.1 (identical on main bc0889a): metricserver/pg_collector.go, wal.go,
// controller/instance_controller.go reconcileMetrics, replication/legacy.go.
func Samples(c *apiv1.Cluster, pod, cluster string, f *Facts) []queries.Sample {
	var out []queries.Sample
	if c != nil {
		out = clusterSamples(c, pod, f)
	}
	if f != nil {
		full := fmt.Sprintf("%d.%d", f.Version.Major(), f.Version.Patch())
		// Both parts are integers, so the parse cannot fail.
		v, _ := strconv.ParseFloat(full, 64)
		out = append(out, gauge("postgres_version", "Postgres version", v, label("full", full), label("cluster", cluster)))
	}

	slices.SortStableFunc(out, func(a, b queries.Sample) int {
		if c := cmp.Compare(a.Metric, b.Metric); c != 0 {
			return c
		}
		return slices.CompareFunc(a.Labels, b.Labels, queries.CompareLabels)
	})
	return out
}

func clusterSamples(c *apiv1.Cluster, pod string, f *Facts) []queries.Sample {
	out := []queries.Sample{
		gauge("replica_mode", "1 if the cluster is in replica mode, 0 otherwise", boolValue(isReplica(c))),
		gauge("fencing_on", "1 if the instance is fenced, 0 otherwise", boolValue(isFenced(c.Annotations, pod))),
		gauge("nodes_used", nodesUsedHelp, nodesUsed(c)),
	}

	syncHelp := "Number of requested synchronous replicas (synchronous_standby_names)"
	out = append(out,
		gauge("sync_replicas", syncHelp, float64(c.Spec.MinSyncReplicas), label("value", "min")),
		gauge("sync_replicas", syncHelp, float64(c.Spec.MaxSyncReplicas), label("value", "max")),
		gauge("sync_replicas", syncHelp, float64(expectedSyncReplicas(c)), label("value", "expected")),
	)

	if c.Status.CurrentPrimary == pod {
		out = append(out, gauge("manual_switchover_required", switchoverHelp,
			boolValue(c.Status.Phase == apiv1.PhaseWaitingForUser)))
	}

	if f == nil {
		return out
	}

	if f.Primary {
		out = append(out,
			gauge("first_recoverability_point", "The first point of recoverability for the cluster as a unix timestamp (Deprecated)",
				timestamp(c.Status.FirstRecoverabilityPoint)), //nolint:staticcheck // the field CNPG reports
			gauge("last_available_backup_timestamp", "The last available backup as a unix timestamp (Deprecated)",
				timestamp(c.Status.LastSuccessfulBackup)), //nolint:staticcheck // the field CNPG reports
			gauge("last_failed_backup_timestamp", "The last failed backup as a unix timestamp (Deprecated)",
				timestamp(c.Status.LastFailedBackup)), //nolint:staticcheck // the field CNPG reports
		)
	}

	size := walVolumeSize(c)
	segments := math.NaN()
	if size != 0 {
		segments = size / f.WALSegmentSize
	} else {
		size = math.NaN()
	}
	return append(out,
		gauge("pg_wal", queries.PGWALHelp, size, label("value", "volume_size")),
		gauge("pg_wal", queries.PGWALHelp, segments, label("value", "volume_max")),
	)
}

const (
	nodesUsedHelp = "NodesUsed represents the count of distinct nodes accommodating the instances. " +
		"A value of '-1' suggests that the metric is not available. " +
		"A value of '1' suggests that all instances are hosted on a single node, " +
		"implying the absence of High Availability (HA). Ideally this value " +
		"should match the number of instances in the cluster."
)

func gauge(name, help string, v float64, labels ...queries.Label) queries.Sample {
	sort.Slice(labels, func(i, j int) bool { return labels[i].Key < labels[j].Key })
	return queries.Sample{Metric: prefix + name, Description: help, Kind: queries.KindGauge, Labels: labels, Value: v}
}

func label(key, value string) queries.Label { return queries.Label{Key: key, Value: value} }

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// isReplica ports Cluster.IsReplica (api/v1/cluster_funcs.go:1087-1112 at v1.30.1).
func isReplica(c *apiv1.Cluster) bool {
	r := c.Spec.ReplicaCluster
	if r == nil {
		return false
	}
	if r.Enabled != nil {
		return *r.Enabled
	}
	self := r.Self
	if self == "" {
		self = c.Name
	}
	return self != r.Primary
}

// isFenced ports Cluster.IsInstanceFenced with GetFencedInstances (pkg/utils/fencing.go:46-64): a
// malformed annotation fences nothing.
func isFenced(annotations map[string]string, pod string) bool {
	raw, ok := annotations[fencedKey]
	if !ok {
		return false
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return false
	}
	return slices.Contains(names, fenceAll) || slices.Contains(names, pod)
}

func nodesUsed(c *apiv1.Cluster) float64 {
	if !c.Status.Topology.SuccessfullyExtracted {
		return notExtracted
	}
	return float64(c.Status.Topology.NodesUsed)
}

// timestamp ports setTimestampMetric: an empty or unparsable value gives 0.
func timestamp(value string) float64 {
	if value == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return 0
	}
	return float64(t.Unix())
}

// walVolumeSize ports getWalVolumeSize (wal.go:225-239): 0 without walStorage or a usable size.
func walVolumeSize(c *apiv1.Cluster) float64 {
	ws := c.Spec.WalStorage
	if ws == nil {
		return 0
	}
	var q *resource.Quantity
	switch {
	case ws.Size != "":
		parsed, err := resource.ParseQuantity(ws.Size)
		if err != nil {
			return 0
		}
		q = &parsed
	case ws.PersistentVolumeClaimTemplate != nil:
		q = ws.PersistentVolumeClaimTemplate.Resources.Requests.Storage()
	}
	if q == nil {
		return 0
	}
	return q.AsApproximateFloat64()
}

// expectedSyncReplicas ports GetExpectedSyncReplicasNumber (replication.go:30-37) and getSyncReplicasData
// (legacy.go:55-169, without the log lines).
func expectedSyncReplicas(c *apiv1.Cluster) int {
	if s := c.Spec.PostgresConfiguration.Synchronous; s != nil {
		return s.Number
	}

	ready := len(c.Status.InstancesStatus[apiv1.PodHealthy]) - 1
	if ready < 0 {
		return 0
	}

	n := c.Spec.MaxSyncReplicas
	if ready < n {
		n = c.Spec.MinSyncReplicas
	}
	if ready < c.Spec.MinSyncReplicas {
		n = ready
	}

	if electable := len(electableReplicas(c)); electable < n {
		n = electable
	}
	return n
}

// electableReplicas ports getElectableSyncReplicas (legacy.go:127-169).
func electableReplicas(c *apiv1.Cluster) []string {
	var others []string
	for _, name := range c.Status.InstancesStatus[apiv1.PodHealthy] {
		if name != c.Status.CurrentPrimary {
			others = append(others, name)
		}
	}
	sort.Strings(others)

	constraint := c.Spec.PostgresConfiguration.SyncReplicaElectionConstraint
	topology := c.Status.Topology
	if !constraint.Enabled || !topology.SuccessfullyExtracted {
		return others
	}

	primary := apiv1.PodName(c.Status.CurrentPrimary)
	if primary == "" {
		return nil
	}
	primaryTopology, ok := topology.Instances[primary]
	if !ok {
		return nil
	}

	var electable []string
	for _, name := range others {
		instance, ok := topology.Instances[apiv1.PodName(name)]
		if !ok {
			continue
		}
		if !matchesTopology(primaryTopology, instance) {
			electable = append(electable, name)
		}
	}
	return electable
}

// matchesTopology ports PodTopologyLabels.MatchesTopology (cluster_funcs.go:187-194).
func matchesTopology(a, b apiv1.PodTopologyLabels) bool {
	for k, v := range a {
		if v != b[k] {
			return false
		}
	}
	return true
}
