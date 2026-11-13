package framework_test

import (
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("PeerCluster", func() {
	var cluster *apiv1.Cluster

	BeforeEach(func() {
		cluster = framework.PeerCluster("ns", "peer", "img")
	})

	It("is a one-instance Cluster", func() {
		Expect(cluster.Namespace).To(Equal("ns"))
		Expect(cluster.Name).To(Equal("peer"))
		Expect(cluster.Spec.ImageName).To(Equal("img"))
		Expect(cluster.Spec.Instances).To(Equal(1))
	})

	It("enables our plugin once, in the default mode", func() {
		Expect(cluster.Spec.Plugins).To(HaveExactElements(SatisfyAll(
			HaveField("Name", metadata.PluginName),
			HaveField("Parameters", BeEmpty()),
		)))
	})

	It("maps the OS user nobody to cnpg_metrics_exporter", func() {
		Expect(cluster.Spec.PostgresConfiguration.PgIdent).To(Equal([]string{
			"cnpg_metrics_exporter nobody cnpg_metrics_exporter",
		}))
	})
})

var _ = Describe("PasswordCluster", func() {
	var cluster *apiv1.Cluster

	BeforeEach(func() {
		cluster = framework.PasswordCluster("ns", "pw", "img", "otel_monitor", "otel-monitor")
	})

	It("is a one-instance Cluster", func() {
		Expect(cluster.Namespace).To(Equal("ns"))
		Expect(cluster.Name).To(Equal("pw"))
		Expect(cluster.Spec.ImageName).To(Equal("img"))
		Expect(cluster.Spec.Instances).To(Equal(1))
	})

	It("enables our plugin once, in password mode", func() {
		Expect(cluster.Spec.Plugins).To(HaveExactElements(SatisfyAll(
			HaveField("Name", metadata.PluginName),
			HaveField("Parameters", Equal(map[string]string{ //nolint:gosec // a Secret name, not a credential
				"auth":           "password",
				"passwordSecret": "otel-monitor",
			})),
		)))
	})

	It("manages the role as a pg_monitor member with the password Secret", func() {
		Expect(cluster.Spec.Managed).NotTo(BeNil())
		Expect(cluster.Spec.Managed.Roles).To(Equal([]apiv1.RoleConfiguration{{
			Name:           "otel_monitor",
			Ensure:         apiv1.EnsurePresent,
			Login:          true,
			InRoles:        []string{"pg_monitor"},
			PasswordSecret: &apiv1.LocalObjectReference{Name: "otel-monitor"},
		}}))
	})

	It("lets the role in over loopback only, with SCRAM", func() {
		Expect(cluster.Spec.PostgresConfiguration.PgHBA).To(Equal([]string{
			"host all otel_monitor 127.0.0.1/32 scram-sha-256",
			"host all otel_monitor ::1/128 scram-sha-256",
			"host all otel_monitor all reject",
		}))
	})

	It("adds no pg_ident line", func() {
		Expect(cluster.Spec.PostgresConfiguration.PgIdent).To(BeEmpty())
	})
})

var _ = Describe("RolloutComplete", func() {
	rolledOut := func() *appsv1.Deployment {
		d := &appsv1.Deployment{}
		d.Generation = 2
		d.Spec.Replicas = ptr.To(int32(2))
		d.Status = appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Replicas:           2,
			UpdatedReplicas:    2,
			AvailableReplicas:  2,
		}

		return d
	}

	DescribeTable("reports",
		func(mutate func(*appsv1.Deployment), want bool) {
			d := rolledOut()
			mutate(d)
			done, err := framework.RolloutComplete(d)
			Expect(err).NotTo(HaveOccurred())
			Expect(done).To(Equal(want))
		},
		Entry("done when every replica is updated and available", func(*appsv1.Deployment) {}, true),
		Entry("not done before the spec change is observed", func(d *appsv1.Deployment) { d.Generation = 3 }, false),
		Entry("not done while replicas wait for the update", func(d *appsv1.Deployment) {
			d.Status.UpdatedReplicas = 1
			d.Status.AvailableReplicas = 1
		}, false),
		Entry("not done while old replicas terminate", func(d *appsv1.Deployment) { d.Status.Replicas = 3 }, false),
		Entry("not done while updated replicas are unavailable", func(d *appsv1.Deployment) {
			d.Status.AvailableReplicas = 1
		}, false),
	)

	It("fails once the progress deadline is exceeded", func() {
		d := rolledOut()
		d.Status.Conditions = []appsv1.DeploymentCondition{{
			Type:   appsv1.DeploymentProgressing,
			Reason: "ProgressDeadlineExceeded",
		}}
		_, err := framework.RolloutComplete(d)
		Expect(err).To(MatchError(framework.ErrRolloutTimedOut))
	})
})
