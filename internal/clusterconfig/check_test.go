package clusterconfig_test

import (
	"sort"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

const (
	identLine  = "cnpg_metrics_exporter nobody cnpg_metrics_exporter"
	hbaLocal4  = "host postgres metrics 127.0.0.1/32 scram-sha-256"
	hbaLocal6  = "host postgres metrics ::1/128 scram-sha-256"
	hbaReject  = "host all metrics all reject"
	passSecret = "metrics-pw"
)

func metricsRole() apiv1.RoleConfiguration {
	return apiv1.RoleConfiguration{
		Name:           "metrics",
		Ensure:         apiv1.EnsurePresent,
		Login:          true,
		InRoles:        []string{"pg_monitor"},
		PasswordSecret: &apiv1.LocalObjectReference{Name: passSecret},
	}
}

func clusterWith(ident, hba []string, roles ...apiv1.RoleConfiguration) *apiv1.Cluster {
	c := newCluster()
	c.Spec.PostgresConfiguration.PgIdent = ident
	c.Spec.PostgresConfiguration.PgHBA = hba
	if len(roles) > 0 {
		c.Spec.Managed = &apiv1.ManagedConfiguration{Roles: roles}
	}
	return c
}

var _ = Describe("SpecProblems", func() {
	peer := podpatch.Params{Auth: podpatch.AuthPeer}
	password := podpatch.Params{Auth: podpatch.AuthPassword, PasswordSecret: passSecret}

	DescribeTable("lists what the Cluster spec lacks",
		func(c *apiv1.Cluster, p podpatch.Params, want ...string) {
			Expect(clusterconfig.SpecProblems(c, p)).To(ConsistOf(want))
			Expect(sort.StringsAreSorted(clusterconfig.SpecProblems(c, p))).To(BeTrue())
		},
		Entry("a correct peer Cluster", clusterWith([]string{identLine}, nil), peer),
		Entry("peer mode accepts extra whitespace in the pg_ident line",
			clusterWith([]string{"cnpg_metrics_exporter \t nobody   cnpg_metrics_exporter"}, nil), peer),
		Entry("peer mode without the pg_ident line", clusterWith(nil, nil), peer,
			`peer mode needs the pg_ident line "cnpg_metrics_exporter nobody cnpg_metrics_exporter"`),
		Entry("peer mode with another pg_ident line", clusterWith([]string{"other nobody other"}, nil), peer,
			`peer mode needs the pg_ident line "cnpg_metrics_exporter nobody cnpg_metrics_exporter"`),
		Entry("a correct password Cluster",
			clusterWith(nil, []string{hbaLocal4, hbaLocal6, hbaReject}, metricsRole()), password),
		Entry("password mode without passwordSecret",
			clusterWith(nil, nil), podpatch.Params{Auth: podpatch.AuthPassword},
			"password mode needs the passwordSecret parameter"),
		Entry("no managed role uses the passwordSecret",
			clusterWith(nil, []string{hbaLocal4, hbaLocal6, hbaReject}), password,
			`no managed role uses passwordSecret "metrics-pw" with login and pg_monitor`),
		Entry("the role cannot log in",
			clusterWith(nil, nil, func() apiv1.RoleConfiguration { r := metricsRole(); r.Login = false; return r }()), password,
			`no managed role uses passwordSecret "metrics-pw" with login and pg_monitor`),
		Entry("the role is not in pg_monitor",
			clusterWith(nil, nil, func() apiv1.RoleConfiguration { r := metricsRole(); r.InRoles = nil; return r }()), password,
			`no managed role uses passwordSecret "metrics-pw" with login and pg_monitor`),
		Entry("the role is absent",
			clusterWith(nil, nil, func() apiv1.RoleConfiguration { r := metricsRole(); r.Ensure = apiv1.EnsureAbsent; return r }()), password,
			`no managed role uses passwordSecret "metrics-pw" with login and pg_monitor`),
		Entry("an unset ensure means present",
			clusterWith(nil, []string{hbaLocal4, hbaLocal6, hbaReject},
				func() apiv1.RoleConfiguration { r := metricsRole(); r.Ensure = ""; return r }()), password),
		Entry("no loopback IPv4 line",
			clusterWith(nil, []string{hbaLocal6, hbaReject}, metricsRole()), password,
			`pg_hba has no line for role "metrics" from 127.0.0.1/32`),
		Entry("no loopback IPv6 line",
			clusterWith(nil, []string{hbaLocal4, hbaReject}, metricsRole()), password,
			`pg_hba has no line for role "metrics" from ::1/128`),
		Entry("no reject line",
			clusterWith(nil, []string{hbaLocal4, hbaLocal6}, metricsRole()), password,
			`pg_hba has no reject line for role "metrics"`),
		Entry("does not judge line order",
			clusterWith(nil, []string{hbaReject, hbaLocal4, hbaLocal6}, metricsRole()), password),
		Entry("a user list item is not judged by its form",
			clusterWith(nil, []string{hbaLocal4, hbaLocal6, "host all all all reject"}, metricsRole()), password),
		Entry("a CIDR that excludes the loopback address leaves both gaps",
			clusterWith(nil, []string{"host all metrics 10.0.0.0/8 scram-sha-256", hbaReject}, metricsRole()), password,
			`pg_hba has no line for role "metrics" from 127.0.0.1/32`,
			`pg_hba has no line for role "metrics" from ::1/128`),
		Entry("a local line does not count",
			clusterWith(nil, []string{"local all metrics scram-sha-256 map=x", hbaReject}, metricsRole()), password,
			`pg_hba has no line for role "metrics" from 127.0.0.1/32`,
			`pg_hba has no line for role "metrics" from ::1/128`),
		Entry("a line for another role does not count",
			clusterWith(nil, []string{"host all other 127.0.0.1/32 trust", hbaLocal6, hbaReject}, metricsRole()), password,
			`pg_hba has no line for role "metrics" from 127.0.0.1/32`),
		Entry("hostssl and extra whitespace count",
			clusterWith(nil, []string{
				"hostssl  postgres   metrics\t127.0.0.1/32 scram-sha-256", hbaLocal6, "host all metrics all  reject",
			}, metricsRole()), password),
		Entry("user all", clusterWith(nil, []string{"host all all 127.0.0.1/32 scram-sha-256", "host all all ::1/128 scram-sha-256", hbaReject}, metricsRole()), password),
		Entry("user +group", clusterWith(nil, []string{"host all +pg_monitor 127.0.0.1/32 scram-sha-256", "host all +pg_monitor ::1/128 scram-sha-256", hbaReject}, metricsRole()), password),
		Entry("user /regex", clusterWith(nil, []string{"host all /^metr 127.0.0.1/32 scram-sha-256", "host all /^metr ::1/128 scram-sha-256", hbaReject}, metricsRole()), password),
		Entry("user @file", clusterWith(nil, []string{"host all @admins 127.0.0.1/32 scram-sha-256", "host all @admins ::1/128 scram-sha-256", hbaReject}, metricsRole()), password),
		Entry("a quoted user", clusterWith(nil, []string{`host all "metrics" 127.0.0.1/32 scram-sha-256`, `host all "metrics" ::1/128 scram-sha-256`, hbaReject}, metricsRole()), password),
		Entry("address samehost", clusterWith(nil, []string{"host all metrics samehost scram-sha-256", hbaReject}, metricsRole()), password),
		Entry("wider CIDRs", clusterWith(nil, []string{"host all metrics 127.0.0.0/8 scram-sha-256", "host all metrics ::/0 scram-sha-256", hbaReject}, metricsRole()), password),
		Entry("the IP plus mask form", clusterWith(nil, []string{
			"host all metrics 127.0.0.1 255.255.255.255 scram-sha-256",
			"host all metrics ::1 ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff scram-sha-256", hbaReject,
		}, metricsRole()), password),
		Entry("the problems come out sorted",
			clusterWith(nil, nil, metricsRole()), password,
			`pg_hba has no line for role "metrics" from 127.0.0.1/32`,
			`pg_hba has no line for role "metrics" from ::1/128`,
			`pg_hba has no reject line for role "metrics"`),
	)
})

var _ = Describe("Check", func() {
	peer := podpatch.Params{Auth: podpatch.AuthPeer}

	It("accepts an exporter whose endpoint only envFrom provides", func() {
		e := newExporter()
		e.Spec.Exporter.Endpoint = ""
		e.Spec.EnvFrom = &v1alpha1.EnvFromSource{Secret: &v1alpha1.LocalObjectReference{Name: "otel"}}

		Expect(clusterconfig.Check(newCluster(), peer, e)).To(Succeed())
	})

	It("rejects an exporter with no endpoint and no envFrom", func() {
		e := newExporter()
		e.Spec.Exporter.Endpoint = ""

		Expect(clusterconfig.Check(newCluster(), peer, e)).To(HaveOccurred())
	})

	It("accepts an envFrom endpoint together with tls options", func() {
		e := newExporter()
		e.Spec.Exporter.Endpoint = ""
		e.Spec.EnvFrom = &v1alpha1.EnvFromSource{ConfigMap: &v1alpha1.LocalObjectReference{Name: "otel"}}
		e.Spec.Exporter.TLS = &v1alpha1.OTLPTLS{
			CA:                &v1alpha1.KeyReference{Secret: &v1alpha1.KeySelector{Name: "ca", Key: "ca.crt"}},
			ClientCertificate: &v1alpha1.LocalObjectReference{Name: "client"},
			ServerName:        "collector",
		}

		Expect(clusterconfig.Check(newCluster(), peer, e)).To(Succeed())
	})

	It("accepts a complete exporter", func() {
		Expect(clusterconfig.Check(newCluster(), peer, newExporter())).To(Succeed())
	})

	It("does not blame the exporter for a missing passwordSecret parameter", func() {
		p := podpatch.Params{Auth: podpatch.AuthPassword}

		Expect(clusterconfig.Check(newCluster(), p, newExporter())).To(Succeed())
	})

	It("accepts password mode without reading the Secret", func() {
		p := podpatch.Params{Auth: podpatch.AuthPassword, PasswordSecret: passSecret}

		Expect(clusterconfig.Check(newCluster(), p, newExporter())).To(Succeed())
	})

	It("rejects an invalid exclude glob, naming its index", func() {
		e := newExporter()
		e.Spec.Metrics.Exclude.Queries = []string{"pg_stat_["}

		Expect(clusterconfig.Check(newCluster(), peer, e)).To(MatchError(ContainSubstring("spec.metrics.exclude.queries[0]: invalid pattern")))
	})

	It("rejects an invalid group glob, naming its index", func() {
		e := newExporter()
		e.Spec.Metrics.Groups = []v1alpha1.QueryGroup{{Name: "g", Queries: []string{`a\`}}}

		Expect(clusterconfig.Check(newCluster(), peer, e)).To(MatchError(ContainSubstring("spec.metrics.groups[0].queries[0]")))
	})

	It("rejects an invalid endpoint", func() {
		e := newExporter()
		e.Spec.Exporter.Endpoint = "ftp://c:4318"

		Expect(clusterconfig.Check(newCluster(), peer, e)).To(HaveOccurred())
	})
})
