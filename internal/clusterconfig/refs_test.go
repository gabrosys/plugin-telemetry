package clusterconfig_test

import (
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

func cmSel(name, key string) apiv1.ConfigMapKeySelector {
	return apiv1.ConfigMapKeySelector{LocalObjectReference: machineryapi.LocalObjectReference{Name: name}, Key: key}
}

func secSel(name, key string) apiv1.SecretKeySelector {
	return apiv1.SecretKeySelector{LocalObjectReference: machineryapi.LocalObjectReference{Name: name}, Key: key}
}

func monitored() *apiv1.Cluster {
	c := newCluster()
	c.Spec.Monitoring = &apiv1.MonitoringConfiguration{
		CustomQueriesConfigMap: []apiv1.ConfigMapKeySelector{cmSel("cnpg-default-monitoring", "queries"), cmSel("mine", "q.yaml")},
		CustomQueriesSecret:    []apiv1.SecretKeySelector{secSel("sq", "q.yaml")},
	}
	return c
}

var _ = Describe("References", func() {
	It("lists the Cluster, the exporter named by the parameter, or default", func() {
		r := clusterconfig.References(newCluster(), podpatch.Params{Exporter: "prod"}, nil)
		Expect(r.Cluster).To(Equal("pg"))
		Expect(r.Exporter).To(Equal("prod"))

		r = clusterconfig.References(newCluster(), podpatch.Params{}, nil)
		Expect(r.Exporter).To(Equal("default"))
		Expect(r.ConfigMaps).To(BeEmpty())
		Expect(r.Secrets).To(BeEmpty())
		Expect(r.QuerySets).To(BeEmpty())
	})

	It("lists spec.monitoring ConfigMaps and Secrets, cnpg-default-monitoring included, when useClusterMonitoring is unset or true", func() {
		for _, use := range []*bool{nil, ptr.To(true)} {
			e := newExporter()
			e.Spec.Metrics.UseClusterMonitoring = use
			r := clusterconfig.References(monitored(), peerParams(), e)
			Expect(r.ConfigMaps).To(Equal([]string{"cnpg-default-monitoring", "mine"}))
			Expect(r.Secrets).To(Equal([]string{"sq"}))
		}
		r := clusterconfig.References(monitored(), peerParams(), nil)
		Expect(r.ConfigMaps).To(Equal([]string{"cnpg-default-monitoring", "mine"}))
		Expect(r.Secrets).To(Equal([]string{"sq"}))
	})

	It("drops spec.monitoring objects when useClusterMonitoring is false", func() {
		e := newExporter()
		e.Spec.Metrics.UseClusterMonitoring = ptr.To(false)
		r := clusterconfig.References(monitored(), peerParams(), e)
		Expect(r.ConfigMaps).To(BeEmpty())
		Expect(r.Secrets).To(BeEmpty())
	})

	It("adds metrics.querySets, the headers Secret, the TLS CA Secret or ConfigMap, the client certificate Secret and the envFrom objects", func() {
		e := newExporter()
		e.Spec.Metrics.QuerySets = []string{"qs1", "qs2"}
		e.Spec.Exporter.Headers = &v1alpha1.KeySelector{Name: "hdr", Key: "h"}
		e.Spec.Exporter.TLS = &v1alpha1.OTLPTLS{
			CA:                &v1alpha1.KeyReference{Secret: &v1alpha1.KeySelector{Name: "ca-s", Key: "ca.crt"}},
			ClientCertificate: &v1alpha1.LocalObjectReference{Name: "client"},
		}
		e.Spec.EnvFrom = &v1alpha1.EnvFromSource{
			ConfigMap: &v1alpha1.LocalObjectReference{Name: "envcm"},
			Secret:    &v1alpha1.LocalObjectReference{Name: "envsecret"},
		}
		r := clusterconfig.References(newCluster(), peerParams(), e)
		Expect(r.QuerySets).To(Equal([]string{"qs1", "qs2"}))
		Expect(r.Secrets).To(Equal([]string{"ca-s", "client", "envsecret", "hdr"}))
		Expect(r.ConfigMaps).To(Equal([]string{"envcm"}))

		e.Spec.Exporter.TLS.CA = &v1alpha1.KeyReference{ConfigMap: &v1alpha1.KeySelector{Name: "ca-cm", Key: "ca.crt"}}
		r = clusterconfig.References(newCluster(), peerParams(), e)
		Expect(r.ConfigMaps).To(Equal([]string{"ca-cm", "envcm"}))
		Expect(r.Secrets).To(Equal([]string{"client", "envsecret", "hdr"}))
	})

	It("adds the password Secret in password mode only", func() {
		p := podpatch.Params{Exporter: "default", Auth: podpatch.AuthPassword, PasswordSecret: "pw"}
		Expect(clusterconfig.References(newCluster(), p, newExporter()).Secrets).To(Equal([]string{"pw"}))
		Expect(clusterconfig.References(newCluster(), p, nil).Secrets).To(Equal([]string{"pw"}))

		p.Auth = podpatch.AuthPeer
		Expect(clusterconfig.References(newCluster(), p, newExporter()).Secrets).To(BeEmpty())
	})

	It("sorts and deduplicates every list and never returns an empty name", func() {
		c := newCluster()
		c.Spec.Monitoring = &apiv1.MonitoringConfiguration{
			CustomQueriesConfigMap: []apiv1.ConfigMapKeySelector{cmSel("b", "x"), cmSel("a", "x"), cmSel("b", "y"), cmSel("", "z")},
			CustomQueriesSecret:    []apiv1.SecretKeySelector{secSel("s", "x"), secSel("s", "y")},
		}
		e := newExporter()
		e.Spec.Metrics.QuerySets = []string{"q2", "q1", "q2", ""}
		e.Spec.Exporter.Headers = &v1alpha1.KeySelector{Name: "s", Key: "h"}
		e.Spec.EnvFrom = &v1alpha1.EnvFromSource{ConfigMap: &v1alpha1.LocalObjectReference{Name: "a"}}
		r := clusterconfig.References(c, podpatch.Params{Auth: podpatch.AuthPassword}, e)
		Expect(r.ConfigMaps).To(Equal([]string{"a", "b"}))
		Expect(r.Secrets).To(Equal([]string{"s"}))
		Expect(r.QuerySets).To(Equal([]string{"q1", "q2"}))
	})
})
