package framework_test

import (
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("PluginStatus", func() {
	clusterWith := func(statuses ...apiv1.PluginStatus) *apiv1.Cluster {
		c := &apiv1.Cluster{}
		c.Status.PluginStatus = statuses

		return c
	}

	It("decodes our pluginStatus entry and ignores others", func() {
		c := clusterWith(
			apiv1.PluginStatus{Name: "other.example", Status: `{"exporter":"x"}`},
			apiv1.PluginStatus{
				Name:   metadata.PluginName,
				Status: `{"exporter":"default","exporterState":"ok","identity":"ready","problems":["p"]}`,
			},
		)
		st, ok, err := framework.PluginStatus(c)
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(st.Exporter).To(Equal("default"))
		Expect(st.ExporterState).To(Equal("ok"))
		Expect(st.Identity).To(Equal("ready"))
		Expect(st.Problems).To(Equal([]string{"p"}))
	})

	It("reports false when our entry is absent or empty", func() {
		for _, c := range []*apiv1.Cluster{
			clusterWith(),
			clusterWith(apiv1.PluginStatus{Name: "other.example", Status: `{}`}),
			clusterWith(apiv1.PluginStatus{Name: metadata.PluginName}),
		} {
			_, ok, err := framework.PluginStatus(c)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
		}
	})

	It("fails when our entry is not JSON", func() {
		_, _, err := framework.PluginStatus(clusterWith(apiv1.PluginStatus{Name: metadata.PluginName, Status: "nope"}))
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ExporterFor", func() {
	It("points at the collector with a 10s metrics interval", func() {
		e := framework.ExporterFor("ns", "default")
		Expect(e.Namespace).To(Equal("ns"))
		Expect(e.Name).To(Equal("default"))
		Expect(e.Spec.Exporter.Endpoint).To(Equal("http://collector.telemetry-e2e.svc:4318"))
		Expect(e.Spec.Metrics.Interval.Duration).To(Equal(10 * time.Second))
	})
})
