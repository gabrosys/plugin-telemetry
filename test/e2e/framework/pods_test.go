package framework_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/preflight"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("DecodeReport", func() {
	It("decodes the report line", func() {
		rep, err := framework.DecodeReport(`{"uid":65534,"gid":26,"groups":[26],"podTokenPresent":false,` +
			`"kube":{"username":"system:serviceaccount:ns:c-telemetry","clusterReadable":true},` +
			`"postgres":{"host":"/controller/run","user":"cnpg_metrics_exporter","connected":true,` +
			`"currentUser":"cnpg_metrics_exporter","walFiles":2,"archiveStatusFiles":1}}` + "\n")
		Expect(err).NotTo(HaveOccurred())
		Expect(rep).To(Equal(preflight.Report{
			UID:    65534,
			GID:    26,
			Groups: []int{26},
			Kube:   &preflight.KubeReport{Username: "system:serviceaccount:ns:c-telemetry", ClusterReadable: true},
			Postgres: &preflight.PostgresReport{
				Host:               "/controller/run",
				User:               "cnpg_metrics_exporter",
				Connected:          true,
				CurrentUser:        "cnpg_metrics_exporter",
				WALFiles:           2,
				ArchiveStatusFiles: 1,
			},
		}))
	})

	DescribeTable("rejects",
		func(out string) {
			_, err := framework.DecodeReport(out)
			Expect(err).To(MatchError(framework.ErrInvalidReport))
		},
		Entry("empty output", ""),
		Entry("text", "Error: boom\n"),
		Entry("an unknown key", `{"uid":1,"surprise":true}`),
		Entry("a second document", "{\"uid\":1}\n{\"uid\":2}\n"),
	)
})

var _ = Describe("DecodeReportFromLogs", func() {
	const (
		line   = `{"level":"info","msg":"starting preflight"}`
		report = `{"uid":65534,"podTokenPresent":false}`
	)

	It("returns the report among log lines", func() {
		rep, err := framework.DecodeReportFromLogs(line + "\n" + report + "\n")
		Expect(err).NotTo(HaveOccurred())
		Expect(rep).To(Equal(preflight.Report{UID: 65534}))
	})

	DescribeTable("rejects",
		func(logs string) {
			_, err := framework.DecodeReportFromLogs(logs)
			Expect(err).To(MatchError(framework.ErrInvalidReport))
		},
		Entry("no report", line+"\nError: boom\n"),
		Entry("empty logs", ""),
		Entry("two reports", report+"\n"+line+"\n"+report+"\n"),
	)
})
