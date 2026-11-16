package framework_test

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("ParseFailedHookCalls", func() {
	It("finds the five failure messages for one namespace after since", func() {
		since := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
		line := func(ts time.Time, ns, msg string) string {
			return fmt.Sprintf(`{"level":"error","ts":%q,"msg":%q,"controller":"cluster","namespace":%q,"name":"resilience"}`,
				ts.Format(time.RFC3339Nano), msg, ns)
		}
		after := since.Add(time.Second + 123456789)

		messages := []string{
			"Error loading plugins, retrying",
			"Pre-reconcile hook stopped the reconciliation loop",
			"Error while calling LifecycleHook",
			"Post-reconcile hook returned an error",
			"Error while calling SetStatusInCluster",
		}
		var (
			logs strings.Builder
			want []string
		)
		for _, msg := range messages {
			l := line(after, "ns-a", msg)
			want = append(want, l)
			logs.WriteString(l + "\n")
		}
		logs.WriteString(line(since.Add(-time.Second), "ns-a", messages[0]) + "\n")
		logs.WriteString(line(after, "ns-b", messages[0]) + "\n")
		logs.WriteString(line(after, "ns-a", "Detected stale/broken plugin connection, closing and trying again") + "\n")
		logs.WriteString("E1116 10:00:05 not json: Error loading plugins, retrying\n")

		Expect(framework.ParseFailedHookCalls(logs.String(), "ns-a", since)).To(Equal(want))
	})
})
