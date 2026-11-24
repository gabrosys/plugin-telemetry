package framework_test

import (
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
		var want []string
		for _, msg := range messages {
			want = append(want, line(after, "ns-a", msg))
		}
		nested := strings.Replace(line(after, "ns-b", messages[0]), `"controller":"cluster"`,
			`"controller":"cluster","Cluster":{"name":"resilience","namespace":"ns-a"}`, 1)
		logs := strings.Join(append(slices.Clone(want),
			line(since.Add(-time.Second), "ns-a", messages[0]),
			line(after, "ns-b", messages[0]),
			line(after, "ns-a", "Detected stale/broken plugin connection, closing and trying again"),
			nested,
			line(since, "ns-a", messages[0]),
			"E1116 10:00:05 not json: Error loading plugins, retrying",
		), "\n")

		Expect(framework.ParseFailedHookCalls(logs, "ns-a", since)).To(Equal(want))
	})
})

var _ = Describe("CheckOperatorRestart", func() {
	since := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	terminated := func(at time.Time) *corev1.ContainerStateTerminated {
		return &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(at)}
	}
	pod := func(statuses ...corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "cnpg-1"},
			Status:     corev1.PodStatus{ContainerStatuses: statuses},
		}
	}

	It("accepts a restart before since", func() {
		p := pod(corev1.ContainerStatus{Name: "manager", LastTerminationState: corev1.ContainerState{Terminated: terminated(since.Add(-time.Minute))}})
		Expect(framework.CheckOperatorRestart(p, since)).To(Succeed())
	})

	It("accepts a Pod that never restarted", func() {
		Expect(framework.CheckOperatorRestart(pod(corev1.ContainerStatus{Name: "manager"}), since)).To(Succeed())
	})

	It("rejects a restart after since", func() {
		p := pod(corev1.ContainerStatus{Name: "manager", LastTerminationState: corev1.ContainerState{Terminated: terminated(since.Add(time.Minute))}})
		Expect(framework.CheckOperatorRestart(p, since)).To(MatchError(ContainSubstring("the CNPG operator container cnpg-1 restarted at")))
	})

	It("rejects a current termination after since", func() {
		p := pod(corev1.ContainerStatus{Name: "manager", State: corev1.ContainerState{Terminated: terminated(since.Add(time.Minute))}})
		Expect(framework.CheckOperatorRestart(p, since)).To(MatchError(ContainSubstring("its earlier log is gone")))
	})

	It("ignores other containers", func() {
		p := pod(corev1.ContainerStatus{Name: "other", LastTerminationState: corev1.ContainerState{Terminated: terminated(since.Add(time.Minute))}})
		Expect(framework.CheckOperatorRestart(p, since)).To(Succeed())
	})
})
