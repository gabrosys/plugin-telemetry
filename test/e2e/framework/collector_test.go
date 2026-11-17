package framework_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	lineAt2 = `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"a"},"metrics":[{"name":"m","gauge":` +
		`{"dataPoints":[{"timeUnixNano":"2000000000","asInt":"1"}]}}]}]}]}` + "\n"
	lineAt3 = `{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"a"},"metrics":[{"name":"m","gauge":` +
		`{"dataPoints":[{"timeUnixNano":"3000000000","asInt":"2"}]}}]}]}]}` + "\n"
)

var _ = Describe("CollectorReader", func() {
	var (
		files   map[types.UID]string // what each fake pod's file holds
		offsets map[types.UID][]int  // offsets each read was asked for
		reader  *framework.CollectorReader
	)

	pod := func(uid types.UID) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: uid}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	}

	BeforeEach(func() {
		files = map[types.UID]string{}
		offsets = map[types.UID][]int{}
		reader = framework.NewCollectorReaderWith(
			func(context.Context) ([]corev1.Pod, error) {
				var pods []corev1.Pod
				for uid := range files {
					pods = append(pods, pod(uid))
				}

				return pods, nil
			},
			func(_ context.Context, p *corev1.Pod, offset int) (string, error) {
				offsets[p.UID] = append(offsets[p.UID], offset)

				return files[p.UID][offset:], nil
			})
	})

	It("starts a new pod at offset 0", func(ctx SpecContext) {
		files["a"] = lineAt2
		_, err := reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(offsets["a"]).To(Equal([]int{0}))
	})

	It("accumulates lines across calls and filters them by time", func(ctx SpecContext) {
		files["a"] = lineAt2
		got, err := reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))

		files["a"] += lineAt3
		got, err = reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
		Expect(offsets["a"]).To(Equal([]int{0, len(lineAt2)}))

		got, err = reader.Since(ctx, time.Unix(3, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
	})

	It("leaves a partial last line for the next call", func(ctx SpecContext) {
		files["a"] = lineAt2 + lineAt3[:10]
		got, err := reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))

		files["a"] = lineAt2 + lineAt3
		got, err = reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
		Expect(offsets["a"]).To(Equal([]int{0, len(lineAt2)}))
	})

	It("reads two pods independently", func(ctx SpecContext) {
		files["a"] = lineAt2
		_, err := reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())

		files["b"] = lineAt3
		got, err := reader.Since(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
		Expect(offsets["b"]).To(Equal([]int{0}))
		Expect(offsets["a"]).To(Equal([]int{0, len(lineAt2)}))
	})

	It("reports no running pod", func(ctx SpecContext) {
		_, err := reader.Since(ctx, time.Time{})
		Expect(err).To(MatchError(framework.ErrNoCollectorPod))
	})

	It("wraps the read error", func(ctx SpecContext) {
		r := framework.NewCollectorReaderWith(
			func(context.Context) ([]corev1.Pod, error) { return []corev1.Pod{pod("a")}, nil },
			func(context.Context, *corev1.Pod, int) (string, error) { return "", errors.New("boom") })
		_, err := r.Since(ctx, time.Time{})
		Expect(err).To(MatchError(ContainSubstring("boom")))
	})
})
