package framework_test

import (
	"context"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("ParseUsage", func() {
	pod := types.NamespacedName{Namespace: "ns", Name: "pod-1"}

	summary := func() []byte {
		data, err := os.ReadFile("testdata/kubelet-summary.json")
		Expect(err).NotTo(HaveOccurred())

		return data
	}

	It("reads a native sidecar's working set, RSS and CPU from the stats summary", func() {
		got, err := framework.ParseUsage(summary(), pod, "otel-metrics")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(framework.Usage{WorkingSet: 12341248, RSS: 11751424, CPUNanoCores: 31556}))
	})

	It("fails with ErrNoContainer for a container the summary doesn't list", func() {
		_, err := framework.ParseUsage(summary(), pod, "nope")
		Expect(err).To(MatchError(framework.ErrNoContainer))

		_, err = framework.ParseUsage(summary(), types.NamespacedName{Namespace: "ns", Name: "other"}, "postgres")
		Expect(err).To(MatchError(framework.ErrNoContainer))
	})
})

var _ = Describe("SampleUsage", func() {
	pods := []types.NamespacedName{{Namespace: "ns", Name: "a"}, {Namespace: "ns", Name: "b"}}

	It("samples each pod once per tick for the duration, in pod order, and returns what it has when the context ends", func() {
		clk := clocktesting.NewFakeClock(time.Unix(0, 0))
		var (
			mu    sync.Mutex
			calls int
		)
		read := func(_ context.Context, pod types.NamespacedName) (framework.Usage, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			w := uint64(calls)
			if pod.Name == "b" {
				w += 100
			}

			return framework.Usage{WorkingSet: w}, nil
		}
		count := func() int {
			mu.Lock()
			defer mu.Unlock()

			return calls
		}

		type result struct {
			got []framework.Usage
			err error
		}
		step := func(done <-chan result, want int) {
			Eventually(count).Should(Equal(want))
			Consistently(done, 20*time.Millisecond).ShouldNot(Receive())
		}

		// Full duration: four ticks over 30 s (t=0, 10, 20, 30), the first at once.
		done := make(chan result, 1)
		go func() {
			defer GinkgoRecover()
			got, err := framework.SampleUsageWith(context.Background(), clk, read, pods, 10*time.Second, 30*time.Second)
			done <- result{got, err}
		}()
		step(done, 2)
		Eventually(clk.HasWaiters).Should(BeTrue())
		clk.Step(10 * time.Second)
		step(done, 4)
		clk.Step(10 * time.Second)
		step(done, 6)
		clk.Step(10 * time.Second)
		var res result
		Eventually(done).Should(Receive(&res))
		Expect(res.err).NotTo(HaveOccurred())
		Expect(res.got).To(Equal([]framework.Usage{
			{WorkingSet: 1},
			{WorkingSet: 102},
			{WorkingSet: 3},
			{WorkingSet: 104},
			{WorkingSet: 5},
			{WorkingSet: 106},
			{WorkingSet: 7},
			{WorkingSet: 108},
		}))

		// Context ends after the second tick: the readings so far come back with its error.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			defer GinkgoRecover()
			got, err := framework.SampleUsageWith(ctx, clk, read, pods, 10*time.Second, time.Hour)
			done <- result{got, err}
		}()
		step(done, 10)
		Eventually(clk.HasWaiters).Should(BeTrue())
		cancel()
		Eventually(done).Should(Receive(&res))
		Expect(res.err).To(MatchError(context.Canceled))
		Expect(res.got).To(HaveLen(2))
	})
})

var _ = Describe("StatOf", func() {
	It("returns the min, median and max of unsorted readings", func() {
		Expect(framework.StatOf([]uint64{5, 1, 9, 3, 7})).To(Equal(framework.Stat{Min: 1, Median: 5, Max: 9}))
	})
})

var _ = Describe("Settled", func() {
	flat := func(v uint64) []uint64 { return []uint64{v, v, v, v} }

	DescribeTable("compares the median of the latest window with the previous one",
		func(prev, cur []uint64, want bool) {
			Expect(framework.Settled(prev, cur, 0.02)).To(Equal(want))
		},
		Entry("the CI series is still climbing",
			[]uint64{18677760, 18694144, 18694144, 18718720}, []uint64{18722816, 18722816, 19189760, 19775488}, false),
		Entry("a flat series is settled",
			[]uint64{21106688, 21139456, 21139456, 21151744}, []uint64{21164032, 21168128, 21168128, 20779008}, true),
		Entry("exactly 2 % up is settled", flat(1000), flat(1020), true),
		Entry("just over 2 % up is not", flat(1000), flat(1021), false),
		Entry("a 2 % drop is settled", flat(1000), flat(980), true),
		Entry("more than a 2 % drop is not", flat(1000), flat(979), false),
		Entry("an empty previous window is not", nil, flat(1000), false),
		Entry("an empty latest window is not", flat(1000), []uint64{}, false),
	)
})
