package snapshot_test

import (
	"math"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

var (
	t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	k1 = snapshot.Key{Query: "q1", Database: "app"}
	k2 = snapshot.Key{Query: "q2", Database: "app"}
)

func counter(metric string, value float64, labels ...queries.Label) queries.Sample {
	return queries.Sample{Metric: metric, Kind: queries.KindCounter, Labels: labels, Value: value}
}

func gauge(metric string, value float64, labels ...queries.Label) queries.Sample {
	return queries.Sample{Metric: metric, Kind: queries.KindGauge, Labels: labels, Value: value}
}

func histogram(count uint64) queries.Sample {
	return queries.Sample{
		Metric: "h", Kind: queries.KindHistogram,
		Histogram: &queries.Histogram{Bounds: []float64{1, 2}, Cumulative: []uint64{count, count}, Count: count, Sum: 1},
	}
}

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

func only(s *snapshot.Store) snapshot.Point {
	GinkgoHelper()
	points, _ := s.Points()
	Expect(points).To(HaveLen(1))

	return points[0]
}

var _ = Describe("Store", func() {
	var s *snapshot.Store

	BeforeEach(func() { s = snapshot.New(0) })

	It("gives a new counter series Start equal to its first observation", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 5)}, at(10))).To(Succeed())

		p := only(s)
		Expect(p.Start).To(Equal(at(10)))
		Expect(p.Observed).To(Equal(at(10)))
		Expect(p.Value).To(Equal(5.0))
	})

	It("gives gauges a zero Start", func() {
		Expect(s.Put(k1, "f", []queries.Sample{gauge("g", 5)}, at(10))).To(Succeed())
		Expect(only(s).Start.IsZero()).To(BeTrue())
	})

	It("keeps Start while a counter grows, and moves it to the previous observation when the value drops", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 5)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 7)}, at(20))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))

		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 2)}, at(30))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(20)))

		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 2)}, at(40))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(20)))
	})

	It("ignores NaN for resets and keeps the last real value", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 5)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", math.NaN())}, at(20))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))

		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 6)}, at(30))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))

		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 4)}, at(40))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(30)))
	})

	It("starts a series whose first values are NaN at its first observation", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", math.NaN())}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 3)}, at(20))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))
	})

	It("applies the same reset rule to a histogram's Count", func() {
		Expect(s.Put(k1, "f", []queries.Sample{histogram(5)}, at(10))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))

		Expect(s.Put(k1, "f", []queries.Sample{histogram(8)}, at(20))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))

		Expect(s.Put(k1, "f", []queries.Sample{histogram(1)}, at(30))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(20)))
	})

	It("tracks start times per label set", func() {
		a := queries.Label{Key: "x", Value: "a"}
		b := queries.Label{Key: "x", Value: "b"}
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 5, a)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 1, a), counter("c", 9, b)}, at(20))).To(Succeed())

		points, _ := s.Points()
		Expect(points).To(HaveLen(2))
		Expect(points[0].Start).To(Equal(at(10)))
		Expect(points[1].Start).To(Equal(at(20)))
	})

	It("forgets a series' start when it leaves the entry, so its return starts fresh", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 5)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("other", 1)}, at(20))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 9)}, at(30))).To(Succeed())

		points, _ := s.Points()
		Expect(points).To(HaveLen(1))
		Expect(points[0].Start).To(Equal(at(30)))
	})

	It("stops exporting a failed key and keeps the start time of its counter", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 10)}, at(10))).To(Succeed())
		s.Fail(k1)
		points, _ := s.Points()
		Expect(points).To(BeEmpty())
		Expect(s.Series()).To(BeZero())

		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 15)}, at(30))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))
	})

	It("applies the reset rule against the last success after a failed run", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 10)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 12)}, at(20))).To(Succeed())
		s.Fail(k1)
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 3)}, at(40))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(20)))
	})

	It("ignores a Fail of an unknown key", func() {
		s.Fail(k1)
		Expect(s.Series()).To(BeZero())
	})

	It("forgets a failed key when Retain drops it", func() {
		Expect(s.Put(k1, "old", []queries.Sample{counter("c", 10)}, at(10))).To(Succeed())
		s.Fail(k1)
		s.Retain(func(snapshot.Key, string) bool { return false })

		Expect(s.Put(k1, "old", []queries.Sample{counter("c", 15)}, at(30))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(30)))
	})

	It("keeps the share of a failed key in maxSeriesTotal until its next Put", func() {
		s = snapshot.New(3)
		Expect(s.Put(k1, "f", []queries.Sample{counter("a", 1), counter("b", 1), counter("c", 1)}, at(10))).To(Succeed())
		s.Fail(k1)
		Expect(s.Series()).To(BeZero())

		Expect(s.Put(k2, "f", []queries.Sample{counter("d", 1)}, at(20))).To(MatchError(snapshot.ErrTotalCap))

		Expect(s.Put(k1, "f", []queries.Sample{counter("a", 2)}, at(30))).To(Succeed())
		Expect(s.Put(k2, "f", []queries.Sample{counter("d", 1)}, at(30))).To(Succeed())
		Expect(s.Series()).To(Equal(2))
	})

	It("rejects a Put above maxSeriesTotal and keeps the previous entry", func() {
		s = snapshot.New(3)
		Expect(s.Put(k1, "f", []queries.Sample{counter("a", 5), counter("b", 5)}, at(10))).To(Succeed())

		// replacing k1's 2 series with 3 gives a total of 3: allowed
		Expect(s.Put(k1, "f", []queries.Sample{counter("a", 6), counter("b", 6), counter("c", 6)}, at(20))).To(Succeed())

		// 4 would exceed
		err := s.Put(k1, "f", []queries.Sample{counter("a", 1), counter("b", 1), counter("c", 1), counter("d", 1)}, at(30))
		Expect(err).To(MatchError(snapshot.ErrTotalCap))
		Expect(s.Series()).To(Equal(3))

		points, _ := s.Points()
		Expect(points).To(HaveLen(3))
		Expect(points[0].Value).To(Equal(6.0))
		Expect(points[0].Observed).To(Equal(at(20)))

		// the start times are untouched: a later drop moves Start to the last stored observation
		Expect(s.Put(k1, "f", []queries.Sample{counter("a", 1)}, at(40))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(20)))
	})

	It("counts other entries against the cap", func() {
		s = snapshot.New(2)
		Expect(s.Put(k1, "f", []queries.Sample{gauge("a", 1)}, at(10))).To(Succeed())
		Expect(s.Put(k2, "f", []queries.Sample{gauge("b", 1), gauge("c", 1)}, at(10))).To(MatchError(snapshot.ErrTotalCap))
		s.SetMaxSeriesTotal(0)
		Expect(s.Put(k2, "f", []queries.Sample{gauge("b", 1), gauge("c", 1)}, at(10))).To(Succeed())
		Expect(s.Series()).To(Equal(3))
	})

	It("keeps entries Retain accepts across a plan swap, with their start times", func() {
		Expect(s.Put(k1, "same", []queries.Sample{counter("c", 5)}, at(10))).To(Succeed())
		Expect(s.Put(k2, "old", []queries.Sample{counter("d", 5)}, at(10))).To(Succeed())

		s.Retain(func(_ snapshot.Key, fingerprint string) bool { return fingerprint == "same" })
		Expect(s.Series()).To(Equal(1))

		Expect(s.Put(k1, "same", []queries.Sample{counter("c", 7)}, at(20))).To(Succeed())
		Expect(only(s).Start).To(Equal(at(10)))

		Expect(s.Put(k2, "new", []queries.Sample{counter("d", 9)}, at(30))).To(Succeed())
		points, _ := s.Points()
		Expect(points[0].Start).To(Equal(at(10)))
		Expect(points[1].Start).To(Equal(at(30)))
	})

	It("returns a duplicate series across entries once, from the first Key", func() {
		Expect(s.Put(k2, "f", []queries.Sample{gauge("m", 2), gauge("n", 2)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{gauge("m", 1)}, at(10))).To(Succeed())

		points, dups := s.Points()
		Expect(points).To(HaveLen(2))
		Expect(points[0].Metric).To(Equal("m"))
		Expect(points[0].Value).To(Equal(1.0))
		Expect(points[1].Metric).To(Equal("n"))
		Expect(dups).To(Equal(map[snapshot.Key]int{k2: 1}))
	})

	It("sorts by metric, then labels", func() {
		a := queries.Label{Key: "x", Value: "a"}
		b := queries.Label{Key: "x", Value: "b"}
		Expect(s.Put(k1, "f", []queries.Sample{gauge("z", 1), gauge("m", 1, b), gauge("m", 1, a), gauge("m", 1)}, at(10))).To(Succeed())

		points, _ := s.Points()
		Expect(points).To(HaveLen(4))
		Expect(points[0].Labels).To(BeEmpty())
		Expect(points[1].Labels).To(Equal([]queries.Label{a}))
		Expect(points[2].Labels).To(Equal([]queries.Label{b}))
		Expect(points[3].Metric).To(Equal("z"))
	})

	It("stores its own copy of the labels and the histogram", func() {
		shared := []queries.Label{{Key: "x", Value: "a"}}
		h := histogram(3)
		Expect(s.Put(k1, "f", []queries.Sample{gauge("g", 1, shared...), h}, at(10))).To(Succeed())

		shared[0].Value = "changed"
		h.Histogram.Cumulative[0] = 99

		points, _ := s.Points()
		Expect(points[0].Metric).To(Equal("g"))
		Expect(points[0].Labels[0].Value).To(Equal("a"))
		Expect(points[1].Histogram.Cumulative[0]).To(Equal(uint64(3)))
	})

	It("does not write into a caller's label slice", func() {
		backing := make([]queries.Label, 1, 8)
		backing[0] = queries.Label{Key: "x", Value: "a"}
		Expect(s.Put(k1, "f", []queries.Sample{gauge("g", 1, backing...)}, at(10))).To(Succeed())
		Expect(backing[:2][1]).To(Equal(queries.Label{}))
	})

	It("gives each Point the series id of Put", func() {
		labels := []queries.Label{{Key: "x", Value: "a"}}
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 1, labels...), gauge("g", 1)}, at(10))).To(Succeed())

		points, _ := s.Points()
		Expect(points).To(HaveLen(2))
		Expect(points[0].ID).To(Equal(queries.SeriesID("c", labels)))
		Expect(points[1].ID).To(Equal(queries.SeriesID("g", nil)))
	})

	It("keeps two series that differ only where a separator byte would join them", func() {
		joined := queries.Label{Key: "x", Value: "1\x00y\x012"}
		split := []queries.Label{{Key: "x", Value: "1"}, {Key: "y", Value: "2"}}
		Expect(s.Put(k1, "f", []queries.Sample{gauge("m", 1, joined)}, at(10))).To(Succeed())
		Expect(s.Put(k2, "f", []queries.Sample{gauge("m", 2, split...)}, at(10))).To(Succeed())

		points, dups := s.Points()
		Expect(points).To(HaveLen(2))
		Expect(dups).To(BeEmpty())
	})

	It("restarts a series whose kind changes", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("h", 5)}, at(10))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{histogram(3)}, at(20))).To(Succeed())

		Expect(only(s).Start).To(Equal(at(20)))
	})

	It("keeps the start of a histogram that arrives without data", func() {
		Expect(s.Put(k1, "f", []queries.Sample{histogram(3)}, at(10))).To(Succeed())
		empty := queries.Sample{Metric: "h", Kind: queries.KindHistogram}
		Expect(s.Put(k1, "f", []queries.Sample{empty}, at(20))).To(Succeed())
		Expect(s.Put(k1, "f", []queries.Sample{histogram(4)}, at(30))).To(Succeed())

		Expect(only(s).Start).To(Equal(at(10)))
	})

	It("forgets start times of an entry that Retain drops", func() {
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 5)}, at(10))).To(Succeed())
		s.Retain(func(snapshot.Key, string) bool { return false })
		Expect(s.Put(k1, "f", []queries.Sample{counter("c", 6)}, at(20))).To(Succeed())

		Expect(only(s).Start).To(Equal(at(20)))
	})

	It("counts a series repeated inside one entry against that entry", func() {
		Expect(s.Put(k1, "f", []queries.Sample{gauge("m", 1), gauge("m", 2)}, at(10))).To(Succeed())

		points, dups := s.Points()
		Expect(points).To(HaveLen(1))
		Expect(dups).To(Equal(map[snapshot.Key]int{k1: 1}))
	})

	It("never mixes two Puts of one entry in a Points", func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for n := range 500 {
				v := float64(n)
				Expect(s.Put(k1, "f", []queries.Sample{gauge("a", v), gauge("b", v), gauge("c", v)}, at(n))).To(Succeed())
			}
		}()
		for range 500 {
			points, _ := s.Points()
			for _, p := range points {
				Expect(p.Value).To(Equal(points[0].Value))
			}
		}
		wg.Wait()
	})

	It("is safe for concurrent use", func() {
		var wg sync.WaitGroup
		for i := range 4 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				k := snapshot.Key{Query: "q", Database: string(rune('a' + i))}
				for n := range 50 {
					Expect(s.Put(k, "f", []queries.Sample{counter("c", float64(n))}, at(n))).To(Succeed())
					if n%10 == 9 {
						s.Fail(k)
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for range 100 {
				s.Points()
				s.Series()
			}
		}()
		wg.Wait()
	})
})
