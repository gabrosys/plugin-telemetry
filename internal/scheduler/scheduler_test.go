package scheduler_test

import (
	"cmp"
	"context"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/scheduler"
)

type runRecord struct {
	job planner.Job
	at  time.Time
}

type skipRecord struct {
	job    planner.Job
	missed int
}

// harness wires a Scheduler to a fake clock and records every run and skip.
type harness struct {
	clk      *testingclock.FakeClock
	s        *scheduler.Scheduler
	hook     func(ctx context.Context, job planner.Job)
	skipHook func()

	mu    sync.Mutex // guards runs and skips
	runs  []runRecord
	skips []skipRecord

	cancel context.CancelFunc
	done   chan error
}

func newHarness() *harness {
	h := &harness{clk: testingclock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))}
	h.s = scheduler.New(h.clk, seeded(),
		func(ctx context.Context, job planner.Job) {
			h.mu.Lock()
			h.runs = append(h.runs, runRecord{job: job, at: h.clk.Now()})
			h.mu.Unlock()
			if h.hook != nil {
				h.hook(ctx, job)
			}
		},
		func(job planner.Job, missed int) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.skips = append(h.skips, skipRecord{job: job, missed: missed})
			if h.skipHook != nil {
				h.skipHook()
			}
		})
	return h
}

// seeded returns the source every spec's Scheduler draws from.
func seeded() *rand.Rand {
	return rand.New(rand.NewPCG(1, 2)) //nolint:gosec // seeded for reproducible timing
}

func (h *harness) start() {
	var ctx context.Context
	ctx, h.cancel = context.WithCancel(context.Background())
	h.done = make(chan error, 1)
	go func() { h.done <- h.s.Run(ctx) }()
}

func (h *harness) stop() {
	if h.cancel != nil {
		h.cancel()
		Eventually(h.done).Should(Receive(Succeed()))
		h.cancel = nil
	}
}

func (h *harness) settle(workers int) {
	GinkgoHelper()
	Eventually(h.clk.Waiters).Should(Equal(workers))
}

// advance steps the clock by total in steps of q, waiting for the workers to re-arm after a timer fired.
func (h *harness) advance(total, q time.Duration, workers int) {
	GinkgoHelper()
	for elapsed := time.Duration(0); elapsed < total; elapsed += q {
		h.clk.Step(q)
		if h.clk.Waiters() < workers {
			h.settle(workers)
		}
	}
}

// stepUntilFired steps by q until a timer has fired, and returns.
func (h *harness) stepUntilFired(q time.Duration, maxSteps int) {
	GinkgoHelper()
	for range maxSteps {
		h.clk.Step(q)
		if !h.clk.HasWaiters() {
			return
		}
	}
	Fail("no timer fired")
}

func (h *harness) runsOf(query string) []time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []time.Time
	for _, r := range h.runs {
		if r.job.Query == query {
			out = append(out, r.at)
		}
	}
	return out
}

func (h *harness) runCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.runs)
}

func (h *harness) skipList() []skipRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.skips)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func job(db, query string, interval time.Duration) planner.Job {
	return planner.Job{Query: query, Database: db, Interval: interval, Fingerprint: "fp-" + query}
}

func plan(jobs ...planner.Job) planner.Plan { return planner.Plan{Jobs: jobs} }

var _ = Describe("Scheduler", func() {
	var h *harness
	var epoch time.Time

	BeforeEach(func() {
		h = newHarness()
		epoch = h.clk.Now()
	})
	AfterEach(func() { h.stop() })

	It("starts every job within its initial delay", func() {
		h.s.Update(plan(
			job("a", "q10", 10*time.Second),
			job("a", "q60", 60*time.Second),
			job("b", "q3", 3*time.Second),
		))
		h.start()
		h.settle(2)

		q := 100 * time.Millisecond
		h.advance(11*time.Second, q, 2)

		limits := map[string]time.Duration{"q10": 10 * time.Second, "q60": scheduler.MaxInitialDelay, "q3": 3 * time.Second}
		for query, limit := range limits {
			runs := h.runsOf(query)
			Expect(runs).NotTo(BeEmpty(), query)
			Expect(runs[0].Sub(epoch)).To(BeNumerically("<", limit+q), query)
		}
	})

	DescribeTable("runs a job at its due time, then every Interval plus at most min(Interval/10, 1s), never sooner",
		func(interval time.Duration) {
			h.s.Update(plan(job("a", "q", interval)))
			h.start()
			h.settle(1)

			q := interval / 100
			for range 10_000 {
				if len(h.runsOf("q")) >= 50 {
					break
				}
				h.advance(q, q, 1)
			}
			runs := h.runsOf("q")
			Expect(runs).To(HaveLen(50))

			// Replays the scheduler's draws: one initial delay, then one jitter per run.
			rnd := seeded()
			due := epoch.Add(time.Duration(rnd.Int64N(int64(min(interval, scheduler.MaxInitialDelay)))))
			for i, at := range runs {
				Expect(at).To(BeTemporally(">=", due), "run %d", i)
				Expect(at).To(BeTemporally("<", due.Add(q)), "run %d", i)
				next := due.Add(interval + time.Duration(rnd.Int64N(int64(min(interval/10, scheduler.MaxJitter)))))
				due = later(next, at.Add(interval))
			}
		},
		Entry("10s group", 10*time.Second),
		Entry("60s group", 60*time.Second),
		Entry("cache_seconds above the group interval", 90*time.Second),
		Entry("sub-10s interval", 2*time.Second),
	)

	It("draws the first due time from the seeded source in [0, min(interval, MaxInitialDelay))", func() {
		const first = 7693732693 * time.Nanosecond // seeded().Int64N(int64(10 * time.Second))
		h.s.Update(plan(job("a", "q", time.Minute)))
		h.start()
		h.settle(1)

		h.clk.Step(first - 1)
		h.settle(1)
		Expect(h.runCount()).To(BeZero())

		h.clk.Step(1)
		Eventually(h.runCount).Should(Equal(1))
		Expect(h.runsOf("q")[0]).To(Equal(epoch.Add(first)))
	})

	It("runs a 10s group and a 60s group each at its own interval", func() {
		h.s.Update(plan(job("a", "q10", 10*time.Second), job("a", "q60", 60*time.Second)))
		h.start()
		h.settle(1)

		q := 100 * time.Millisecond
		h.advance(600*time.Second, q, 1)

		for query, interval := range map[string]time.Duration{"q10": 10 * time.Second, "q60": 60 * time.Second} {
			runs := h.runsOf(query)
			for i := 1; i < len(runs); i++ {
				gap := runs[i].Sub(runs[i-1])
				Expect(gap).To(BeNumerically(">", interval-q), query)
				Expect(gap).To(BeNumerically("<", interval+min(interval/10, scheduler.MaxJitter)+q), query)
			}
		}
		Expect(len(h.runsOf("q10"))).To(BeNumerically("~", 57, 4))
		Expect(len(h.runsOf("q60"))).To(BeNumerically("~", 10, 1))
	})

	It("runs jobs of one database one at a time and jobs of two databases in parallel", func() {
		var mu sync.Mutex // guards inflight and peak
		inflight := map[string]int{}
		peak := map[string]int{}
		entered := make(chan planner.Job, 8)
		release := make(chan struct{})
		h.hook = func(_ context.Context, j planner.Job) {
			mu.Lock()
			inflight[j.Database]++
			peak[j.Database] = max(peak[j.Database], inflight[j.Database])
			mu.Unlock()
			entered <- j
			<-release
			mu.Lock()
			inflight[j.Database]--
			mu.Unlock()
		}
		DeferCleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})

		h.s.Update(plan(
			job("a", "q1", 10*time.Second), job("a", "q2", 10*time.Second), job("b", "q3", 10*time.Second),
		))
		h.start()
		h.settle(2)
		h.clk.Step(11 * time.Second)

		var first, second planner.Job
		Eventually(entered).Should(Receive(&first))
		Eventually(entered).Should(Receive(&second))
		Expect([]string{first.Database, second.Database}).To(ConsistOf("a", "b"))

		close(release)
		Eventually(entered).Should(Receive())
		Expect(h.runCount()).To(Equal(3))
		mu.Lock()
		defer mu.Unlock()
		Expect(peak).To(Equal(map[string]int{"a": 1, "b": 1}))
	})

	It("skips and reports runs that fall due while the job is still running", func() {
		var calls int
		gate := make(chan struct{})
		h.hook = func(_ context.Context, _ planner.Job) {
			calls++
			if calls == 1 {
				<-gate
			}
		}
		DeferCleanup(func() {
			select {
			case <-gate:
			default:
				close(gate)
			}
		})

		h.s.Update(plan(job("a", "q", 10*time.Second)))
		h.start()
		h.settle(1)
		q := 100 * time.Millisecond
		h.stepUntilFired(q, 200)
		Eventually(h.runCount).Should(Equal(1))
		t0 := h.runsOf("q")[0]

		h.clk.Step(35*time.Second - h.clk.Now().Sub(t0))
		close(gate)
		Eventually(h.skipList).Should(HaveLen(1))
		Expect(h.skipList()[0].missed).To(Equal(3))
		Expect(h.skipList()[0].job.Query).To(Equal("q"))
		h.settle(1)
		Expect(h.runCount()).To(Equal(1))

		h.stepUntilFired(q, 200)
		Eventually(h.runCount).Should(Equal(2))
		second := h.runsOf("q")[1]
		Expect(second.Sub(t0)).To(BeNumerically(">=", 40*time.Second-q))
		Expect(second.Sub(t0)).To(BeNumerically("<", 41*time.Second+q))
		h.settle(1)
		Expect(h.skipList()).To(HaveLen(1))
	})

	It("keeps every worker running after a RunFunc panics", func() {
		h.hook = func(context.Context, planner.Job) { panic("run broke") }
		h.s.Update(plan(job("a", "qa", 10*time.Second), job("b", "qb", 10*time.Second)))
		h.start()
		h.settle(2)

		h.advance(32*time.Second, 100*time.Millisecond, 2)

		Expect(h.runsOf("qa")).To(HaveLen(3))
		Expect(h.runsOf("qb")).To(HaveLen(3))
		Expect(h.s.WorkerCount()).To(Equal(2))
	})

	It("keeps the worker running after a SkipFunc panics", func() {
		var calls int
		h.hook = func(context.Context, planner.Job) {
			calls++
			if calls == 1 {
				h.clk.Step(35 * time.Second)
			}
		}
		h.skipHook = func() { panic("skip broke") }
		h.s.Update(plan(job("a", "q", 10*time.Second)))
		h.start()
		h.settle(1)
		h.stepUntilFired(100*time.Millisecond, 200)
		Eventually(h.skipList).Should(HaveLen(1))

		h.advance(15*time.Second, 100*time.Millisecond, 1)
		Expect(h.runCount()).To(BeNumerically(">=", 2))
	})

	It("keeps the next due time of an unchanged job across Update, and starts changed and new jobs fresh", func() {
		h.s.Update(plan(job("a", "q1", 60*time.Second), job("a", "q2", 60*time.Second)))
		h.start()
		h.settle(1)
		q := 100 * time.Millisecond
		h.advance(11*time.Second, q, 1)
		Expect(h.runsOf("q1")).To(HaveLen(1))
		Expect(h.runsOf("q2")).To(HaveLen(1))

		h.s.Update(plan(job("a", "q1", 30*time.Second), job("a", "q2", 60*time.Second), job("a", "q3", 60*time.Second)))
		h.settle(1)
		h.advance(11*time.Second, q, 1)

		Expect(h.runsOf("q1")).To(HaveLen(2))
		Expect(h.runsOf("q3")).To(HaveLen(1))
		Expect(h.runsOf("q2")).To(HaveLen(1))
	})

	It("stops the worker of a database that leaves the plan", func() {
		h.s.Update(plan(job("a", "qa", 10*time.Second), job("b", "qb", 10*time.Second)))
		h.start()
		h.settle(2)
		q := 100 * time.Millisecond
		h.advance(11*time.Second, q, 2)
		Expect(h.runsOf("qb")).To(HaveLen(1))
		aBefore := len(h.runsOf("qa"))

		h.s.Update(plan(job("a", "qa", 10*time.Second)))
		h.settle(1)
		h.advance(100*time.Second, time.Second, 1)

		Expect(h.runsOf("qb")).To(HaveLen(1))
		Expect(len(h.runsOf("qa"))).To(BeNumerically(">", aBefore))
		Eventually(h.s.WorkerCount).Should(Equal(1))

		h.s.Update(plan(job("a", "qa", 10*time.Second), job("b", "qb", 10*time.Second)))
		Expect(h.s.WorkerCount()).To(Equal(2))
		h.settle(2)
		h.advance(scheduler.MaxInitialDelay+time.Second, q, 2)
		Expect(h.runsOf("qb")).To(HaveLen(2))
	})

	It("runs the overdue jobs of a database earliest due first", func() {
		const n = 5
		interval := 10 * time.Second
		var jobs []planner.Job
		for i := range n {
			jobs = append(jobs, job("a", "q"+string(rune('1'+i)), interval))
		}
		// Update draws one initial delay per job, in plan order, from the seeded source.
		rnd := seeded()
		due := map[string]time.Duration{}
		for _, j := range jobs {
			due[j.Query] = time.Duration(rnd.Int64N(int64(interval)))
		}
		want := slices.SortedFunc(maps.Keys(due), func(a, b string) int { return cmp.Compare(due[a], due[b]) })
		Expect(want).NotTo(Equal([]string{"q1", "q2", "q3", "q4", "q5"}), "the oracle must differ from plan order")

		gate := make(chan struct{})
		var once sync.Once
		h.hook = func(_ context.Context, _ planner.Job) { once.Do(func() { <-gate }) }
		DeferCleanup(func() { once.Do(func() {}); closeOnce(gate) })

		h.s.Update(plan(jobs...))
		h.start()
		h.settle(1)
		h.clk.Step(11 * time.Second)
		Eventually(h.runCount).Should(Equal(1))
		close(gate)
		Eventually(h.runCount).Should(Equal(n))

		h.mu.Lock()
		defer h.mu.Unlock()
		var got []string
		for _, r := range h.runs[:n] {
			got = append(got, r.job.Query)
		}
		Expect(got).To(Equal(want))
	})

	It("drops a job removed while it runs, without a skip or another run", func() {
		entered := make(chan struct{})
		gate := make(chan struct{})
		h.hook = func(_ context.Context, _ planner.Job) {
			close(entered)
			<-gate
		}
		DeferCleanup(func() { closeOnce(gate) })

		h.s.Update(plan(job("a", "q", 10*time.Second)))
		h.start()
		h.settle(1)
		h.clk.Step(11 * time.Second)
		Eventually(entered).Should(BeClosed())

		h.s.Update(plan())
		h.clk.Step(35 * time.Second)
		close(gate)
		Eventually(h.s.WorkerCount).Should(Equal(0))
		h.advance(100*time.Second, time.Second, 0)

		Expect(h.runsOf("q")).To(HaveLen(1))
		Expect(h.skipList()).To(BeEmpty())
	})

	It("starts a job changed while it runs fresh, without a skip", func() {
		var calls int
		gate := make(chan struct{})
		h.hook = func(_ context.Context, _ planner.Job) {
			calls++
			if calls == 1 {
				<-gate
			}
		}
		DeferCleanup(func() { closeOnce(gate) })

		h.s.Update(plan(job("a", "q", 10*time.Second)))
		h.start()
		h.settle(1)
		h.clk.Step(11 * time.Second)
		Eventually(h.runCount).Should(Equal(1))

		h.s.Update(plan(job("a", "q", 20*time.Second)))
		h.clk.Step(35 * time.Second)
		close(gate)
		Eventually(h.runCount).Should(Equal(2))
		h.settle(1)

		Expect(h.skipList()).To(BeEmpty())
	})

	It("keeps the interval between the start of a late run and the next one", func() {
		var calls int
		gate := make(chan struct{})
		h.hook = func(_ context.Context, _ planner.Job) {
			calls++
			if calls == 1 {
				<-gate
			}
		}
		DeferCleanup(func() { closeOnce(gate) })

		interval := 10 * time.Second
		h.s.Update(plan(job("a", "q1", interval), job("a", "q2", interval)))
		h.start()
		h.settle(1)
		h.clk.Step(11 * time.Second)
		Eventually(h.runCount).Should(Equal(1))
		h.clk.Step(5 * time.Second)
		close(gate)
		Eventually(h.runCount).Should(Equal(2))
		h.settle(1)

		h.mu.Lock()
		late := h.runs[1]
		h.mu.Unlock()
		Expect(late.at.Sub(epoch)).To(Equal(16 * time.Second))

		q := 100 * time.Millisecond
		h.advance(30*time.Second, q, 1)
		runs := h.runsOf(late.job.Query)
		Expect(len(runs)).To(BeNumerically(">=", 2))
		Expect(runs[1].Sub(runs[0])).To(BeNumerically(">=", interval))
	})

	It("gives a job whose fingerprint alone changed a new due time, as a new job", func() {
		interval := 60 * time.Second
		h.s.Update(plan(job("a", "q", interval)))
		h.start()
		h.settle(1)
		h.advance(11*time.Second, 100*time.Millisecond, 1)
		Expect(h.runsOf("q")).To(HaveLen(1))
		ran := h.runsOf("q")[0]

		changed := job("a", "q", interval)
		changed.Fingerprint = "another"
		h.s.Update(plan(changed))
		h.settle(1)
		h.advance(11*time.Second, 100*time.Millisecond, 1)

		Expect(h.runsOf("q")).To(HaveLen(2))
		Expect(h.runsOf("q")[1].Sub(ran)).To(BeNumerically("<", interval))
	})

	It("counts missed runs in constant time", func() {
		h.hook = func(_ context.Context, _ planner.Job) { h.clk.Step(1000 * time.Hour) }
		h.s.Update(plan(job("a", "q", time.Second)))
		h.start()
		h.settle(1)
		h.clk.Step(time.Second)

		Eventually(h.skipList).WithTimeout(100 * time.Millisecond).Should(HaveLen(1))
		Expect(h.skipList()[0].missed).To(Equal(3_600_000))
	})

	It("counts the runs missed after a late start only", func() {
		interval := 10 * time.Second
		h.hook = func(_ context.Context, _ planner.Job) { h.clk.Step(25 * time.Second) }
		h.s.Update(plan(job("a", "q", interval)))
		h.start()
		h.settle(1)
		h.clk.Step(35 * time.Second)

		Eventually(h.skipList).Should(HaveLen(1))
		Expect(h.skipList()[0].missed).To(Equal(2))
	})

	It("starts no due job once the context has ended", func() {
		entered := make(chan struct{})
		release := make(chan struct{})
		h.hook = func(_ context.Context, j planner.Job) {
			if j.Query == "qa" {
				close(entered)
				<-release
			}
		}
		DeferCleanup(func() { closeOnce(release) })

		h.s.Update(plan(job("a", "qa", 10*time.Second)))
		h.start()
		h.settle(1)
		h.clk.Step(11 * time.Second)
		Eventually(entered).Should(BeClosed())

		h.cancel()
		// A one-nanosecond interval draws a due time of now.
		h.s.Update(plan(job("a", "qa", 10*time.Second), job("b", "qb", time.Nanosecond)))
		close(release)
		Eventually(h.done).Should(Receive(Succeed()))
		h.cancel = nil

		Expect(h.runsOf("qb")).To(BeEmpty())
		Expect(h.s.WorkerCount()).To(BeZero())
	})

	It("returns when the context ends, after the running job returns", func() {
		entered := make(chan struct{})
		sawCancel := make(chan struct{})
		release := make(chan struct{})
		h.hook = func(ctx context.Context, _ planner.Job) {
			close(entered)
			<-ctx.Done()
			close(sawCancel)
			<-release
		}
		DeferCleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})

		h.s.Update(plan(job("a", "q", 10*time.Second)))
		h.start()
		h.settle(1)
		h.clk.Step(11 * time.Second)
		Eventually(entered).Should(BeClosed())

		h.cancel()
		Eventually(sawCancel).Should(BeClosed())
		Consistently(h.done, 50*time.Millisecond).ShouldNot(Receive())

		close(release)
		Eventually(h.done).Should(Receive(Succeed()))
		h.cancel = nil
	})
})
