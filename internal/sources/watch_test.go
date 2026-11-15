package sources_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

var _ = Describe("Watch", func() {
	var (
		dir    string
		file   string
		clk    *clocktesting.FakeClock
		count  atomic.Int32
		list   atomic.Pointer[[]string]
		errs   chan error
		cancel context.CancelFunc
		done   chan error

		mu    sync.Mutex // guards lines
		lines []string
	)

	setFiles := func(files ...string) { list.Store(&files) }
	files := func() []string { return *list.Load() }

	logged := func(text string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, l := range lines {
			if strings.Contains(l, text) {
				n++
			}
		}
		return n
	}

	// startWith runs watch and returns once it has notified that it is watching.
	startWith := func(watch func(ctx context.Context, notify func()) error, fs ...string) {
		GinkgoHelper()
		setFiles(fs...)
		logger := funcr.New(func(_, args string) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, args)
		}, funcr.Options{})
		var ctx context.Context
		ctx, cancel = context.WithCancel(ctrllog.IntoContext(context.Background(), logger))
		go func() {
			done <- watch(ctx, func() { count.Add(1) })
		}()
		Eventually(count.Load, 5*time.Second).Should(Equal(int32(1)), "notifies once watching")
	}

	// start runs Watch on the fake clock.
	start := func(fs ...string) {
		GinkgoHelper()
		startWith(func(ctx context.Context, notify func()) error { return sources.WatchWith(ctx, clk, files, notify) }, fs...)
	}

	// armed reports whether the debounce timer is pending beside the retry timer.
	armed := func() bool { return clk.Waiters() == 2 }

	// fire waits for a change to arm the debounce timer and lets it fire.
	fire := func() {
		GinkgoHelper()
		Eventually(armed, 5*time.Second).Should(BeTrue())
		clk.Step(sources.Debounce)
	}

	expect := func(n int32) {
		GinkgoHelper()
		Eventually(count.Load, 5*time.Second).Should(Equal(n))
		Consistently(count.Load, 200*time.Millisecond).Should(Equal(n))
	}

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		file = filepath.Join(dir, "queries.yaml")
		write(file, "a:\n  query: SELECT 1\n")
		clk = clocktesting.NewFakeClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
		count.Store(0)
		errs = make(chan error)
		done = make(chan error, 1)
		mu.Lock()
		lines = nil
		mu.Unlock()
	})

	AfterEach(func() {
		cancel()
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))
	})

	It("notifies once watching and not again without a change", func() {
		start(file)
		expect(1)
		clk.Step(sources.WatchRetry)
		expect(1)
	})

	It("notifies once after a write", func() {
		start(file)
		write(file, "a:\n  query: SELECT 2\n")
		fire()
		expect(2)
	})

	It("notifies once after an editor-style rename over the file", func() {
		start(file)
		tmp := filepath.Join(dir, ".queries.yaml.swp")
		write(tmp, "a:\n  query: SELECT 2\n")
		Expect(os.Rename(tmp, file)).To(Succeed())
		fire()
		expect(2)
	})

	It("notifies once after a kubernetes-style ..data symlink swap", func() {
		Expect(os.Remove(file)).To(Succeed())
		v1 := filepath.Join(dir, "..2026_1")
		v2 := filepath.Join(dir, "..2026_2")
		Expect(os.Mkdir(v1, 0o700)).To(Succeed())
		Expect(os.Mkdir(v2, 0o700)).To(Succeed())
		write(filepath.Join(v1, "queries.yaml"), "a:\n  query: SELECT 1\n")
		write(filepath.Join(v2, "queries.yaml"), "a:\n  query: SELECT 2\n")
		Expect(os.Symlink("..2026_1", filepath.Join(dir, "..data"))).To(Succeed())
		Expect(os.Symlink(filepath.Join("..data", "queries.yaml"), file)).To(Succeed())

		start(file)

		Expect(os.Symlink("..2026_2", filepath.Join(dir, "..data_tmp"))).To(Succeed())
		Expect(os.Rename(filepath.Join(dir, "..data_tmp"), filepath.Join(dir, "..data"))).To(Succeed())
		Expect(os.RemoveAll(v1)).To(Succeed())
		fire()
		expect(2)
	})

	It("notifies once for ten writes inside the debounce window", func() {
		start(file)
		for i := range 10 {
			write(file, "a:\n  query: SELECT "+string(rune('0'+i))+"\n")
		}
		Eventually(armed, 5*time.Second).Should(BeTrue())
		time.Sleep(100 * time.Millisecond) // lets the watcher read the remaining events
		clk.Step(sources.Debounce)
		expect(2)
	})

	It("notifies once after files() adds a directory, then watches it", func() {
		other := filepath.Join(GinkgoT().TempDir(), "headers")
		write(other, "a=1")
		start(file)

		setFiles(file, other)
		clk.Step(sources.WatchRetry)
		expect(2)

		write(other, "a=2")
		fire()
		expect(3)
	})

	It("stops watching a directory files() no longer lists", func() {
		other := filepath.Join(GinkgoT().TempDir(), "headers")
		write(other, "a=1")
		start(file, other)

		setFiles(file)
		clk.Step(sources.WatchRetry)
		Eventually(clk.Waiters, 5*time.Second).Should(Equal(1))
		write(other, "a=2")
		Consistently(armed, 300*time.Millisecond).Should(BeFalse())
		expect(1)
	})

	It("retries a missing directory every 5 s, logging it and its recovery once", func() {
		missing := filepath.Join(dir, "later")
		start(file, filepath.Join(missing, "q.yaml"))
		Expect(logged("watch the configuration files")).To(Equal(1))

		for range 3 {
			clk.Step(sources.WatchRetry)
			Eventually(clk.Waiters, 5*time.Second).Should(Equal(1))
		}
		Expect(logged("watch the configuration files")).To(Equal(1))
		expect(1)

		Expect(os.Mkdir(missing, 0o700)).To(Succeed())
		clk.Step(sources.WatchRetry)
		expect(2)
		Expect(logged("watching the configuration files again")).To(Equal(1))

		write(filepath.Join(missing, "q.yaml"), "b:\n  query: SELECT 1\n")
		fire()
		expect(3)
		Expect(logged("watch the configuration files")).To(Equal(1))
		Expect(logged("watching the configuration files again")).To(Equal(1))
	})

	It("logs an fsnotify error once and notifies", func() {
		startWith(func(ctx context.Context, notify func()) error {
			return sources.WatchWithErrors(ctx, clk, files, notify, errs)
		}, file)
		errs <- fsnotify.ErrEventOverflow
		expect(2)
		errs <- fsnotify.ErrEventOverflow
		expect(3)
		Expect(logged("overflow")).To(Equal(1))
	})
})
