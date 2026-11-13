package sources_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

const debounce = 200 * time.Millisecond

var _ = Describe("Watch", func() {
	var (
		dir    string
		file   string
		count  atomic.Int32
		cancel context.CancelFunc
		done   chan error
	)

	start := func(files ...string) {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		done = make(chan error, 1)
		count.Store(0)
		ready := make(chan struct{})
		go func() {
			done <- sources.Watch(ctx, files, debounce, func() { count.Add(1) },
				sources.WithReady(func() { close(ready) }))
		}()
		Eventually(ready).Should(BeClosed())
	}

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		file = filepath.Join(dir, "queries.yaml")
		write(file, "a:\n  query: SELECT 1\n")
	})

	AfterEach(func() {
		cancel()
		Eventually(done).Should(Receive(BeNil()))
	})

	expectOne := func() {
		GinkgoHelper()
		Eventually(count.Load, debounce+time.Second).Should(Equal(int32(1)))
		Consistently(count.Load, 2*debounce).Should(Equal(int32(1)))
	}

	It("notifies once after a write", func() {
		start(file)
		write(file, "a:\n  query: SELECT 2\n")
		expectOne()
	})

	It("notifies once after an editor-style rename over the file", func() {
		start(file)
		tmp := filepath.Join(dir, ".queries.yaml.swp")
		write(tmp, "a:\n  query: SELECT 2\n")
		Expect(os.Rename(tmp, file)).To(Succeed())
		expectOne()
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
		expectOne()
	})

	It("notifies once for ten writes inside the debounce window", func() {
		start(file)
		for i := range 10 {
			write(file, "a:\n  query: SELECT "+string(rune('0'+i))+"\n")
			time.Sleep(5 * time.Millisecond)
		}
		expectOne()
	})

	It("notifies for a write made right after ready fires", func() {
		ctx, c := context.WithCancel(context.Background())
		cancel = c
		done = make(chan error, 1)
		count.Store(0)
		ready := make(chan struct{})
		go func() {
			done <- sources.Watch(ctx, []string{file}, debounce, func() { count.Add(1) },
				sources.WithReady(func() { close(ready) }))
		}()
		<-ready
		write(file, "a:\n  query: SELECT 9\n")
		expectOne()
	})

	It("returns an error when a directory can't be watched", func() {
		cancel = func() {}
		done = make(chan error, 1)
		done <- nil
		err := sources.Watch(context.Background(), []string{filepath.Join(dir, "nope", "q.yaml")}, debounce, func() {})
		Expect(err).To(HaveOccurred())
	})
})
