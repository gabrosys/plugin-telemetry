package memlimit_test

import (
	"errors"
	"fmt"
	"io/fs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/memlimit"
)

var _ = Describe("Set", func() {
	var applied []int64

	BeforeEach(func() {
		applied = nil
		DeferCleanup(memlimit.SetLimiter(func(n int64) int64 {
			applied = append(applied, n)
			return 0
		}))
	})

	file := func(content string) func(string) ([]byte, error) {
		return func(name string) ([]byte, error) {
			Expect(name).To(Equal("/sys/fs/cgroup/memory.max"))
			return []byte(content), nil
		}
	}
	noEnv := func(string) string { return "" }

	It("sets 80% of memory.max", func() {
		n, err := memlimit.Set(file("67108864\n"), noEnv)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(53687091)))
		Expect(applied).To(Equal([]int64{53687091}))
	})

	It("leaves the limit alone when memory.max is max", func() {
		n, err := memlimit.Set(file("max\n"), noEnv)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(BeZero())
		Expect(applied).To(BeEmpty())
	})

	It("leaves the limit alone when GOMEMLIMIT is set", func() {
		n, err := memlimit.Set(file("67108864"), func(k string) string {
			if k == "GOMEMLIMIT" {
				return "1GiB"
			}
			return ""
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(BeZero())
		Expect(applied).To(BeEmpty())
	})

	It("leaves the limit alone when the file is missing", func() {
		n, err := memlimit.Set(func(string) ([]byte, error) {
			return nil, fmt.Errorf("open: %w", fs.ErrNotExist)
		}, noEnv)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(BeZero())
		Expect(applied).To(BeEmpty())
	})

	It("leaves the limit alone when the file does not parse", func() {
		n, err := memlimit.Set(file("lots"), noEnv)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(BeZero())
		Expect(applied).To(BeEmpty())
	})

	It("reports a read error other than a missing file", func() {
		n, err := memlimit.Set(func(string) ([]byte, error) { return nil, fs.ErrPermission }, noEnv)
		Expect(errors.Is(err, fs.ErrPermission)).To(BeTrue())
		Expect(n).To(BeZero())
		Expect(applied).To(BeEmpty())
	})

	It("leaves the limit alone when 80% of memory.max rounds down to zero", func() {
		n, err := memlimit.Set(file("1\n"), noEnv)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(BeZero())
		Expect(applied).To(BeEmpty())
	})
})
