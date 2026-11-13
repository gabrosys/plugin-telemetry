package sources_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

func write(path, content string) {
	GinkgoHelper()
	Expect(os.WriteFile(path, []byte(content), 0o600)).To(Succeed())
}

func keys(r sources.Result) []string {
	out := make([]string, 0, len(r.Entries))
	for _, e := range r.Entries {
		out = append(out, e.Key)
	}

	return out
}

func entry(r sources.Result, key string) sources.Entry {
	GinkgoHelper()
	for _, e := range r.Entries {
		if e.Key == key {
			return e
		}
	}
	Fail("no entry " + key)

	return sources.Entry{}
}

var _ = Describe("Loader", func() {
	var dir, f1, f2 string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		f1 = filepath.Join(dir, "one.yaml")
		f2 = filepath.Join(dir, "two.yaml")
	})

	It("loads the builtin query first and always ok", func() {
		r := sources.NewLoader(nil).Load()

		Expect(keys(r)).To(Equal([]string{"collector"}))
		Expect(r.Status).To(HaveLen(1))
		Expect(r.Status[0].Source).To(Equal("builtin"))
		Expect(r.Status[0].Outcome).To(Equal("ok"))
		Expect(entry(r, "collector").Source).To(Equal("builtin"))
	})

	It("lets later sources overwrite earlier keys and reports the duplicates", func() {
		write(f1, "a:\n  query: SELECT 1\nb:\n  query: SELECT 1\ncollector:\n  query: SELECT 0\n")
		write(f2, "b:\n  query: SELECT 2\nc:\n  query: SELECT 3\na:\n  query: SELECT 2\n")

		r := sources.NewLoader([]string{f1, f2}).Load()

		Expect(keys(r)).To(Equal([]string{"a", "b", "c", "collector"}))
		Expect(entry(r, "a").Query.Query).To(Equal("SELECT 2"))
		Expect(entry(r, "a").Source).To(Equal(f2))
		Expect(entry(r, "collector").Source).To(Equal(f1))
		Expect(r.Status).To(HaveLen(3))
		Expect(r.Status[0].Duplicates).To(BeEmpty())
		Expect(r.Status[1].Source).To(Equal(f1))
		Expect(r.Status[1].Duplicates).To(Equal([]string{"collector"}))
		Expect(r.Status[2].Duplicates).To(Equal([]string{"a", "b"}))
	})

	It("skips a missing file", func() {
		write(f2, "b:\n  query: SELECT 2\n")

		r := sources.NewLoader([]string{f1, f2}).Load()

		Expect(r.Status[1].Outcome).To(Equal("missing"))
		Expect(r.Status[1].Err).To(HaveOccurred())
		Expect(r.Status[2].Outcome).To(Equal("ok"))
		Expect(keys(r)).To(Equal([]string{"b", "collector"}))
	})

	It("skips a file that is invalid from the start", func() {
		write(f1, "a: [not, a, map")

		r := sources.NewLoader([]string{f1}).Load()

		Expect(r.Status[1].Outcome).To(Equal("invalid"))
		Expect(r.Status[1].Err).To(HaveOccurred())
		Expect(r.Status[1].UsingPrevious).To(BeFalse())
		Expect(keys(r)).To(Equal([]string{"collector"}))
	})

	It("keeps the last good version of a file that stops parsing", func() {
		write(f1, "a:\n  query: SELECT 1\n")
		l := sources.NewLoader([]string{f1})
		good := l.Load()

		write(f1, "a: [not, a, map")
		r := l.Load()

		Expect(r.Status[1].Outcome).To(Equal("invalid"))
		Expect(r.Status[1].UsingPrevious).To(BeTrue())
		Expect(keys(r)).To(Equal([]string{"a", "collector"}))
		Expect(r.Hash).To(Equal(good.Hash))

		write(f1, "a:\n  query: SELECT 5\n")
		r = l.Load()
		Expect(r.Status[1].Outcome).To(Equal("ok"))
		Expect(r.Status[1].UsingPrevious).To(BeFalse())
		Expect(entry(r, "a").Query.Query).To(Equal("SELECT 5"))
	})

	It("changes the hash only when entries change", func() {
		write(f1, "a:\n  query: SELECT 1\n")
		l := sources.NewLoader([]string{f1})
		first := l.Load()
		Expect(first.Hash).To(MatchRegexp("^[0-9a-f]{64}$"))

		write(f1, "# a comment\na:\n  query: SELECT 1\n")
		Expect(l.Load().Hash).To(Equal(first.Hash))

		write(f1, "a:\n  query: SELECT 2\n")
		Expect(l.Load().Hash).NotTo(Equal(first.Hash))
	})
})
