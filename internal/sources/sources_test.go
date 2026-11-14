package sources_test

import (
	"fmt"
	"io/fs"
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
		r := sources.NewLoader(os.ReadFile, nil).Load()

		Expect(keys(r)).To(Equal([]string{"collector"}))
		Expect(r.Status).To(HaveLen(1))
		Expect(r.Status[0].Source).To(Equal("builtin"))
		Expect(r.Status[0].Outcome).To(Equal("ok"))
		Expect(entry(r, "collector").Source).To(Equal("builtin"))
	})

	It("lets later sources overwrite earlier keys and reports the duplicates", func() {
		write(f1, "a:\n  query: SELECT 1\nb:\n  query: SELECT 1\ncollector:\n  query: SELECT 0\n")
		write(f2, "b:\n  query: SELECT 2\nc:\n  query: SELECT 3\na:\n  query: SELECT 2\n")

		r := sources.NewLoader(os.ReadFile, []string{f1, f2}).Load()

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

		r := sources.NewLoader(os.ReadFile, []string{f1, f2}).Load()

		Expect(r.Status[1].Outcome).To(Equal("missing"))
		Expect(r.Status[1].Err).To(HaveOccurred())
		Expect(r.Status[2].Outcome).To(Equal("ok"))
		Expect(keys(r)).To(Equal([]string{"b", "collector"}))
	})

	It("skips a file that is invalid from the start", func() {
		write(f1, "a: [not, a, map")

		r := sources.NewLoader(os.ReadFile, []string{f1}).Load()

		Expect(r.Status[1].Outcome).To(Equal("invalid"))
		Expect(r.Status[1].Err).To(HaveOccurred())
		Expect(r.Status[1].UsingPrevious).To(BeFalse())
		Expect(keys(r)).To(Equal([]string{"collector"}))
	})

	It("keeps the last good version of a file that stops parsing", func() {
		write(f1, "a:\n  query: SELECT 1\n")
		l := sources.NewLoader(os.ReadFile, []string{f1})
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
		l := sources.NewLoader(os.ReadFile, []string{f1})
		first := l.Load()
		Expect(first.Hash).To(MatchRegexp("^[0-9a-f]{64}$"))

		write(f1, "# a comment\na:\n  query: SELECT 1\n")
		Expect(l.Load().Hash).To(Equal(first.Hash))

		write(f1, "a:\n  query: SELECT 2\n")
		Expect(l.Load().Hash).NotTo(Equal(first.Hash))
	})

	It("marks a denied source OutcomeDenied in its status and keeps its last good version", func() {
		denied := false
		read := func(name string) ([]byte, error) {
			if denied {
				return nil, fmt.Errorf("get %s: %w", name, sources.ErrDenied)
			}
			return []byte("a:\n  query: SELECT 1\n"), nil
		}
		l := sources.NewLoader(read, []string{"mem/one.yaml"})
		good := l.Load()
		Expect(good.Status[1].Outcome).To(Equal(sources.OutcomeOK))

		denied = true
		r := l.Load()
		Expect(r.Status[1].Outcome).To(Equal(sources.OutcomeDenied))
		Expect(r.Status[1].Outcome).To(Equal("denied"))
		Expect(r.Status[1].Err).To(MatchError(sources.ErrDenied))
		Expect(r.Status[1].UsingPrevious).To(BeTrue())
		Expect(keys(r)).To(Equal([]string{"a", "collector"}))
		Expect(r.Hash).To(Equal(good.Hash))
	})

	It("skips a denied source that never loaded", func() {
		read := func(name string) ([]byte, error) { return nil, fmt.Errorf("get %s: %w", name, sources.ErrDenied) }
		r := sources.NewLoader(read, []string{"mem/one.yaml"}).Load()
		Expect(r.Status[1].Outcome).To(Equal(sources.OutcomeDenied))
		Expect(r.Status[1].UsingPrevious).To(BeFalse())
		Expect(keys(r)).To(Equal([]string{"collector"}))
	})

	It("reads through the reader it is given", func() {
		var read []string
		files := map[string]string{"mem/one.yaml": "a:\n  query: SELECT 1\n"}
		l := sources.NewLoader(func(name string) ([]byte, error) {
			read = append(read, name)
			data, ok := files[name]
			if !ok {
				return nil, fmt.Errorf("read %s: %w", name, fs.ErrNotExist)
			}
			return []byte(data), nil
		}, []string{"mem/one.yaml", "mem/two.yaml"})

		r := l.Load()
		Expect(read).To(Equal([]string{"mem/one.yaml", "mem/two.yaml"}))
		Expect(r.Status[2].Outcome).To(Equal(sources.OutcomeMissing))
		Expect(keys(r)).To(Equal([]string{"a", "collector"}))
	})
})
