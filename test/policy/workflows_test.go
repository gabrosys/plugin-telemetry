package policy_test

import (
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.yaml.in/yaml/v4"
)

const (
	workflowDir = "../../.github/workflows"
	makefile    = "../../Makefile"
)

var (
	pinnedAction  = regexp.MustCompile(`^[\w.-]+/[\w./-]+@[0-9a-f]{40}$`)
	pinnedDocker  = regexp.MustCompile(`^docker://.+@sha256:[0-9a-f]{64}$`)
	secretsUse    = regexp.MustCompile(`secrets\.|secrets:\s*inherit`)
	privileged    = regexp.MustCompile(`pull_request_target|workflow_run`)
	pushPattern   = regexp.MustCompile(`docker\s+push|--push\b|push:\s*true|type=registry|packages:\s*write`)
	readOnlyScope = map[string]bool{"read": true, "none": true}
)

type workflow struct {
	Permissions any            `yaml:"permissions"`
	Jobs        map[string]job `yaml:"jobs"`
}

type job struct {
	Permissions    any    `yaml:"permissions"`
	TimeoutMinutes int    `yaml:"timeout-minutes"`
	Uses           string `yaml:"uses"`
	Steps          []step `yaml:"steps"`
}

type step struct {
	Uses string         `yaml:"uses"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
}

type workflowFile struct {
	name string
	raw  string
	doc  workflow
}

func loadWorkflows() ([]workflowFile, error) {
	fsys := os.DirFS(workflowDir)
	var names []string
	for _, pattern := range []string{"*.yml", "*.yaml"} {
		matches, err := fs.Glob(fsys, pattern)
		if err != nil {
			return nil, fmt.Errorf("glob %s: %w", pattern, err)
		}
		names = append(names, matches...)
	}
	sort.Strings(names)

	files := make([]workflowFile, 0, len(names))
	for _, name := range names {
		raw, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		var doc workflow
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		files = append(files, workflowFile{name: name, raw: string(raw), doc: doc})
	}

	return files, nil
}

func isPinned(uses string) bool {
	return strings.HasPrefix(uses, "./") || pinnedAction.MatchString(uses) || pinnedDocker.MatchString(uses)
}

func isEmptyMapping(v any) bool {
	m, ok := v.(map[string]any)

	return ok && len(m) == 0
}

func nonReadScopes(v any) []string {
	m, ok := v.(map[string]any)
	if !ok {
		return []string{fmt.Sprintf("not a mapping: %v", v)}
	}
	var bad []string
	for scope, level := range m {
		if s, _ := level.(string); !readOnlyScope[s] {
			bad = append(bad, fmt.Sprintf("%s: %v", scope, level))
		}
	}
	sort.Strings(bad)

	return bad
}

func sortedJobs(wf workflow) []string {
	names := make([]string, 0, len(wf.Jobs))
	for name := range wf.Jobs {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

var workflows, loadErr = loadWorkflows()

var _ = Describe("CI workflows", func() {
	It("load without errors", func() {
		Expect(loadErr).NotTo(HaveOccurred())
	})

	It("include ci.yml", func() {
		var names []string
		for _, wf := range workflows {
			names = append(names, wf.name)
		}
		Expect(names).To(ContainElement("ci.yml"))
	})

	It("are not pushed from the Makefile", func() {
		raw, err := os.ReadFile(makefile)
		Expect(err).NotTo(HaveOccurred())
		Expect(pushPattern.FindAllString(string(raw), -1)).To(BeEmpty())
	})

	for _, wf := range workflows {
		Describe(wf.name, func() {
			It("pins every action to a commit SHA", func() {
				var unpinned []string
				for _, name := range sortedJobs(wf.doc) {
					j := wf.doc.Jobs[name]
					if j.Uses != "" && !isPinned(j.Uses) {
						unpinned = append(unpinned, name+": "+j.Uses)
					}
					for _, s := range j.Steps {
						if s.Uses != "" && !isPinned(s.Uses) {
							unpinned = append(unpinned, name+": "+s.Uses)
						}
					}
				}
				Expect(unpinned).To(BeEmpty())
			})

			It("declares empty top-level permissions", func() {
				Expect(isEmptyMapping(wf.doc.Permissions)).To(BeTrue())
			})

			It("grants each job read-only permissions", func() {
				bad := map[string][]string{}
				for _, name := range sortedJobs(wf.doc) {
					if scopes := nonReadScopes(wf.doc.Jobs[name].Permissions); len(scopes) > 0 {
						bad[name] = scopes
					}
				}
				Expect(bad).To(BeEmpty())
			})

			It("sets a timeout on every job", func() {
				var missing []string
				for _, name := range sortedJobs(wf.doc) {
					if wf.doc.Jobs[name].TimeoutMinutes <= 0 {
						missing = append(missing, name)
					}
				}
				Expect(missing).To(BeEmpty())
			})

			It("uses no secrets", func() {
				Expect(secretsUse.FindAllString(wf.raw, -1)).To(BeEmpty())
			})

			It("uses no privileged triggers", func() {
				Expect(privileged.FindAllString(wf.raw, -1)).To(BeEmpty())
			})

			It("does not persist checkout credentials", func() {
				var persisting []string
				for _, name := range sortedJobs(wf.doc) {
					for _, s := range wf.doc.Jobs[name].Steps {
						if strings.HasPrefix(s.Uses, "actions/checkout@") && s.With["persist-credentials"] != false {
							persisting = append(persisting, name)
						}
					}
				}
				Expect(persisting).To(BeEmpty())
			})

			It("keeps expressions out of run scripts", func() {
				var injected []string
				for _, name := range sortedJobs(wf.doc) {
					for _, s := range wf.doc.Jobs[name].Steps {
						if strings.Contains(s.Run, "${{") {
							injected = append(injected, name+": "+s.Run)
						}
					}
				}
				Expect(injected).To(BeEmpty())
			})

			It("pushes nothing", func() {
				Expect(pushPattern.FindAllString(wf.raw, -1)).To(BeEmpty())
			})
		})
	}
})
