package policy_test

import (
	"encoding/json"
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
	workflowDir     = "../../.github/workflows"
	makefile        = "../../Makefile"
	repoRoot        = "../.."
	releaseConfig   = "release-please-config.json"
	releaseManifest = ".release-please-manifest.json"
	releaseWorkflow = "release.yml"
)

var (
	pinnedAction  = regexp.MustCompile(`^[\w.-]+/[\w./-]+@[0-9a-f]{40}$`)
	pinnedDocker  = regexp.MustCompile(`^docker://.+@sha256:[0-9a-f]{64}$`)
	secretsUse    = regexp.MustCompile(`\$\{\{[^}]*\bsecrets\b|secrets:\s*inherit`)
	privileged    = regexp.MustCompile(`pull_request_target|workflow_run`)
	pushPattern   = regexp.MustCompile(`docker\s+push|--push\b|push:\s*true|push=true|type=registry|packages:\s*write`)
	readOnlyScope = map[string]bool{"read": true, "none": true}
	semver        = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

	releaseWriteScopes = map[string]bool{"contents": true, "issues": true, "pull-requests": true, "packages": true}
)

type workflow struct {
	On          any            `yaml:"on"`
	Permissions any            `yaml:"permissions"`
	Jobs        map[string]job `yaml:"jobs"`
}

type job struct {
	If             string `yaml:"if"`
	Needs          any    `yaml:"needs"`
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

func disallowedScopes(v any, writable map[string]bool) []string {
	m, ok := v.(map[string]any)
	if !ok {
		return []string{fmt.Sprintf("not a mapping: %v", v)}
	}
	var bad []string
	for scope, level := range m {
		s, _ := level.(string)
		if !readOnlyScope[s] && (s != "write" || !writable[scope]) {
			bad = append(bad, fmt.Sprintf("%s: %v", scope, level))
		}
	}
	sort.Strings(bad)

	return bad
}

func writableScopes(name string) map[string]bool {
	if name == releaseWorkflow {
		return releaseWriteScopes
	}

	return nil
}

func findWorkflow(name string) (workflowFile, bool) {
	for _, wf := range workflows {
		if wf.name == name {
			return wf, true
		}
	}

	return workflowFile{}, false
}

func readJSON(name string, v any) error {
	raw, err := fs.ReadFile(os.DirFS(repoRoot), name)
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}

	return nil
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

	It("include ci.yml and release.yml", func() {
		var names []string
		for _, wf := range workflows {
			names = append(names, wf.name)
		}
		Expect(names).To(ContainElements("ci.yml", releaseWorkflow))
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

			It("grants write permissions only where allowed", func() {
				bad := map[string][]string{}
				for _, name := range sortedJobs(wf.doc) {
					if scopes := disallowedScopes(wf.doc.Jobs[name].Permissions, writableScopes(wf.name)); len(scopes) > 0 {
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

			if wf.name != releaseWorkflow {
				It("pushes nothing", func() {
					Expect(pushPattern.FindAllString(wf.raw, -1)).To(BeEmpty())
				})
			}
		})
	}
})

var _ = Describe("the release workflow", func() {
	It("grants release-please the permissions it documents", func() {
		wf, ok := findWorkflow(releaseWorkflow)
		Expect(ok).To(BeTrue())
		Expect(wf.doc.Jobs).To(HaveKey("release-please"))
		Expect(wf.doc.Jobs["release-please"].Permissions).To(Equal(map[string]any{
			"contents":      "write",
			"issues":        "write",
			"pull-requests": "write",
		}))
	})

	It("runs only on pushes to main", func() {
		wf, ok := findWorkflow(releaseWorkflow)
		Expect(ok).To(BeTrue())
		Expect(wf.doc.On).To(Equal(map[string]any{"push": map[string]any{"branches": []any{"main"}}}))
	})

	It("publishes only after release-please creates a release", func() {
		wf, ok := findWorkflow(releaseWorkflow)
		Expect(ok).To(BeTrue())
		var publishers []string
		for _, name := range sortedJobs(wf.doc) {
			j := wf.doc.Jobs[name]
			perms, _ := j.Permissions.(map[string]any)
			if perms["packages"] != "write" {
				continue
			}
			publishers = append(publishers, name)
			Expect(j.If).To(ContainSubstring("needs.release-please.outputs.release_created == 'true'"), name)
			Expect(j.Needs).To(Equal("release-please"), name)
		}
		Expect(publishers).NotTo(BeEmpty())
	})

	It("versions a Go module from 0.1.0 and keeps breaking changes on 0.x", func() {
		var cfg struct {
			Packages map[string]struct {
				ReleaseType       string `json:"release-type"`
				InitialVersion    string `json:"initial-version"`
				BumpMinorPreMajor bool   `json:"bump-minor-pre-major"`
			} `json:"packages"`
		}
		Expect(readJSON(releaseConfig, &cfg)).To(Succeed())
		Expect(cfg.Packages).To(HaveKey("."))
		Expect(cfg.Packages["."].ReleaseType).To(Equal("go"))
		Expect(cfg.Packages["."].InitialVersion).To(Equal("0.1.0"))
		Expect(cfg.Packages["."].BumpMinorPreMajor).To(BeTrue())
	})

	It("keeps the manifest empty until the first release, then semver", func() {
		var manifest map[string]string
		Expect(readJSON(releaseManifest, &manifest)).To(Succeed())
		if len(manifest) > 0 {
			Expect(manifest).To(HaveKeyWithValue(".", MatchRegexp(semver.String())))
			Expect(manifest["."]).NotTo(Equal("0.0.0"))
		}
	})
})

var _ = DescribeTable("the push pattern",
	func(text string, want bool) {
		Expect(pushPattern.MatchString(text)).To(Equal(want))
	},
	Entry("matches docker push", "docker push img", true),
	Entry("matches buildx --push", "docker buildx build --push .", true),
	Entry("matches push: true", "push: true", true),
	Entry("matches an output with push=true", "--output type=image,name=img,push=true", true),
	Entry("matches a registry output", "--output type=registry", true),
	Entry("ignores the push trigger", "on:\n  push:\n    branches: [main]", false),
)

var _ = DescribeTable("the secrets pattern",
	func(text string, want bool) {
		Expect(secretsUse.MatchString(text)).To(Equal(want))
	},
	Entry("matches property access", "${{ secrets.TOKEN }}", true),
	Entry("matches index access", "${{ secrets['TOKEN'] }}", true),
	Entry("matches the whole context", "${{ toJSON(secrets) }}", true),
	Entry("matches inherited secrets", "secrets: inherit", true),
	Entry("ignores other expressions", "${{ github.ref }}", false),
)

var _ = It("builds the image and loads it into kind in e2e.yml", func() {
	var runs []string
	for _, wf := range workflows {
		if wf.name != "e2e.yml" {
			continue
		}
		for _, name := range sortedJobs(wf.doc) {
			for _, s := range wf.doc.Jobs[name].Steps {
				runs = append(runs, s.Run)
			}
		}
	}
	Expect(runs).To(ContainElement(MatchRegexp(`\bmake\b.*\bimage\b.*\bkind-load\b`)))
})

var e2eSuites = regexp.MustCompile(`(?m)^E2E_SUITES\s*:?=\s*(.*)$`)

var _ = It("offers every Makefile e2e suite in e2e.yml", func() {
	raw, err := os.ReadFile(makefile)
	Expect(err).NotTo(HaveOccurred())
	match := e2eSuites.FindStringSubmatch(string(raw))
	Expect(match).NotTo(BeNil())

	wf, ok := findWorkflow("e2e.yml")
	Expect(ok).To(BeTrue())
	var doc struct {
		On struct {
			Dispatch struct {
				Inputs map[string]struct {
					Options []string `yaml:"options"`
				} `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
	}
	Expect(yaml.Unmarshal([]byte(wf.raw), &doc)).To(Succeed())

	Expect(doc.On.Dispatch.Inputs).To(HaveKey("suite"))
	Expect(doc.On.Dispatch.Inputs["suite"].Options).To(ConsistOf(strings.Fields(match[1])))
})
