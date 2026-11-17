package operator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

const testImage = "ghcr.io/gabrosys/plugin-telemetry@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeImages string

func (f fakeImages) Image() string { return string(f) }

const (
	podpatchData = "../podpatch/testdata"
	operatorData = "testdata"
)

// fixturePod reads an input shared with the podpatch goldens.
func fixturePod(name string) []byte { return fixture.Read(GinkgoTB(), podpatchData, name+".json") }

// matchGolden compares a Pod with a golden in internal/operator/testdata; UPDATE_GOLDEN=1 rewrites it.
func matchGolden(name string, pod []byte) {
	GinkgoHelper()

	if os.Getenv("UPDATE_GOLDEN") != "" {
		var out bytes.Buffer
		Expect(json.Indent(&out, canonical(pod), "", "  ")).To(Succeed())
		out.WriteByte('\n')

		Expect(fixture.Open(GinkgoTB(), operatorData).WriteFile(name+".json", out.Bytes(), 0o600)).To(Succeed())
	}

	Expect(canonical(pod)).To(MatchJSON(fixture.Read(GinkgoTB(), operatorData, name+".json")))
}

func initContainerNames(raw []byte) []string {
	GinkgoHelper()

	pod := &corev1.Pod{}
	Expect(json.Unmarshal(raw, pod)).To(Succeed())
	names := make([]string, 0, len(pod.Spec.InitContainers))
	for _, c := range pod.Spec.InitContainers {
		names = append(names, c.Name)
	}

	return names
}

func clusterJSON(plugins ...apiv1.PluginConfiguration) []byte {
	GinkgoHelper()

	c := apiv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: apiv1.SchemeGroupVersion.String(), Kind: apiv1.ClusterKind},
		ObjectMeta: metav1.ObjectMeta{Namespace: "fixture", Name: "fixture"},
		Spec:       apiv1.ClusterSpec{Plugins: plugins},
	}
	out, err := json.Marshal(c)
	Expect(err).NotTo(HaveOccurred())

	return out
}

func ours(params map[string]string) apiv1.PluginConfiguration {
	return apiv1.PluginConfiguration{Name: metadata.PluginName, Parameters: params}
}

func request(op lifecycle.OperatorOperationType_Type, cluster, object []byte) *lifecycle.OperatorLifecycleRequest {
	return &lifecycle.OperatorLifecycleRequest{
		OperationType:     &lifecycle.OperatorOperationType{Type: op},
		ClusterDefinition: cluster,
		ObjectDefinition:  object,
	}
}

func applyPatch(doc, patch []byte) []byte {
	GinkgoHelper()

	p, err := jsonpatch.DecodePatch(patch)
	Expect(err).NotTo(HaveOccurred())
	out, err := p.Apply(doc)
	Expect(err).NotTo(HaveOccurred())

	return out
}

func canonical(raw []byte) []byte {
	GinkgoHelper()

	pod := &corev1.Pod{}
	Expect(json.Unmarshal(raw, pod)).To(Succeed())
	out, err := json.Marshal(pod)
	Expect(err).NotTo(HaveOccurred())

	return out
}

var _ = Describe("Lifecycle", func() {
	var srv *operator.Lifecycle

	BeforeEach(func() {
		srv = &operator.Lifecycle{Images: fakeImages(testImage)}
	})

	It("hooks Pod creation and evaluation only", func(ctx context.Context) {
		resp, err := srv.GetCapabilities(ctx, &lifecycle.OperatorLifecycleCapabilitiesRequest{})
		Expect(err).NotTo(HaveOccurred())

		Expect(resp.GetLifecycleCapabilities()).To(HaveLen(1))
		capability := resp.GetLifecycleCapabilities()[0]
		Expect(capability.GetGroup()).To(BeEmpty())
		Expect(capability.GetKind()).To(Equal("Pod"))

		var types []lifecycle.OperatorOperationType_Type
		for _, t := range capability.GetOperationTypes() {
			types = append(types, t.GetType())
		}
		Expect(types).To(ConsistOf(
			lifecycle.OperatorOperationType_TYPE_CREATE,
			lifecycle.OperatorOperationType_TYPE_EVALUATE,
		))
	})

	for _, fixture := range []string{"pod-v1.30.1", "pod-main"} {
		Context("with the "+fixture+" Pod", func() {
			var pod []byte

			BeforeEach(func() { pod = fixturePod(fixture) })

			It("patches the Pod on EVALUATE", func(ctx context.Context) {
				resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
					clusterJSON(ours(nil)), pod))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetJsonPatch()).NotTo(BeEmpty())

				want := &corev1.Pod{}
				Expect(json.Unmarshal(pod, want)).To(Succeed())
				Expect(podpatch.Apply(want, "fixture", mustParams(nil), testImage)).To(Succeed())
				wantJSON, err := json.Marshal(want)
				Expect(err).NotTo(HaveOccurred())

				Expect(canonical(applyPatch(pod, resp.GetJsonPatch()))).To(MatchJSON(wantJSON))
			})

			It("updates the resources of an injected sidecar on EVALUATE", func(ctx context.Context) {
				resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
					clusterJSON(ours(nil)), pod))
				Expect(err).NotTo(HaveOccurred())
				injected := applyPatch(pod, resp.GetJsonPatch())

				resources := map[string]string{"sidecarCPU": "100m", "sidecarMemory": "128Mi"}
				resp, err = srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
					clusterJSON(ours(resources)), injected))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetJsonPatch()).NotTo(BeEmpty())

				want := &corev1.Pod{}
				Expect(json.Unmarshal(injected, want)).To(Succeed())
				Expect(podpatch.Apply(want, "fixture", mustParams(resources), testImage)).To(Succeed())
				wantJSON, err := json.Marshal(want)
				Expect(err).NotTo(HaveOccurred())
				patchedJSON := applyPatch(injected, resp.GetJsonPatch())
				Expect(canonical(patchedJSON)).To(MatchJSON(wantJSON))

				patched := &corev1.Pod{}
				Expect(json.Unmarshal(patchedJSON, patched)).To(Succeed())
				var sidecar corev1.Container
				found := false
				for _, c := range patched.Spec.InitContainers {
					if c.Name == metadata.SidecarContainerName {
						sidecar, found = c, true
					}
				}
				Expect(found).To(BeTrue(), "init container %q not found in the patched Pod", metadata.SidecarContainerName)
				for _, list := range []corev1.ResourceList{sidecar.Resources.Requests, sidecar.Resources.Limits} {
					cpu, memory := list[corev1.ResourceCPU], list[corev1.ResourceMemory]
					Expect(cpu.String()).To(Equal("100m"))
					Expect(memory.String()).To(Equal("128Mi"))
				}
			})

			It("changes nothing on CREATE after EVALUATE", func(ctx context.Context) {
				cluster := clusterJSON(ours(nil))
				resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, pod))
				Expect(err).NotTo(HaveOccurred())
				evaluated := applyPatch(pod, resp.GetJsonPatch())

				resp, err = srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_CREATE, cluster, evaluated))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetJsonPatch()).To(BeEmpty())
			})

			It("is idempotent: EVALUATE of the same Pod gives the same patched Pod, and of the patched Pod no patch", func(ctx context.Context) {
				cluster := clusterJSON(ours(nil))
				first, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, pod))
				Expect(err).NotTo(HaveOccurred())
				again, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, pod))
				Expect(err).NotTo(HaveOccurred())
				Expect(applyPatch(pod, again.GetJsonPatch())).To(MatchJSON(applyPatch(pod, first.GetJsonPatch())))

				second, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster,
					applyPatch(pod, first.GetJsonPatch())))
				Expect(err).NotTo(HaveOccurred())
				Expect(second.GetJsonPatch()).To(BeEmpty())
			})

			It("patches when the plugin is explicitly enabled", func(ctx context.Context) {
				resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
					clusterJSON(apiv1.PluginConfiguration{Name: metadata.PluginName, Enabled: ptr.To(true)}), pod))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetJsonPatch()).NotTo(BeEmpty())
			})

			DescribeTable("returns no patch",
				func(ctx context.Context, images string, cluster []byte) {
					srv := &operator.Lifecycle{Images: fakeImages(images)}
					resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, pod))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp).NotTo(BeNil())
					Expect(resp.GetJsonPatch()).To(BeEmpty())
				},
				Entry("for a Cluster without the plugin", testImage,
					clusterJSON(apiv1.PluginConfiguration{Name: "other.example.com"})),
				Entry("for a Cluster with no plugins", testImage, clusterJSON()),
				Entry("when the plugin is disabled", testImage,
					clusterJSON(apiv1.PluginConfiguration{Name: metadata.PluginName, Enabled: ptr.To(false)})),
				Entry("for an unsupported auth mode", testImage, clusterJSON(ours(map[string]string{"auth": "ldap"}))),
				Entry("when the store has no image", "", clusterJSON(ours(nil))),
			)
		})
	}

	It("logs the unknown keys of a Cluster with an invalid auth", func(ctx context.Context) {
		ctx, all := captureAll(ctx, 2)
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(map[string]string{"auth": "ldap", "atuh": "password"})), fixturePod("pod-main")))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonPatch()).To(BeEmpty())
		Expect(all()).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("msg", "ignoring unknown plugin parameters"),
			HaveKeyWithValue("keys", ConsistOf("atuh")),
		)))
	})

	It("returns no patch and no error without an image source", func(ctx context.Context) {
		resp, err := (&operator.Lifecycle{}).LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), fixturePod("pod-main")))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonPatch()).To(BeEmpty())
	})

	It("falls back to the default for an invalid sidecarMemory and still patches", func(ctx context.Context) {
		ctx, all := captureAll(ctx, 2)
		pod := fixturePod("pod-main")
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(map[string]string{"sidecarMemory": "16Mi"})), pod))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonPatch()).NotTo(BeEmpty())

		patched := &corev1.Pod{}
		Expect(json.Unmarshal(applyPatch(pod, resp.GetJsonPatch()), patched)).To(Succeed())
		var sidecar corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == metadata.SidecarContainerName {
				sidecar = c
			}
		}
		mem := sidecar.Resources.Limits[corev1.ResourceMemory]
		Expect(mem.String()).To(Equal("96Mi"))

		Expect(all()).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("msg", "invalid sidecar resource, using the default"),
			HaveKeyWithValue("namespace", "fixture"),
			HaveKeyWithValue("cluster", "fixture"),
			HaveKeyWithValue("error", ContainSubstring("sidecarMemory")),
		)))
		Expect(all()).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("msg", "lifecycle hook"),
			HaveKeyWithValue("outcome", "patched"),
		)))
	})

	It("keeps another plugin's sidecar and volume and adds ours after them", func(ctx context.Context) {
		pod := fixture.Read(GinkgoTB(), operatorData, "pod-main.other-plugin.json")
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), pod))
		Expect(err).NotTo(HaveOccurred())
		patched := applyPatch(pod, resp.GetJsonPatch())

		Expect(initContainerNames(patched)).To(Equal([]string{"bootstrap-controller", "other-plugin", metadata.SidecarContainerName}))
		Expect(string(patched)).To(ContainSubstring("other-plugin-data"))
		matchGolden("pod-main.other-plugin.peer.golden", patched)
	})

	It("adds the sidecar on CREATE without moving the bootstrap init containers", func(ctx context.Context) {
		pod := fixture.Read(GinkgoTB(), operatorData, "pod-main.create.json")
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_CREATE,
			clusterJSON(ours(nil)), pod))
		Expect(err).NotTo(HaveOccurred())
		patched := applyPatch(pod, resp.GetJsonPatch())

		Expect(initContainerNames(patched)).To(Equal([]string{"bootstrap-controller", "bootstrap-instance", metadata.SidecarContainerName}))
		matchGolden("pod-main.create.peer.golden", patched)
	})

	It("warns about an unknown plugin parameter and still patches", func(ctx context.Context) {
		ctx, all := captureAll(ctx, 2)
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(map[string]string{"sidecarCpu": "1"})), fixturePod("pod-main")))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonPatch()).NotTo(BeEmpty())

		Expect(all()).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("msg", "ignoring unknown plugin parameters"),
			HaveKeyWithValue("keys", ConsistOf("sidecarCpu")),
			HaveKeyWithValue("namespace", "fixture"),
			HaveKeyWithValue("cluster", "fixture"),
		)))
	})

	It("returns no patch for a Pod with no scratch volume in peer mode", func(ctx context.Context) {
		pod := &corev1.Pod{}
		Expect(json.Unmarshal(fixturePod("pod-main"), pod)).To(Succeed())
		pod.Spec.Volumes = nil
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())

		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), raw))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		Expect(resp.GetJsonPatch()).To(BeEmpty())
	})

	It("returns no patch for a Job", func(ctx context.Context) {
		job, err := json.Marshal(batchv1.Job{
			TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		})
		Expect(err).NotTo(HaveOccurred())

		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_CREATE,
			clusterJSON(ours(nil)), job))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		Expect(resp.GetJsonPatch()).To(BeEmpty())
	})

	It("rejects a malformed Cluster", func(ctx context.Context) {
		_, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			[]byte(`{"spec":`), fixturePod("pod-main")))
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("rejects a malformed object", func(ctx context.Context) {
		_, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), []byte(`{"kind":`)))
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})
})

// captureAll returns a context whose logger keeps the lines up to verbosity, and a
// function returning all lines decoded from JSON.
func captureAll(ctx context.Context, verbosity int) (context.Context, func() []map[string]any) {
	var lines []string
	sink := funcr.NewJSON(func(obj string) { lines = append(lines, obj) }, funcr.Options{Verbosity: verbosity})

	return logr.NewContext(ctx, sink), func() []map[string]any {
		GinkgoHelper()

		entries := make([]map[string]any, 0, len(lines))
		for _, line := range lines {
			var entry map[string]any
			Expect(json.Unmarshal([]byte(line), &entry)).To(Succeed())
			entries = append(entries, entry)
		}

		return entries
	}
}

// captureLogs is captureAll narrowed to the "lifecycle hook" lines.
func captureLogs(ctx context.Context) (context.Context, func() []map[string]any) {
	ctx, all := captureAll(ctx, 2)

	return ctx, func() []map[string]any {
		var hook []map[string]any
		for _, entry := range all() {
			if entry["msg"] == "lifecycle hook" {
				hook = append(hook, entry)
			}
		}

		return hook
	}
}

func hookLine(operation, outcome string) OmegaMatcher {
	return SatisfyAll(
		HaveKeyWithValue("operation", operation),
		HaveKeyWithValue("namespace", "fixture"),
		HaveKeyWithValue("cluster", "fixture"),
		HaveKeyWithValue("pod", "fixture-1"),
		HaveKeyWithValue("outcome", outcome),
		HaveKeyWithValue("level", BeEquivalentTo(2)),
		Not(HaveKey("error")),
	)
}

var _ = Describe("Lifecycle logging", func() {
	var srv *operator.Lifecycle

	BeforeEach(func() {
		srv = &operator.Lifecycle{Images: fakeImages(testImage)}
	})

	It("logs one line for an EVALUATE that patches", func(ctx context.Context) {
		ctx, hookLines := captureLogs(ctx)
		_, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), fixturePod("pod-main")))
		Expect(err).NotTo(HaveOccurred())

		Expect(hookLines()).To(HaveExactElements(hookLine("EVALUATE", "patched")))
	})

	It("logs one line for a CREATE that changes nothing", func(ctx context.Context) {
		cluster, pod := clusterJSON(ours(nil)), fixturePod("pod-main")
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, pod))
		Expect(err).NotTo(HaveOccurred())
		evaluated := applyPatch(pod, resp.GetJsonPatch())

		ctx, hookLines := captureLogs(ctx)
		resp, err = srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_CREATE, cluster, evaluated))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonPatch()).To(BeEmpty())

		Expect(hookLines()).To(HaveExactElements(hookLine("CREATE", "unchanged")))
	})

	DescribeTable("logs one skipped line",
		func(ctx context.Context, cluster []byte, object func() []byte) {
			ctx, hookLines := captureLogs(ctx)
			resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, object()))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.GetJsonPatch()).To(BeEmpty())

			Expect(hookLines()).To(HaveExactElements(hookLine("EVALUATE", "skipped")))
		},
		Entry("for a Cluster without the plugin", clusterJSON(), func() []byte { return fixturePod("pod-main") }),
		Entry("for an object that is not a Pod", clusterJSON(ours(nil)), func() []byte {
			job, err := json.Marshal(batchv1.Job{
				TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
				ObjectMeta: metav1.ObjectMeta{Namespace: "fixture", Name: "fixture-1"},
			})
			Expect(err).NotTo(HaveOccurred())

			return job
		}),
	)

	It("logs the hook line with outcome error when the sidecar can't be injected", func(ctx context.Context) {
		ctx, hookLines := captureLogs(ctx)
		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(map[string]string{"auth": "ldap"})), fixturePod("pod-main")))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetJsonPatch()).To(BeEmpty())

		Expect(hookLines()).To(HaveExactElements(hookLine("EVALUATE", "error")))
	})

	DescribeTable("logs an error before rejecting an undecodable request",
		func(ctx context.Context, cluster, object []byte, what string) {
			ctx, all := captureAll(ctx, 2)
			_, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, object))
			Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

			Expect(all()).To(ContainElement(SatisfyAll(
				HaveKey("error"),
				HaveKeyWithValue("msg", ContainSubstring(what)),
			)))
		},
		Entry("a malformed Cluster", []byte(`{"spec":`), []byte(`{"kind":"Pod"}`), "decode cluster"),
		Entry("a malformed object", clusterJSON(ours(nil)), []byte(`{"kind":`), "get object kind"),
		Entry("an object whose metadata does not decode", clusterJSON(ours(nil)),
			[]byte(`{"kind":"Pod","metadata":"x"}`), "decode object metadata"),
		Entry("a Pod that does not decode", clusterJSON(ours(nil)), []byte(`{"kind":"Pod","spec":"x"}`), "decode pod"),
	)

	It("logs the lifecycle hook line at debug level only", func(ctx context.Context) {
		hookRequest := func() *lifecycle.OperatorLifecycleRequest {
			return request(lifecycle.OperatorOperationType_TYPE_EVALUATE, clusterJSON(ours(nil)), fixturePod("pod-main"))
		}
		hookEntries := func(all []map[string]any) []map[string]any {
			var hook []map[string]any
			for _, entry := range all {
				if entry["msg"] == "lifecycle hook" {
					hook = append(hook, entry)
				}
			}

			return hook
		}

		debugCtx, debugLines := captureAll(ctx, 2)
		_, err := srv.LifecycleHook(debugCtx, hookRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(hookEntries(debugLines())).To(HaveExactElements(hookLine("EVALUATE", "patched")))

		infoCtx, infoLines := captureAll(ctx, 0)
		_, err = srv.LifecycleHook(infoCtx, hookRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(hookEntries(infoLines())).To(BeEmpty())
	})

	It("logs no hook line for a request it cannot decode", func(ctx context.Context) {
		ctx, hookLines := captureLogs(ctx)
		_, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), []byte(`{"kind":`)))
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		Expect(hookLines()).To(BeEmpty())
	})
})

func mustParams(raw map[string]string) podpatch.Params {
	GinkgoHelper()

	r, err := podpatch.ParseParams(raw)
	Expect(err).NotTo(HaveOccurred())

	return r.Params
}
