package operator_test

import (
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
)

const testImage = "ghcr.io/gabrosys/plugin-telemetry@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeImages string

func (f fakeImages) Image() string { return string(f) }

func fixturePod(name string) []byte {
	GinkgoHelper()

	root, err := os.OpenRoot("../podpatch/testdata")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)

	data, err := root.ReadFile(name + ".json")
	Expect(err).NotTo(HaveOccurred())

	return data
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

			It("changes nothing on CREATE after EVALUATE", func(ctx context.Context) {
				cluster := clusterJSON(ours(nil))
				resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, pod))
				Expect(err).NotTo(HaveOccurred())
				evaluated := applyPatch(pod, resp.GetJsonPatch())

				resp, err = srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_CREATE, cluster, evaluated))
				Expect(err).NotTo(HaveOccurred())

				if len(resp.GetJsonPatch()) == 0 {
					return
				}
				Expect(applyPatch(evaluated, resp.GetJsonPatch())).To(Equal(evaluated))
			})

			for name, c := range map[string]struct {
				images  string
				cluster func() []byte
			}{
				"for a Cluster without the plugin": {testImage, func() []byte {
					return clusterJSON(apiv1.PluginConfiguration{Name: "other.example.com"})
				}},
				"for a Cluster with no plugins": {testImage, func() []byte { return clusterJSON() }},
				"when the plugin is disabled": {testImage, func() []byte {
					return clusterJSON(apiv1.PluginConfiguration{Name: metadata.PluginName, Enabled: ptr.To(false)})
				}},
				"for an unsupported auth mode": {testImage, func() []byte {
					return clusterJSON(ours(map[string]string{"auth": "ldap"}))
				}},
				"when the store has no image": {"", func() []byte { return clusterJSON(ours(nil)) }},
			} {
				It("returns no patch "+name, func(ctx context.Context) {
					srv := &operator.Lifecycle{Images: fakeImages(c.images)}
					resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, c.cluster(), pod))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp).NotTo(BeNil())
					Expect(resp.GetJsonPatch()).To(BeEmpty())
				})
			}
		})
	}

	It("falls back to the default for an invalid sidecarMemory and still patches", func(ctx context.Context) {
		ctx, all := captureAll(ctx)
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
		Expect(mem.String()).To(Equal("64Mi"))

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

	It("returns no patch for a Pod with no scratch volume in peer mode", func(ctx context.Context) {
		pod := &corev1.Pod{}
		Expect(json.Unmarshal(fixturePod("pod-main"), pod)).To(Succeed())
		pod.Spec.Volumes = nil
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())

		resp, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE,
			clusterJSON(ours(nil)), raw))
		Expect(err).NotTo(HaveOccurred())
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

// captureAll returns a context whose logger keeps every line, and a
// function returning all lines decoded from JSON.
func captureAll(ctx context.Context) (context.Context, func() []map[string]any) {
	var lines []string
	sink := funcr.NewJSON(func(obj string) { lines = append(lines, obj) }, funcr.Options{})

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
	ctx, all := captureAll(ctx)

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
		HaveKeyWithValue("level", BeEquivalentTo(0)),
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

			Expect(hookLines()).To(HaveExactElements(SatisfyAll(
				HaveKeyWithValue("operation", "EVALUATE"),
				HaveKeyWithValue("outcome", "skipped"),
				HaveKeyWithValue("level", BeEquivalentTo(0)),
			)))
		},
		Entry("for a Cluster without the plugin", clusterJSON(), func() []byte { return fixturePod("pod-main") }),
		Entry("for invalid plugin parameters", clusterJSON(ours(map[string]string{"auth": "ldap"})),
			func() []byte { return fixturePod("pod-main") }),
		Entry("for an object that is not a Pod", clusterJSON(ours(nil)), func() []byte {
			job, err := json.Marshal(batchv1.Job{
				TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
				ObjectMeta: metav1.ObjectMeta{Namespace: "fixture", Name: "fixture-1"},
			})
			Expect(err).NotTo(HaveOccurred())

			return job
		}),
	)

	DescribeTable("logs an error before rejecting an undecodable request",
		func(ctx context.Context, cluster, object []byte, what string) {
			ctx, all := captureAll(ctx)
			_, err := srv.LifecycleHook(ctx, request(lifecycle.OperatorOperationType_TYPE_EVALUATE, cluster, object))
			Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

			Expect(all()).To(ContainElement(SatisfyAll(
				HaveKey("error"),
				HaveKeyWithValue("msg", ContainSubstring(what)),
			)))
		},
		Entry("a malformed Cluster", []byte(`{"spec":`), []byte(`{"kind":"Pod"}`), "decode cluster"),
		Entry("a malformed object", clusterJSON(ours(nil)), []byte(`{"kind":`), "get object kind"),
		Entry("a Pod that does not decode", clusterJSON(ours(nil)), []byte(`{"kind":"Pod","spec":"x"}`), "decode pod"),
	)

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
