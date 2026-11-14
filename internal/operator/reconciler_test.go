package operator_test

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

var errApply = errors.New("apply refused")

func newClient(objs []client.Object, funcs interceptor.Funcs) client.Client {
	GinkgoHelper()

	scheme, err := operator.NewScheme()
	Expect(err).NotTo(HaveOccurred())

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
}

func newCluster(namespace, name string, plugins ...apiv1.PluginConfiguration) *apiv1.Cluster {
	return &apiv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: apiv1.SchemeGroupVersion.String(), Kind: apiv1.ClusterKind},
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(name + "-uid"), Generation: 1},
		Spec:       apiv1.ClusterSpec{Plugins: plugins},
	}
}

func disabled(params map[string]string) apiv1.PluginConfiguration {
	p := ours(params)
	p.Enabled = ptr.To(false)

	return p
}

func newExporter(namespace, name string, querySets ...string) *v1alpha1.TelemetryExporter {
	e := &v1alpha1.TelemetryExporter{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	e.Spec.Metrics.QuerySets = querySets

	return e
}

func hookRequest(c *apiv1.Cluster) *reconciler.ReconcilerHooksRequest {
	GinkgoHelper()

	raw, err := json.Marshal(c)
	Expect(err).NotTo(HaveOccurred())

	return &reconciler.ReconcilerHooksRequest{ClusterDefinition: raw, ResourceDefinition: raw}
}

// identityExists reports whether each identity object of a Cluster in namespace "ns" is present.
func identityExists(ctx context.Context, c client.Reader, cluster string) [4]bool {
	GinkgoHelper()

	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: "ns", Name: name} }
	account, token := metadata.ServiceAccountName(cluster), metadata.TokenSecretName(cluster)
	var found [4]bool
	for i, probe := range []struct {
		name string
		obj  client.Object
	}{
		{account, &corev1.ServiceAccount{}}, {account, &rbacv1.Role{}}, {account, &rbacv1.RoleBinding{}}, {token, &corev1.Secret{}},
	} {
		err := c.Get(ctx, key(probe.name), probe.obj)
		if err != nil {
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), err.Error())
		}
		found[i] = err == nil
	}

	return found
}

var allFour = [4]bool{true, true, true, true}

var _ = Describe("Reconciler", func() {
	var (
		c   client.Client
		srv *operator.Reconciler
	)

	build := func(funcs interceptor.Funcs, objs ...client.Object) {
		c = newClient(objs, funcs)
		srv = &operator.Reconciler{
			Identity:  &identity.Ensurer{Reader: c, Writer: c},
			Exporters: c,
			Timeout:   time.Second,
		}
	}

	BeforeEach(func() { build(interceptor.Funcs{}) })

	hookLine := func(entries []map[string]any, operation string) map[string]any {
		GinkgoHelper()

		var lines []map[string]any
		for _, e := range entries {
			if e["msg"] == "reconciler hook" && e["operation"] == operation {
				lines = append(lines, e)
			}
		}
		Expect(lines).To(HaveLen(1))

		return lines[0]
	}

	It("Pre ensures the identity of an enabled Cluster and returns CONTINUE", func(ctx context.Context) {
		ctx, all := captureAll(ctx)
		cluster := newCluster("ns", "pg", ours(nil))

		res, err := srv.Pre(ctx, hookRequest(cluster))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE))
		Expect(identityExists(ctx, c, "pg")).To(Equal(allFour))

		line := hookLine(all(), "Pre")
		Expect(line).To(SatisfyAll(
			HaveKeyWithValue("namespace", "ns"),
			HaveKeyWithValue("cluster", "pg"),
			HaveKeyWithValue("outcome", "applied"),
		))

		_, err = srv.Pre(ctx, hookRequest(cluster))
		Expect(err).NotTo(HaveOccurred())
		var outcomes []any
		for _, e := range all() {
			if e["msg"] == "reconciler hook" {
				outcomes = append(outcomes, e["outcome"])
			}
		}
		Expect(outcomes).To(Equal([]any{"applied", "unchanged"}))
	})

	It("Pre ensures the identity with the exporter's query sets in the Role", func(ctx context.Context) {
		build(interceptor.Funcs{}, newExporter("ns", "default", "base"))

		_, err := srv.Pre(ctx, hookRequest(newCluster("ns", "pg", ours(nil))))
		Expect(err).NotTo(HaveOccurred())

		var role rbacv1.Role
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg-telemetry"}, &role)).To(Succeed())
		Expect(role.Rules).To(ContainElement(HaveField("ResourceNames", ContainElement("base"))))
	})

	DescribeTable("Pre and Post never return REQUEUE, TERMINATE or a gRPC error",
		func(ctx context.Context, request func() *reconciler.ReconcilerHooksRequest, funcs interceptor.Funcs) {
			build(funcs)
			srv.Timeout = 50 * time.Millisecond
			ctx, _ = captureAll(ctx)

			for name, hook := range map[string]func(context.Context, *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error){
				"Pre": srv.Pre, "Post": srv.Post,
			} {
				res, err := hook(ctx, request())
				Expect(err).NotTo(HaveOccurred(), name)
				Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE), name)
				Expect(res.GetRequeueAfter()).To(BeZero(), name)
			}
		},
		Entry("enabled", func() *reconciler.ReconcilerHooksRequest {
			return hookRequest(newCluster("ns", "pg", ours(nil)))
		}, interceptor.Funcs{}),
		Entry("absent", func() *reconciler.ReconcilerHooksRequest {
			return hookRequest(newCluster("ns", "pg"))
		}, interceptor.Funcs{}),
		Entry("disabled", func() *reconciler.ReconcilerHooksRequest {
			return hookRequest(newCluster("ns", "pg", disabled(nil)))
		}, interceptor.Funcs{}),
		Entry("invalid auth", func() *reconciler.ReconcilerHooksRequest {
			return hookRequest(newCluster("ns", "pg", ours(map[string]string{"auth": "kerberos"})))
		}, interceptor.Funcs{}),
		Entry("no UID", func() *reconciler.ReconcilerHooksRequest {
			cluster := newCluster("ns", "pg", ours(nil))
			cluster.UID = ""

			return hookRequest(cluster)
		}, interceptor.Funcs{}),
		Entry("Ensure fails", func() *reconciler.ReconcilerHooksRequest {
			return hookRequest(newCluster("ns", "pg", ours(nil)))
		}, interceptor.Funcs{Apply: func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
			return errApply
		}}),
		Entry("Ensure runs past the timeout", func() *reconciler.ReconcilerHooksRequest {
			return hookRequest(newCluster("ns", "pg", ours(nil)))
		}, interceptor.Funcs{Apply: func(ctx context.Context, _ client.WithWatch, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
			<-ctx.Done()

			return ctx.Err()
		}}),
		Entry("a Backup", func() *reconciler.ReconcilerHooksRequest {
			raw := []byte(`{"apiVersion":"postgresql.cnpg.io/v1","kind":"Backup","metadata":{"name":"b"}}`)
			req := hookRequest(newCluster("ns", "pg", ours(nil)))
			req.ResourceDefinition = raw

			return req
		}, interceptor.Funcs{}),
		Entry("undecodable input", func() *reconciler.ReconcilerHooksRequest {
			return &reconciler.ReconcilerHooksRequest{ClusterDefinition: []byte(`{"spec":`), ResourceDefinition: []byte(`{"kind":`)}
		}, interceptor.Funcs{}),
		Entry("an empty request", func() *reconciler.ReconcilerHooksRequest {
			return &reconciler.ReconcilerHooksRequest{}
		}, interceptor.Funcs{}),
	)

	It("Pre does nothing and returns CONTINUE when the plugin is absent, disabled, or auth is invalid", func(ctx context.Context) {
		for name, cluster := range map[string]*apiv1.Cluster{
			"absent":       newCluster("ns", "pg"),
			"disabled":     newCluster("ns", "pg", disabled(nil)),
			"invalid auth": newCluster("ns", "pg", ours(map[string]string{"auth": "kerberos"})),
		} {
			ctx, all := captureAll(ctx)

			res, err := srv.Pre(ctx, hookRequest(cluster))
			Expect(err).NotTo(HaveOccurred(), name)
			Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE), name)
			Expect(identityExists(ctx, c, "pg")).To(Equal([4]bool{}), name)
			Expect(hookLine(all(), "Pre")).To(HaveKeyWithValue("outcome", "skipped"), name)
		}
	})

	It("Pre returns CONTINUE and logs an error when Ensure fails", func(ctx context.Context) {
		build(interceptor.Funcs{Apply: func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
			return errApply
		}})
		ctx, all := captureAll(ctx)

		res, err := srv.Pre(ctx, hookRequest(newCluster("ns", "pg", ours(nil))))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE))
		Expect(hookLine(all(), "Pre")).To(HaveKeyWithValue("outcome", "error"))
		Expect(all()).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("msg", "ensure identity"),
			HaveKeyWithValue("error", ContainSubstring("apply refused")),
		)))
	})

	It("Pre returns CONTINUE and logs an error when Ensure runs past its timeout", func(ctx context.Context) {
		build(interceptor.Funcs{Apply: func(ctx context.Context, _ client.WithWatch, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
			<-ctx.Done()

			return ctx.Err()
		}})
		srv.Timeout = 50 * time.Millisecond
		ctx, all := captureAll(ctx)

		start := time.Now()
		res, err := srv.Pre(ctx, hookRequest(newCluster("ns", "pg", ours(nil))))
		Expect(err).NotTo(HaveOccurred())
		Expect(time.Since(start)).To(BeNumerically("<", 2*time.Second))
		Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE))
		Expect(hookLine(all(), "Pre")).To(HaveKeyWithValue("outcome", "error"))
		Expect(all()).To(ContainElement(HaveKeyWithValue("error", ContainSubstring("deadline exceeded"))))
	})

	It("Pre returns CONTINUE for a non-Cluster kind and for undecodable input, with an error line", func(ctx context.Context) {
		backup := hookRequest(newCluster("ns", "pg", ours(nil)))
		backup.ResourceDefinition = []byte(`{"apiVersion":"postgresql.cnpg.io/v1","kind":"Backup","metadata":{"name":"b"}}`)
		garbage := &reconciler.ReconcilerHooksRequest{ClusterDefinition: []byte(`{"spec":`), ResourceDefinition: []byte(`{"spec":`)}

		for name, req := range map[string]*reconciler.ReconcilerHooksRequest{"backup": backup, "garbage": garbage} {
			ctx, all := captureAll(ctx)

			res, err := srv.Pre(ctx, req)
			Expect(err).NotTo(HaveOccurred(), name)
			Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE), name)
			Expect(all()).To(ContainElement(HaveKey("error")), name)
			Expect(hookLine(all(), "Pre")).To(HaveKeyWithValue("outcome", "error"), name)
			Expect(identityExists(ctx, c, "pg")).To(Equal([4]bool{}), name)
		}
	})

	It("Post creates nothing and logs a skipped hook", func(ctx context.Context) {
		ctx, all := captureAll(ctx)

		res, err := srv.Post(ctx, hookRequest(newCluster("ns", "pg", ours(nil))))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.GetBehavior()).To(Equal(reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE))
		Expect(identityExists(ctx, c, "pg")).To(Equal([4]bool{}))
		line := hookLine(all(), "Post")
		Expect(line).To(SatisfyAll(
			HaveKeyWithValue("namespace", "ns"),
			HaveKeyWithValue("cluster", "pg"),
			HaveKeyWithValue("outcome", "skipped"),
		))
	})

	It("declares the Cluster kind", func(ctx context.Context) {
		resp, err := srv.GetCapabilities(ctx, &reconciler.ReconcilerHooksCapabilitiesRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetReconcilerCapabilities()).To(HaveLen(1))
		Expect(resp.GetReconcilerCapabilities()[0].GetKind()).To(Equal(reconciler.ReconcilerHooksCapability_KIND_CLUSTER))
	})
})
