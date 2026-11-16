package operator_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

func requestFor(namespace, name string) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: namespace, Name: name}}
}

var errCreate = errors.New("create refused")

var _ = Describe("IdentityController", func() {
	var (
		c       client.Client
		ctrl    *operator.IdentityController
		deletes atomic.Int32
		writes  atomic.Int32
	)

	build := func(objs ...client.Object) {
		deletes.Store(0)
		writes.Store(0)
		c = newClient(objs, interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes.Add(1)
				writes.Add(1)

				return cl.Delete(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				writes.Add(1)

				return cl.Patch(ctx, obj, patch, opts...)
			},
			Apply: func(ctx context.Context, cl client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				writes.Add(1)

				return cl.Apply(ctx, obj, opts...)
			},
		})
		ctrl = &operator.IdentityController{Client: c, Identity: &identity.Ensurer{Reader: c, Writer: c}}
	}

	reconcileOnce := func(ctx context.Context, namespace, name string) {
		GinkgoHelper()

		res, err := ctrl.Reconcile(ctx, requestFor(namespace, name))
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(reconcile.Result{}))
	}

	It("ensures the identity after a Cluster spec change and ignores status-only updates", func(ctx context.Context) {
		build(newCluster("ns", "pg", ours(nil)))
		reconcileOnce(ctx, "ns", "pg")
		Expect(identityExists(ctx, c, "pg")).To(Equal(allFour))

		var cluster apiv1.Cluster
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg"}, &cluster)).To(Succeed())
		cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{
			CustomQueriesConfigMap: []apiv1.ConfigMapKeySelector{{
				LocalObjectReference: apiv1.LocalObjectReference{Name: "queries"}, Key: "q",
			}},
		}
		Expect(c.Update(ctx, &cluster)).To(Succeed())
		reconcileOnce(ctx, "ns", "pg")

		var role rbacv1.Role
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &role)).To(Succeed())
		Expect(role.Rules).To(ContainElement(HaveField("ResourceNames", ContainElement("queries"))))

		old := newCluster("ns", "pg", ours(nil))
		statusOnly := old.DeepCopy()
		statusOnly.Status.Phase = "Cluster in healthy state"
		specChange := old.DeepCopy()
		specChange.Generation = 2

		Expect(operator.SpecChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: statusOnly})).To(BeFalse())
		Expect(operator.SpecChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: specChange})).To(BeTrue())
		Expect(operator.SpecChanged.Create(event.CreateEvent{Object: old})).To(BeTrue())
	})

	It("re-ensures each Cluster that uses an exporter when it changes, and no other", func(ctx context.Context) {
		build(
			newCluster("ns", "by-param", ours(map[string]string{"exporter": "shared"})),
			newCluster("ns", "by-default", ours(nil)),
			newCluster("ns", "other-exporter", ours(map[string]string{"exporter": "elsewhere"})),
			newCluster("ns", "disabled", disabled(map[string]string{"exporter": "shared"})),
			newCluster("ns", "no-plugin"),
			newCluster("other", "same-name-elsewhere", ours(map[string]string{"exporter": "shared"})),
		)
		mapper := operator.ExportersToClusters(c)

		Expect(mapper(ctx, newExporter("ns", "shared"))).To(ConsistOf(requestFor("ns", "by-param")))
		Expect(mapper(ctx, newExporter("ns", "default"))).To(ConsistOf(requestFor("ns", "by-default")))
		Expect(mapper(ctx, newExporter("ns", "unused"))).To(BeEmpty())
		Expect(mapper(ctx, newExporter("other", "shared"))).To(ConsistOf(requestFor("other", "same-name-elsewhere")))
	})

	It("lists the namespace's Clusters without a deep copy", func(ctx context.Context) {
		var listed []*client.ListOptions
		cl := newClient([]client.Object{
			newCluster("ns", "pg", ours(map[string]string{"exporter": "shared"})),
			newCluster("ns", "other", ours(map[string]string{"exporter": "elsewhere"})),
		}, interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				o := &client.ListOptions{}
				o.ApplyOptions(opts)
				listed = append(listed, o)

				return cl.List(ctx, list, opts...)
			},
		})

		Expect(operator.ExportersToClusters(cl)(ctx, newExporter("ns", "shared"))).To(ConsistOf(requestFor("ns", "pg")))

		Expect(listed).To(HaveLen(1))
		Expect(listed[0].UnsafeDisableDeepCopy).To(HaveValue(BeTrue()))
	})

	It("removes the identity when the plugin entry is disabled or removed", func(ctx context.Context) {
		for name, edit := range map[string]func(*apiv1.Cluster){
			"disabled": func(cl *apiv1.Cluster) { cl.Spec.Plugins = []apiv1.PluginConfiguration{disabled(nil)} },
			"removed":  func(cl *apiv1.Cluster) { cl.Spec.Plugins = nil },
		} {
			build(newCluster("ns", "pg", ours(nil)))
			reconcileOnce(ctx, "ns", "pg")
			Expect(identityExists(ctx, c, "pg")).To(Equal(allFour), name)

			var cluster apiv1.Cluster
			Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg"}, &cluster)).To(Succeed())
			edit(&cluster)
			Expect(c.Update(ctx, &cluster)).To(Succeed())
			reconcileOnce(ctx, "ns", "pg")

			Expect(identityExists(ctx, c, "pg")).To(Equal([4]bool{}), name)
		}
	})

	It("makes no delete call for a Cluster that never had an identity", func(ctx context.Context) {
		build(newCluster("ns", "pg"), newCluster("ns", "off", disabled(nil)))

		reconcileOnce(ctx, "ns", "pg")
		reconcileOnce(ctx, "ns", "off")

		Expect(deletes.Load()).To(BeZero())
	})

	It("removes the identity when the Cluster has invalid auth", func(ctx context.Context) {
		build(newCluster("ns", "pg", ours(nil)))
		reconcileOnce(ctx, "ns", "pg")

		var cluster apiv1.Cluster
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg"}, &cluster)).To(Succeed())
		cluster.Spec.Plugins = []apiv1.PluginConfiguration{ours(map[string]string{"auth": "kerberos"})}
		Expect(c.Update(ctx, &cluster)).To(Succeed())
		reconcileOnce(ctx, "ns", "pg")

		Expect(identityExists(ctx, c, "pg")).To(Equal([4]bool{}))
	})

	It("restores an owned object that was deleted or edited", func(ctx context.Context) {
		build(newCluster("ns", "pg", ours(nil)))
		reconcileOnce(ctx, "ns", "pg")

		Expect(c.Delete(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}})).To(Succeed())
		var role rbacv1.Role
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &role)).To(Succeed())
		role.Rules = nil
		Expect(c.Update(ctx, &role)).To(Succeed())

		reconcileOnce(ctx, "ns", "pg")

		Expect(identityExists(ctx, c, "pg")).To(Equal(allFour))
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &role)).To(Succeed())
		Expect(role.Rules).NotTo(BeEmpty())
	})

	It("does nothing for a Cluster being deleted, and for one that is gone", func(ctx context.Context) {
		now := metav1.Now()
		dying := newCluster("ns", "dying", ours(nil))
		dying.DeletionTimestamp = &now
		dying.Finalizers = []string{"test/hold"}
		build(dying)

		reconcileOnce(ctx, "ns", "dying")
		reconcileOnce(ctx, "ns", "gone")

		Expect(identityExists(ctx, c, "dying")).To(Equal([4]bool{}))
		Expect(deletes.Load()).To(BeZero())
	})

	It("ensures the identity whatever other Clusters the namespace holds", func(ctx context.Context) {
		build(newCluster("ns", "pg", ours(nil)), newCluster("ns", "pg-telemetry"))
		reconcileOnce(ctx, "ns", "pg")

		Expect(identityExists(ctx, c, "pg")).To(Equal(allFour))
	})

	It("requeues after 5 s, with no error, while an object is in the way", func(ctx context.Context) {
		build(newCluster("ns", "pg", ours(nil)), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}})
		ctrl.Identity = &identity.Ensurer{Reader: newClient(nil, interceptor.Funcs{}), Writer: c}

		res, err := ctrl.Reconcile(ctx, requestFor("ns", "pg"))

		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(reconcile.Result{RequeueAfter: 5 * time.Second}))
		Expect(identityExists(ctx, c, "pg")).To(Equal([4]bool{false, false, false, true}))
		Expect(writes.Load()).To(BeZero())
	})

	It("drops create events of the owned objects and keeps updates, deletes and generic events", func() {
		sa := &corev1.ServiceAccount{}

		Expect(operator.OwnedObjects.Create(event.CreateEvent{Object: sa})).To(BeFalse())
		Expect(operator.OwnedObjects.Update(event.UpdateEvent{ObjectOld: sa, ObjectNew: sa})).To(BeTrue())
		Expect(operator.OwnedObjects.Delete(event.DeleteEvent{Object: sa})).To(BeTrue())
		Expect(operator.OwnedObjects.Generic(event.GenericEvent{Object: sa})).To(BeTrue())
	})

	It("logs an object in the way once when it starts and once when it clears", func(ctx context.Context) {
		var lines []string
		sink := funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{})
		ctx = logr.NewContext(ctx, sink)
		blocker := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}}
		build(newCluster("ns", "pg", ours(nil)), blocker)
		ctrl.Identity = &identity.Ensurer{Reader: newClient(nil, interceptor.Funcs{}), Writer: c}
		count := func(outcome string) int {
			n := 0
			for _, l := range lines {
				if strings.Contains(l, `"outcome"="`+outcome+`"`) {
					n++
				}
			}

			return n
		}

		for range 3 {
			res, err := ctrl.Reconcile(ctx, requestFor("ns", "pg"))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(5 * time.Second))
		}
		Expect(count("in the way")).To(Equal(1))

		Expect(c.Delete(ctx, blocker)).To(Succeed())
		ctrl.Identity = &identity.Ensurer{Reader: c, Writer: c}
		reconcileOnce(ctx, "ns", "pg")
		reconcileOnce(ctx, "ns", "pg")
		Expect(count("in the way cleared")).To(Equal(1))
	})

	It("registers with a manager", func() {
		mgr, err := operator.NewManager(&rest.Config{Host: discoveryServer().URL}, "cnpg-system", "telemetry-sidecar-image")
		Expect(err).NotTo(HaveOccurred())
		build()

		Expect((&operator.IdentityController{Client: mgr.GetClient(), Identity: ctrl.Identity}).SetupWithManager(mgr)).To(Succeed())
	})

	It("returns the Ensure error so the workqueue backs off", func(ctx context.Context) {
		failing := newClient([]client.Object{newCluster("ns", "pg", ours(nil))}, interceptor.Funcs{
			Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return errCreate
			},
		})
		ctrl = &operator.IdentityController{Client: failing, Identity: &identity.Ensurer{Reader: failing, Writer: failing}}

		res, err := ctrl.Reconcile(ctx, requestFor("ns", "pg"))
		Expect(err).To(MatchError(errCreate))
		Expect(res).To(Equal(reconcile.Result{}))
	})

	Describe("identity reconcile metrics", func() {
		var m *operator.PluginMetrics

		BeforeEach(func() { m = operator.NewPluginMetrics() })

		reconcileWith := func(ctx context.Context, cl, reader client.Client) {
			GinkgoHelper()

			ctrl = &operator.IdentityController{Client: cl, Identity: &identity.Ensurer{Reader: reader, Writer: cl}, Metrics: m}
			_, _ = ctrl.Reconcile(ctx, requestFor("ns", "pg"))
		}

		It("counts identity reconcile errors by reason", func(ctx context.Context) {
			forbidden := newClient([]client.Object{newCluster("ns", "pg", ours(nil))}, interceptor.Funcs{
				Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*rbacv1.RoleBinding); ok {
						return apierrors.NewForbidden(schema.GroupResource{Group: rbacv1.GroupName, Resource: "rolebindings"}, obj.GetName(), errors.New("denied"))
					}

					return cl.Create(ctx, obj, opts...)
				},
			})
			reconcileWith(ctx, forbidden, forbidden)

			unavailable := newClient([]client.Object{newCluster("ns", "pg", ours(nil))}, interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return apierrors.NewServiceUnavailable("overloaded")
				},
			})
			reconcileWith(ctx, unavailable, unavailable)

			deadline := newClient([]client.Object{newCluster("ns", "pg", ours(nil))}, interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return context.DeadlineExceeded
				},
			})
			reconcileWith(ctx, deadline, deadline)

			boom := newClient([]client.Object{newCluster("ns", "pg", ours(nil))}, interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return errors.New("boom")
				},
			})
			reconcileWith(ctx, boom, boom)

			exists := newClient([]client.Object{
				newCluster("ns", "pg", ours(nil)),
				&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}},
			}, interceptor.Funcs{})
			reconcileWith(ctx, exists, newClient(nil, interceptor.Funcs{}))

			Expect(counts(m, identityCount)).To(Equal(map[string]int64{
				"outcome=error,reason=Forbidden":          1,
				"outcome=error,reason=ServiceUnavailable": 1,
				"outcome=error,reason=Timeout":            1,
				"outcome=error,reason=Unknown":            1,
				"outcome=error,reason=AlreadyExists":      1,
			}))
		})

		It("records Unknown for an object in the way with no API error inside", func(ctx context.Context) {
			token := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: metadata.TokenSecretName("pg")},
				Type:       corev1.SecretTypeOpaque,
			}
			cl := newClient([]client.Object{newCluster("ns", "pg", ours(nil)), token}, interceptor.Funcs{})
			reconcileWith(ctx, cl, cl)

			Expect(counts(m, identityCount)).To(Equal(map[string]int64{"outcome=error,reason=Unknown": 1}))
		})

		It("records a clean reconcile as ok with no reason", func(ctx context.Context) {
			cl := newClient([]client.Object{newCluster("ns", "pg", ours(nil))}, interceptor.Funcs{})
			reconcileWith(ctx, cl, cl)

			Expect(counts(m, identityCount)).To(Equal(map[string]int64{"outcome=ok": 1}))
		})
	})
})
