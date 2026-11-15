package identity_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
)

var (
	errBoom   = errors.New("boom")
	errDenied = errors.New("denied")
)

const (
	saName     = "ServiceAccount/pg.telemetry"
	roleName   = "Role/pg.telemetry"
	bindName   = "RoleBinding/pg.telemetry"
	secretRef  = "Secret/pg.telemetry-token" //nolint:gosec // an object name, not a credential
	tokenKey   = "pg.telemetry-token"
	manager    = "plugin-telemetry"
	identityNS = "ns"
)

func kindName(obj client.Object) string {
	switch obj.(type) {
	case *corev1.ServiceAccount:
		return "ServiceAccount/" + obj.GetName()
	case *rbacv1.Role:
		return "Role/" + obj.GetName()
	case *rbacv1.RoleBinding:
		return "RoleBinding/" + obj.GetName()
	default:
		return "Secret/" + obj.GetName()
	}
}

func applyKindName(obj runtime.ApplyConfiguration) (string, map[string]any, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return "", nil, fmt.Errorf("marshal apply configuration: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", nil, fmt.Errorf("unmarshal apply configuration: %w", err)
	}
	meta, _ := out["metadata"].(map[string]any)

	return fmt.Sprintf("%v/%v", out["kind"], meta["name"]), out, nil
}

var _ = Describe("Ensurer", func() {
	var (
		ctx       context.Context
		c         client.Client
		ensurer   *identity.Ensurer
		calls     []string
		creates   map[string]client.Object
		createFM  map[string]string
		applies   map[string]map[string]any
		applyOpts client.ApplyOptions
		deletes   map[string]*client.DeleteOptions
		funcs     interceptor.Funcs
	)

	// cached returns the four desired objects with a uid each, the token with data, as the cache holds them.
	cached := func(mutate func(*identity.Objects)) []client.Object {
		GinkgoHelper()
		want, err := identity.Desired(newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		want.Token.Data = map[string][]byte{"token": []byte("t")}
		if mutate != nil {
			mutate(&want)
		}
		objs := []client.Object{want.ServiceAccount, want.Role, want.RoleBinding, want.Token}
		for _, o := range objs {
			o.SetUID(types.UID("uid-" + kindName(o)))
		}

		return objs
	}

	build := func(objs ...client.Object) {
		GinkgoHelper()
		c = fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
		ensurer = &identity.Ensurer{Reader: c, Writer: c}
	}

	BeforeEach(func() {
		ctx = context.Background()
		calls, creates, createFM, applies, deletes = nil, map[string]client.Object{}, map[string]string{}, map[string]map[string]any{}, map[string]*client.DeleteOptions{}
		applyOpts = client.ApplyOptions{}
		funcs = interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.CreateOption) error {
				name := kindName(obj)
				calls = append(calls, "create "+name)
				creates[name] = obj
				opts := &client.CreateOptions{}
				opts.ApplyOptions(o)
				createFM[name] = opts.FieldManager

				return c.Create(ctx, obj, o...)
			},
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, o ...client.ApplyOption) error {
				name, payload, err := applyKindName(obj)
				if err != nil {
					return err
				}
				calls = append(calls, "apply "+name)
				applies[name] = payload
				applyOpts = client.ApplyOptions{}
				applyOpts.ApplyOptions(o)

				return c.Apply(ctx, obj, o...)
			},
			Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
				return fmt.Errorf("unexpected patch of %s", kindName(obj))
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.DeleteOption) error {
				name := kindName(obj)
				calls = append(calls, "delete "+name)
				opts := &client.DeleteOptions{}
				opts.ApplyOptions(o)
				deletes[name] = opts

				return c.Delete(ctx, obj, o...)
			},
		}
		build()
	})

	It("creates each missing object with a plain create as plugin-telemetry, in order, and applies nothing", func() {
		wrote, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		want := []string{saName, roleName, bindName, secretRef}
		Expect(wrote).To(Equal(want))
		Expect(calls).To(Equal([]string{"create " + saName, "create " + roleName, "create " + bindName, "create " + secretRef}))
		Expect(createFM).To(Equal(map[string]string{saName: manager, roleName: manager, bindName: manager, secretRef: manager}))

		token, ok := creates[secretRef].(*corev1.Secret)
		Expect(ok).To(BeTrue())
		Expect(token.Type).To(Equal(corev1.SecretTypeServiceAccountToken))
		Expect(token.Annotations).To(Equal(map[string]string{corev1.ServiceAccountNameKey: "pg.telemetry"}))
		Expect(token.Data).To(BeEmpty())
		Expect(token.OwnerReferences).To(HaveLen(1))
		Expect(token.Labels).To(HaveKeyWithValue(identity.ManagedByLabel, identity.ManagedBy))
	})

	It("writes nothing when the cache holds every object as desired", func() {
		build(cached(nil)...)

		wrote, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(wrote).To(BeEmpty())
		Expect(calls).To(BeEmpty())
	})

	DescribeTable("re-applies only the drifted object, pinned to its cached uid, with plugin-telemetry and force",
		func(mutate func(*identity.Objects), drifted string) {
			build(cached(mutate)...)

			wrote, err := ensurer.Ensure(ctx, newCluster(), refs())
			Expect(err).NotTo(HaveOccurred())
			Expect(wrote).To(Equal([]string{drifted}))
			Expect(calls).To(Equal([]string{"apply " + drifted}))

			meta, ok := applies[drifted]["metadata"].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(meta).To(HaveKeyWithValue("uid", "uid-"+drifted))
			Expect(applies[drifted]).NotTo(HaveKey("data"))
			Expect(applies[drifted]).NotTo(HaveKey("stringData"))
			Expect(applyOpts.AsPatchOptions().FieldManager).To(Equal(manager))
			Expect(*applyOpts.AsPatchOptions().Force).To(BeTrue())
		},
		Entry("label removed", func(o *identity.Objects) { delete(o.ServiceAccount.Labels, identity.ClusterLabel) }, saName),
		Entry("owner reference removed", func(o *identity.Objects) { o.Role.OwnerReferences = nil }, roleName),
		Entry("rule widened", func(o *identity.Objects) { o.Role.Rules[0].ResourceNames = nil }, roleName),
		Entry("subject added", func(o *identity.Objects) {
			o.RoleBinding.Subjects = append(o.RoleBinding.Subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: "other", Namespace: identityNS})
		}, bindName),
		Entry("token annotation changed", func(o *identity.Objects) {
			o.Token.Annotations[corev1.ServiceAccountNameKey] = "other"
		}, secretRef),
	)

	It("applies nothing when someone adds another, non-controller owner reference", func() {
		build(cached(func(o *identity.Objects) {
			o.ServiceAccount.OwnerReferences = append(o.ServiceAccount.OwnerReferences, metav1.OwnerReference{
				APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "other-uid",
			})
		})...)

		wrote, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(wrote).To(BeEmpty())
		Expect(calls).To(BeEmpty())
	})

	It("stops the pass with ErrInTheWay when a create finds the object already there", func() {
		inner := funcs.Create
		funcs.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.CreateOption) error {
			if _, ok := obj.(*rbacv1.Role); ok {
				calls = append(calls, "create "+kindName(obj))

				return apierrors.NewAlreadyExists(schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"}, obj.GetName())
			}

			return inner(ctx, c, obj, o...)
		}
		build()

		wrote, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(identity.ErrInTheWay))
		Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring(roleName)))
		Expect(wrote).To(Equal([]string{saName}))
		Expect(calls).To(Equal([]string{"create " + saName, "create " + roleName}))
	})

	It("writes nothing over a cached token Secret of another type and returns ErrInTheWay", func() {
		build(cached(func(o *identity.Objects) { o.Token.Type = corev1.SecretTypeOpaque })...)

		wrote, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(identity.ErrInTheWay))
		Expect(err).To(MatchError(ContainSubstring(secretRef)))
		Expect(wrote).To(BeEmpty())
		Expect(calls).To(BeEmpty())
	})

	It("returns a failed create that is not AlreadyExists without ErrInTheWay", func() {
		funcs.Create = func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error { return errBoom }
		build()

		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(errBoom))
		Expect(err).NotTo(MatchError(identity.ErrInTheWay))
		Expect(err).To(MatchError(ContainSubstring(saName)))
	})

	It("returns a failed apply with the object", func() {
		funcs.Apply = func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
			return errBoom
		}
		build(cached(func(o *identity.Objects) { o.Role.OwnerReferences = nil })...)

		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(errBoom))
		Expect(err).To(MatchError(ContainSubstring(roleName)))
	})

	It("returns a failed cache read with the object", func() {
		funcs.Get = func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errBoom
		}
		build()

		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(errBoom))
		Expect(err).To(MatchError(ContainSubstring(saName)))
		Expect(calls).To(BeEmpty())
	})

	It("fails Ensure for a Cluster without a UID before writing anything", func() {
		cl := newCluster()
		cl.UID = ""

		_, err := ensurer.Ensure(ctx, cl, refs())
		Expect(err).To(MatchError(identity.ErrNoUID))
		Expect(calls).To(BeEmpty())
	})

	It("two replicas ensuring the same new Cluster end with one object set and no lasting problem line", func() {
		store := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).Build()
		cacheA := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).Build()
		cacheB := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).Build()
		a := &identity.Ensurer{Reader: cacheA, Writer: store}
		b := &identity.Ensurer{Reader: cacheB, Writer: store}

		wrote, err := a.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(wrote).To(HaveLen(4))

		wrote, err = b.Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(identity.ErrInTheWay))
		Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
		Expect(wrote).To(BeEmpty())

		var token corev1.Secret
		Expect(store.Get(ctx, client.ObjectKey{Namespace: identityNS, Name: tokenKey}, &token)).To(Succeed())
		token.Data = map[string][]byte{"token": []byte("t")}
		Expect(store.Update(ctx, &token)).To(Succeed())
		for _, obj := range identity.Members(identityNS, "pg") {
			Expect(store.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
			obj.SetResourceVersion("")
			Expect(cacheB.Create(ctx, obj)).To(Succeed())
		}

		_, err = b.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(b.Pending(ctx, identityNS, "pg")).To(BeEmpty())

		var roles rbacv1.RoleList
		Expect(store.List(ctx, &roles, client.InNamespace(identityNS))).To(Succeed())
		Expect(roles.Items).To(HaveLen(1))
	})

	Describe("Remove", func() {
		It("deletes what it may, skips a token Secret of another type, and joins the errors", func() {
			funcs.Delete = func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.DeleteOption) error {
				switch obj.(type) {
				case *rbacv1.Role:
					return errBoom
				case *rbacv1.RoleBinding:
					return errDenied
				}
				calls = append(calls, "delete "+kindName(obj))
				opts := &client.DeleteOptions{}
				opts.ApplyOptions(o)
				deletes[kindName(obj)] = opts

				return c.Delete(ctx, obj, o...)
			}
			build(cached(func(o *identity.Objects) { o.Token.Type = corev1.SecretTypeOpaque })...)

			n, err := ensurer.Remove(ctx, identityNS, "pg")
			Expect(err).To(MatchError(errBoom))
			Expect(err).To(MatchError(errDenied))
			Expect(n).To(Equal(1))
			Expect(calls).To(Equal([]string{"delete " + saName}))
			Expect(c.Get(ctx, client.ObjectKey{Namespace: identityNS, Name: tokenKey}, &corev1.Secret{})).To(Succeed())
		})

		It("deletes the four with uid and resourceVersion preconditions and counts them", func() {
			build(cached(nil)...)

			n, err := ensurer.Remove(ctx, identityNS, "pg")
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(4))
			Expect(deletes).To(HaveLen(4))
			for name, opts := range deletes {
				Expect(opts.Preconditions).NotTo(BeNil(), name)
				Expect(opts.Preconditions.UID).To(HaveValue(Equal(types.UID("uid-"+name))), name)
				Expect(opts.Preconditions.ResourceVersion).To(HaveValue(Not(BeEmpty())), name)
			}
		})

		It("counts nothing and fails nothing for objects the cache lacks or that are already gone", func() {
			n, err := ensurer.Remove(ctx, identityNS, "pg")
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(BeZero())

			funcs.Delete = func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "x"}, "x")
			}
			build(cached(nil)...)
			n, err = ensurer.Remove(ctx, identityNS, "pg")
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(BeZero())
		})

		It("returns a failed cache read", func() {
			funcs.Get = func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errBoom
			}
			build()

			n, err := ensurer.Remove(ctx, identityNS, "pg")
			Expect(err).To(MatchError(errBoom))
			Expect(n).To(BeZero())
		})
	})

	Describe("Pending", func() {
		pending := func() []string {
			GinkgoHelper()
			lines, err := ensurer.Pending(ctx, identityNS, "pg")
			Expect(err).NotTo(HaveOccurred())

			return lines
		}

		It("lists each missing object, a wrong-typed token and a token without data, and nothing once ready", func() {
			const missing = " is not in the plugin's cache: not created yet, or not the plugin's"
			Expect(pending()).To(ConsistOf(saName+missing, roleName+missing, bindName+missing, secretRef+missing))

			build(cached(func(o *identity.Objects) { o.Token.Type = corev1.SecretTypeOpaque })...)
			Expect(pending()).To(ConsistOf(secretRef + " has type Opaque, not kubernetes.io/service-account-token: delete it"))

			build(cached(func(o *identity.Objects) { o.Token.Data = nil })...)
			Expect(pending()).To(ConsistOf(secretRef + " has no data.token yet"))

			build(cached(nil)...)
			Expect(pending()).To(BeEmpty())
		})

		It("lists a missing Role beside the other three being cached", func() {
			objs := cached(nil)
			build(objs[0], objs[2], objs[3])

			Expect(pending()).To(ConsistOf(roleName + " is not in the plugin's cache: not created yet, or not the plugin's"))
		})

		It("returns a failed cache read", func() {
			funcs.Get = func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errBoom
			}
			build()

			_, err := ensurer.Pending(ctx, identityNS, "pg")
			Expect(err).To(MatchError(errBoom))
		})
	})
})
