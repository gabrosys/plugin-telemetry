package identity_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
)

type appliedConfig struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
}

var _ = Describe("Ensurer", func() {
	var (
		ctx     context.Context
		c       client.Client
		ensurer *identity.Ensurer
		order   []string
		opts    []client.ApplyOption
		secret  map[string]any
		writes  []string
	)

	BeforeEach(func() {
		ctx = context.Background()
		order, opts, secret, writes = nil, nil, nil, nil

		c = fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithInterceptorFuncs(interceptor.Funcs{
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, o ...client.ApplyOption) error {
				raw, err := runtimeJSON(obj)
				if err != nil {
					return err
				}
				order = append(order, raw.Kind+"/"+raw.Metadata.Name)
				if raw.Kind == "Secret" {
					if secret, err = payload(obj); err != nil {
						return err
					}
				}
				opts = o

				return c.Apply(ctx, obj, o...)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, o ...client.PatchOption) error {
				writes = append(writes, fmt.Sprintf("patch %T %s", obj, obj.GetName()))

				return c.Patch(ctx, obj, p, o...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.DeleteOption) error {
				writes = append(writes, fmt.Sprintf("delete %T %s", obj, obj.GetName()))

				return c.Delete(ctx, obj, o...)
			},
		}).Build()
		ensurer = &identity.Ensurer{Reader: c, Live: c, Writer: c}
	})

	setToken := func(data string) {
		GinkgoHelper()
		var s corev1.Secret
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry-token"}, &s)).To(Succeed())
		s.Data = map[string][]byte{"token": []byte(data)}
		Expect(c.Update(ctx, &s)).To(Succeed())
	}

	// foreign creates objects named like pg's identity that the plugin didn't make.
	foreign := func(objs ...client.Object) {
		GinkgoHelper()
		for _, obj := range objs {
			obj.SetNamespace("ns")
			Expect(c.Create(ctx, obj)).To(Succeed())
		}
	}

	It("applies all four on first Ensure, ServiceAccount before Secret and Role before RoleBinding", func() {
		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		want := []string{"ServiceAccount/pg.telemetry", "Role/pg.telemetry", "RoleBinding/pg.telemetry", "Secret/pg.telemetry-token"}
		Expect(order).To(Equal(want))
		Expect(res.Applied).To(Equal(want))
		Expect(res.Ready).To(BeFalse())

		applyOpts := &client.ApplyOptions{}
		applyOpts.ApplyOptions(opts)
		Expect(applyOpts.AsPatchOptions().FieldManager).To(Equal("plugin-telemetry"))
		Expect(*applyOpts.AsPatchOptions().Force).To(BeTrue())
	})

	It("applies nothing when the cache matches", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		order = nil

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(BeEmpty())
		Expect(res.Applied).To(BeEmpty())
	})

	It("re-applies a Role whose rules drifted, a RoleBinding whose subjects drifted, and a deleted token Secret", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		order = nil

		var role rbacv1.Role
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &role)).To(Succeed())
		role.Rules[0].ResourceNames = nil
		Expect(c.Update(ctx, &role)).To(Succeed())

		var binding rbacv1.RoleBinding
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &binding)).To(Succeed())
		binding.Subjects = append(binding.Subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: "other", Namespace: "ns"})
		Expect(c.Update(ctx, &binding)).To(Succeed())

		Expect(c.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry-token"}})).To(Succeed())

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Applied).To(Equal([]string{"Role/pg.telemetry", "RoleBinding/pg.telemetry", "Secret/pg.telemetry-token"}))

		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &role)).To(Succeed())
		Expect(role.Rules[0].ResourceNames).NotTo(BeEmpty())
	})

	It("reports Ready only once the token Secret has data.token", func() {
		ready, err := ensurer.Ready(ctx, "ns", "pg")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())

		_, err = ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		ready, err = ensurer.Ready(ctx, "ns", "pg")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())

		setToken("t")
		ready, err = ensurer.Ready(ctx, "ns", "pg")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeTrue())

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Ready).To(BeTrue())
	})

	It("reports Present when none, some or all of the four objects are cached", func() {
		present := func() bool {
			GinkgoHelper()
			got, err := ensurer.Present(ctx, "ns", "pg")
			Expect(err).NotTo(HaveOccurred())

			return got
		}
		Expect(present()).To(BeFalse())

		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(present()).To(BeTrue())

		for _, obj := range identity.Members("ns", "pg")[:3] {
			Expect(c.Delete(ctx, obj)).To(Succeed())
		}
		Expect(present()).To(BeTrue())

		Expect(c.Delete(ctx, identity.Members("ns", "pg")[3])).To(Succeed())
		Expect(present()).To(BeFalse())
	})

	It("sends the token Secret apply with only type, name annotation, labels and owner reference", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		Expect(secret).To(HaveLen(4))
		Expect(secret).To(HaveKey("apiVersion"))
		Expect(secret).To(HaveKey("kind"))
		Expect(secret).To(HaveKeyWithValue("type", "kubernetes.io/service-account-token"))
		Expect(secret).To(HaveKey("metadata"))
		meta, ok := secret["metadata"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(meta).To(HaveLen(5))
		Expect(meta).To(HaveKeyWithValue("name", "pg.telemetry-token"))
		Expect(meta).To(HaveKeyWithValue("namespace", "ns"))
		Expect(meta).To(HaveKey("labels"))
		Expect(meta).To(HaveKey("ownerReferences"))
		Expect(meta).To(HaveKeyWithValue("annotations", map[string]any{"kubernetes.io/service-account.name": "pg.telemetry"}))
	})

	It("keeps data.token when it re-applies the token Secret", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		setToken("t")

		var s corev1.Secret
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry-token"}, &s)).To(Succeed())
		delete(s.Labels, "telemetry.gabrosys.github.io/cluster")
		Expect(c.Update(ctx, &s)).To(Succeed())

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Applied).To(ContainElement("Secret/pg.telemetry-token"))

		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry-token"}, &s)).To(Succeed())
		Expect(string(s.Data["token"])).To(Equal("t"))
		Expect(s.Labels).To(HaveKey("telemetry.gabrosys.github.io/cluster"))
	})

	It("fails Ensure for a Cluster without a UID before applying anything", func() {
		cl := newCluster()
		cl.UID = ""
		_, err := ensurer.Ensure(ctx, cl, refs())
		Expect(err).To(MatchError(identity.ErrNoUID))
		Expect(order).To(BeEmpty())
	})

	It("Remove deletes the four and tolerates NotFound", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())

		Expect(ensurer.Remove(ctx, "ns", "pg")).To(Succeed())
		key := client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}
		Expect(c.Get(ctx, key, &corev1.ServiceAccount{})).To(Satisfy(apierrors.IsNotFound))
		Expect(c.Get(ctx, key, &rbacv1.Role{})).To(Satisfy(apierrors.IsNotFound))
		Expect(c.Get(ctx, key, &rbacv1.RoleBinding{})).To(Satisfy(apierrors.IsNotFound))
		key.Name = "pg.telemetry-token"
		Expect(c.Get(ctx, key, &corev1.Secret{})).To(Satisfy(apierrors.IsNotFound))

		Expect(ensurer.Remove(ctx, "ns", "pg")).To(Succeed())
	})

	It("refuses, writing nothing, when an object exists in the API server without the managed-by label", func() {
		foreign(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pg.telemetry"}})
		empty := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).Build()
		ensurer.Reader = empty

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Refused).To(Equal([]string{"ServiceAccount/pg.telemetry"}))
		Expect(res.Applied).To(BeEmpty())
		Expect(res.Ready).To(BeFalse())
		Expect(order).To(BeEmpty())
	})

	It("refuses when the cache still holds the plugin's labels but the API server's object lacks them", func() {
		foreign(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pg.telemetry"}})
		stale := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "pg.telemetry",
			Labels: map[string]string{identity.ManagedByLabel: identity.ManagedBy, identity.ClusterLabel: "pg"},
		}}).Build()
		ensurer.Reader = stale

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Refused).To(Equal([]string{"ServiceAccount/pg.telemetry"}))
		Expect(order).To(BeEmpty())
	})

	It("refuses, writing nothing, when an object is controlled by something other than the Cluster", func() {
		foreign(&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{
			Name:   "pg.telemetry",
			Labels: map[string]string{identity.ManagedByLabel: identity.ManagedBy},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "postgresql.cnpg.io/v1", Kind: "Pooler", Name: "pg.telemetry", UID: "pooler-uid", Controller: ptr.To(true),
			}},
		}})

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Refused).To(Equal([]string{"Role/pg.telemetry"}))
		Expect(order).To(BeEmpty())
	})

	It("re-applies objects an earlier Cluster of the same name owns", func() {
		earlier := newCluster()
		earlier.UID = "earlier-uid"
		_, err := ensurer.Ensure(ctx, earlier, refs())
		Expect(err).NotTo(HaveOccurred())
		order = nil

		res, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Refused).To(BeEmpty())
		Expect(res.Applied).To(HaveLen(4))
	})

	It("lists the objects the plugin doesn't manage, kind and name", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(ensurer.Foreign(ctx, newCluster())).To(BeEmpty())

		Expect(ensurer.Remove(ctx, "ns", "pg")).To(Succeed())
		foreign(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pg.telemetry"}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pg.telemetry-token"}})
		Expect(ensurer.Foreign(ctx, newCluster())).To(ConsistOf("ServiceAccount/pg.telemetry", "Secret/pg.telemetry-token"))
	})

	It("Remove deletes only the objects the plugin manages", func() {
		_, err := ensurer.Ensure(ctx, newCluster(), refs())
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Delete(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg.telemetry"}})).To(Succeed())
		foreign(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pg.telemetry"}})

		Expect(ensurer.Remove(ctx, "ns", "pg")).To(Succeed())
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &corev1.ServiceAccount{})).To(Succeed())
		Expect(c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pg.telemetry"}, &rbacv1.Role{})).To(Satisfy(apierrors.IsNotFound))
	})

	It("wraps a failed apply with the object", func() {
		failing := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithInterceptorFuncs(interceptor.Funcs{
			Apply: func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
				return fmt.Errorf("boom")
			},
		}).Build()
		_, err := (&identity.Ensurer{Reader: failing, Live: failing, Writer: failing}).Ensure(ctx, newCluster(), refs())
		Expect(err).To(MatchError(ContainSubstring("boom")))
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("serviceaccount"))
	})
})

func runtimeJSON(obj runtime.ApplyConfiguration) (appliedConfig, error) {
	var out appliedConfig
	raw, err := json.Marshal(obj)
	if err != nil {
		return out, fmt.Errorf("marshal apply configuration: %w", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("unmarshal apply configuration: %w", err)
	}

	return out, nil
}

func payload(obj runtime.ApplyConfiguration) (map[string]any, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal apply configuration: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("unmarshal apply configuration: %w", err)
	}

	return out, nil
}
