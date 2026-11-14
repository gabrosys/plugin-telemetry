package operator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

// discoveryServer answers the discovery calls NewManager makes to learn which kinds are namespaced.
func discoveryServer() *httptest.Server {
	GinkgoHelper()
	namespaced := func(gv string, kinds map[string]string) metav1.APIResourceList {
		list := metav1.APIResourceList{GroupVersion: gv}
		for name, kind := range kinds {
			list.APIResources = append(list.APIResources,
				metav1.APIResource{Name: name, Kind: kind, SingularName: strings.ToLower(kind), Namespaced: true})
		}

		return list
	}
	const rbacGV = "rbac.authorization.k8s.io/v1"
	responses := map[string]any{
		"/api": metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}},
		"/apis": metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList"}, Groups: []metav1.APIGroup{{
			Name:             "rbac.authorization.k8s.io",
			Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: rbacGV, Version: "v1"}},
			PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: rbacGV, Version: "v1"},
		}}},
		"/api/v1":         namespaced("v1", map[string]string{"serviceaccounts": "ServiceAccount", "secrets": "Secret"}),
		"/apis/" + rbacGV: namespaced(rbacGV, map[string]string{"roles": "Role", "rolebindings": "RoleBinding"}),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			http.NotFound(w, r)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	DeferCleanup(srv.Close)

	return srv
}

var _ = Describe("NewManager", func() {
	It("builds a manager with no listeners and no leader election", func() {
		scheme, err := operator.NewScheme()
		Expect(err).NotTo(HaveOccurred())
		opts := operator.ManagerOptions(scheme)

		Expect(opts.LeaderElection).To(BeFalse())
		Expect(opts.Metrics.BindAddress).To(Equal("0"))
		Expect(opts.HealthProbeBindAddress).To(Equal("0"))
		Expect(opts.PprofBindAddress).To(Equal("0"))
		Expect(opts.Scheme).To(BeIdenticalTo(scheme))
		Expect(opts.Cache.ReaderFailOnMissingInformer).To(BeTrue())

		mgr, err := operator.NewManager(&rest.Config{Host: discoveryServer().URL})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.GetCache()).NotTo(BeNil())
		Expect(mgr.GetClient()).NotTo(BeNil())
	})

	It("registers core, rbac, CNPG and telemetry types", func() {
		scheme, err := operator.NewScheme()
		Expect(err).NotTo(HaveOccurred())

		for _, obj := range []client.Object{
			&corev1.Secret{}, &corev1.ServiceAccount{}, &corev1.ConfigMap{},
			&rbacv1.Role{}, &rbacv1.RoleBinding{},
			&apiv1.Cluster{},
			&v1alpha1.TelemetryExporter{}, &v1alpha1.MetricQuerySet{},
		} {
			gvks, _, err := scheme.ObjectKinds(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(gvks).NotTo(BeEmpty())
		}
	})

	It("limits ServiceAccounts, Roles, RoleBindings and Secrets to the managed-by label and caches the rest unrestricted", func() {
		scheme, err := operator.NewScheme()
		Expect(err).NotTo(HaveOccurred())
		opts := operator.ManagerOptions(scheme)

		limited := map[string]bool{}
		for obj, by := range opts.Cache.ByObject {
			gvks, _, err := scheme.ObjectKinds(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(by.Label).NotTo(BeNil(), gvks[0].Kind)
			Expect(by.Label.Matches(labels.Set{"app.kubernetes.io/managed-by": "plugin-telemetry"})).To(BeTrue())
			Expect(by.Label.Matches(labels.Set{"app.kubernetes.io/managed-by": "someone-else"})).To(BeFalse())
			Expect(by.Label.Matches(labels.Set{})).To(BeFalse())
			Expect(by.Namespaces).To(BeEmpty())
			limited[gvks[0].Kind] = true
		}
		Expect(limited).To(Equal(map[string]bool{"ServiceAccount": true, "Role": true, "RoleBinding": true, "Secret": true}))
		Expect(opts.Cache.DefaultNamespaces).To(BeEmpty())
		Expect(opts.Cache.DefaultLabelSelector).To(BeNil())
	})
})
