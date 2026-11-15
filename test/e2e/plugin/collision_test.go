//go:build e2e

package plugin_test

import (
	"fmt"
	"os"
	"slices"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("security: a CloudNativePG Cluster named like an identity", func() {
	It("gets its objects from the plugin, which backs off without a fight", func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, _, err := framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(environmentTimeout).WithPolling(environmentPollInterval).Should(BeTrue())

		namespace, err := framework.CreateNamespace(ctx, c, "plugin-collision")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
		})
		ns := namespace.Name
		key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }

		// Deleted before the namespace: a namespace deleted around a live Cluster stays Terminating.
		create := func(cluster *apiv1.Cluster) {
			GinkgoHelper()
			Expect(c.Create(ctx, cluster)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(c.Delete(ctx, cluster))).To(Succeed())
				Eventually(func(ctx SpecContext) bool {
					return apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(cluster), &apiv1.Cluster{}))
				}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(BeTrue())
			}, NodeTimeout(4*time.Minute))
		}
		status := func(g Gomega, ctx SpecContext, name string) operator.ClusterStatus {
			var cl apiv1.Cluster
			g.Expect(c.Get(ctx, key(name), &cl)).To(Succeed())
			st, ok, err := framework.PluginStatus(&cl)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "the Cluster has no status from the plugin")

			return st
		}

		const owner = "collide"
		account := metadata.ServiceAccountName(owner)
		create(framework.PeerCluster(ns, owner, env.PostgresImage))
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(status(g, ctx, owner).Identity).To(Equal("ready"))
		}).WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(specPoll).Should(Succeed())
		var sa corev1.ServiceAccount
		Expect(c.Get(ctx, key(account), &sa)).To(Succeed())

		create(framework.NewCluster(ns, account, env.PostgresImage))
		Eventually(func(ctx SpecContext) (bool, error) {
			return framework.ClusterReady(ctx, c, key(account))
		}).WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(5 * time.Second).Should(BeTrue())

		Eventually(func(ctx SpecContext) bool {
			return apierrors.IsNotFound(c.Get(ctx, key(metadata.TokenSecretName(owner)), &corev1.Secret{}))
		}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(BeTrue(), "the legacy token still exists")

		// The Role and RoleBinding keep the plugin's labels: with CloudNativePG's rules in, a label edit is an
		// update the RBAC escalation check refuses.
		var used corev1.ServiceAccount
		Expect(c.Get(ctx, key(account), &used)).To(Succeed())
		Expect(used.UID).To(Equal(sa.UID), "the ServiceAccount the instance Pods use was recreated")
		Expect(used.Labels).NotTo(HaveKeyWithValue(identity.ManagedByLabel, identity.ManagedBy))
		Expect(used.Labels).NotTo(HaveKey(identity.ClusterLabel))
		for _, obj := range []client.Object{&used, &rbacv1.Role{}, &rbacv1.RoleBinding{}} {
			Expect(c.Get(ctx, key(account), obj)).To(Succeed())
			Expect(obj.GetOwnerReferences()).NotTo(ContainElement(HaveField("Name", owner)), "%T", obj)
		}

		// CloudNativePG asks for the status on a reconcile, which an annotation edit triggers.
		touchCluster(ctx, c, key(owner), "collision")
		Eventually(func(g Gomega, ctx SpecContext) {
			st := status(g, ctx, owner)
			g.Expect(st.Identity).To(Equal("pending"))
			g.Expect(st.Problems).To(ContainElement(fmt.Sprintf(
				"Cluster %q has the name of this Cluster's identity objects: the plugin manages none", account)))
		}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())

		var role rbacv1.Role
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(c.Get(ctx, key(account), &role)).To(Succeed())
			g.Expect(slices.ContainsFunc(role.Rules, func(r rbacv1.PolicyRule) bool {
				return slices.Contains(r.Resources, "clusters/status")
			})).To(BeTrue(), "the Role lacks CloudNativePG's rules")
		}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())
		Consistently(func(g Gomega, ctx SpecContext) {
			var now rbacv1.Role
			g.Expect(c.Get(ctx, key(account), &now)).To(Succeed())
			g.Expect(now.Rules).To(Equal(role.Rules))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(specPoll).Should(Succeed())
	}, SpecTimeout(2*clusterTimeout+5*time.Minute))
})
