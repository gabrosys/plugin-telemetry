//go:build e2e

package admission_test

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	policyName     = "plugin-telemetry-identities"
	clusterName    = "adm"
	pluginAccount  = "plugin-telemetry"
	pluginNS       = "cnpg-system"
	grantName      = "plugin-telemetry-admission-e2e"
	tokenGrantName = "plugin-telemetry-admission-token-e2e"
	throwawayName  = "throwaway-telemetry"
	telemetryName  = "adm-telemetry"
	telemetryToken = "adm-telemetry-token"
	otherToken     = "other-token"
	timeout        = 2 * time.Minute
)

var (
	dryRun      = []string{metav1.DryRunAll}
	identities  = []string{"secrets", "serviceaccounts"}
	writeVerbs  = []string{"create", "update", "patch", "delete", "deletecollection"}
	pluginUser  = framework.ServiceAccountUser(pluginNS, pluginAccount)
	secretCheck = authorizationv1.ResourceAttributes{Verb: "create", Resource: "secrets"}
)

func tokenSecret(namespace, name, account string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        name,
			Annotations: map[string]string{corev1.ServiceAccountNameKey: account},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}
}

func serviceAccount(namespace, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// denied matches a Forbidden error raised by the policy, which RBAC denials lack.
func denied() OmegaMatcher {
	return SatisfyAll(Satisfy(apierrors.IsForbidden), MatchError(ContainSubstring(policyName)))
}

var _ = Describe("admission policy", Ordered, ContinueOnFailure, func() {
	var (
		c      client.Client
		plugin *kubernetes.Clientset
		cfg    *rest.Config
		ns     string
	)

	BeforeAll(func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())

		namespace, err := framework.CreateNamespace(ctx, c, "admission")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
		})
		ns = namespace.Name

		Expect(c.Create(ctx, framework.NewCluster(ns, clusterName, env.PostgresImage))).To(Succeed())

		role := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: grantName},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""},
				Resources: identities,
				Verbs:     writeVerbs,
			}},
		}
		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: grantName},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: grantName},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: pluginNS, Name: pluginAccount}},
		}
		for _, obj := range []client.Object{role, binding} {
			Expect(c.Create(ctx, obj)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(c.Delete(ctx, obj))).To(Succeed())
			})
		}

		plugin, err = framework.Impersonate(cfg, pluginUser)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega, ctx SpecContext) {
			allowed, err := framework.CanI(ctx, cfg, pluginUser, secretCheck)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(allowed).To(BeTrue())
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())

		Eventually(func(ctx SpecContext) error {
			return c.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName}, &corev1.ServiceAccount{})
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())
	})

	createToken := func(ctx SpecContext, namespace string, secret *corev1.Secret) error {
		_, err := plugin.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{DryRun: dryRun})

		return err
	}

	It("denies a token Secret for the namespace's default account", func(ctx SpecContext) {
		Expect(createToken(ctx, ns, tokenSecret(ns, "default-token", "default"))).To(denied())
	})

	It("denies a token Secret for CNPG's adm account", func(ctx SpecContext) {
		Expect(createToken(ctx, ns, tokenSecret(ns, "adm-token", clusterName))).To(denied())
	})

	It("denies a token Secret for kube-system/default", func(ctx SpecContext) {
		Expect(createToken(ctx, "kube-system", tokenSecret("kube-system", "default-token", "default"))).To(denied())
	})

	It("denies an Opaque Secret named adm-telemetry-token", func(ctx SpecContext) {
		secret := tokenSecret(ns, telemetryToken, telemetryName)
		secret.Type = corev1.SecretTypeOpaque
		Expect(createToken(ctx, ns, secret)).To(denied())
	})

	It("denies a token Secret whose name isn't <account>-token", func(ctx SpecContext) {
		Expect(createToken(ctx, ns, tokenSecret(ns, otherToken, telemetryName))).To(denied())
	})

	It("denies a ServiceAccount not ending in -telemetry", func(ctx SpecContext) {
		_, err := plugin.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, "other"), metav1.CreateOptions{DryRun: dryRun})
		Expect(err).To(denied())
	})

	It("allows the ServiceAccount adm-telemetry and the Secret adm-telemetry-token", func(ctx SpecContext) {
		_, err := plugin.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, telemetryName), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = plugin.CoreV1().Secrets(ns).Create(ctx, tokenSecret(ns, telemetryToken, telemetryName), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega, ctx SpecContext) {
			var secret corev1.Secret
			g.Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: telemetryToken}, &secret)).To(Succeed())
			g.Expect(secret.Data[corev1.ServiceAccountTokenKey]).NotTo(BeEmpty())
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())
	})

	It("denies an update that points adm-telemetry-token at another account", func(ctx SpecContext) {
		var secret corev1.Secret
		Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: telemetryToken}, &secret)).To(Succeed())
		secret.Annotations[corev1.ServiceAccountNameKey] = clusterName

		_, err := plugin.CoreV1().Secrets(ns).Update(ctx, &secret, metav1.UpdateOptions{DryRun: dryRun})
		Expect(err).To(denied())
	})

	It("denies deleting a Secret or ServiceAccount it may not write, one by one and as a collection", func(ctx SpecContext) {
		plain := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "plain"}}
		Expect(c.Create(ctx, plain)).To(Succeed())

		deleteOpts := metav1.DeleteOptions{DryRun: dryRun}
		Expect(plugin.CoreV1().Secrets(ns).Delete(ctx, "plain", deleteOpts)).To(denied())
		Expect(plugin.CoreV1().ServiceAccounts(ns).Delete(ctx, clusterName, deleteOpts)).To(denied())
		Expect(plugin.CoreV1().Secrets(ns).DeleteCollection(ctx, deleteOpts, metav1.ListOptions{})).To(denied())
		Expect(plugin.CoreV1().ServiceAccounts(ns).DeleteCollection(ctx, deleteOpts, metav1.ListOptions{})).To(denied())
	})

	It("allows deleting adm-telemetry-token and adm-telemetry", func(ctx SpecContext) {
		Expect(plugin.CoreV1().Secrets(ns).Delete(ctx, telemetryToken, metav1.DeleteOptions{})).To(Succeed())
		Expect(plugin.CoreV1().ServiceAccounts(ns).Delete(ctx, telemetryName, metav1.DeleteOptions{})).To(Succeed())
	})

	It("denies the plugin's account a TokenRequest for a -telemetry account", func(ctx SpecContext) {
		grant := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: tokenGrantName},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""},
				Resources: []string{"serviceaccounts/token"},
				Verbs:     []string{"create"},
			}},
		}
		tokenBinding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: tokenGrantName},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: tokenGrantName},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: pluginNS, Name: pluginAccount}},
		}
		for _, obj := range []client.Object{grant, tokenBinding} {
			Expect(c.Create(ctx, obj)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(c.Delete(ctx, obj))).To(Succeed())
			})
		}

		throwaway := serviceAccount(ns, throwawayName)
		Expect(c.Create(ctx, throwaway)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, throwaway))).To(Succeed())
		})

		Eventually(func(g Gomega, ctx SpecContext) {
			allowed, err := framework.CanI(ctx, cfg, pluginUser, authorizationv1.ResourceAttributes{
				Verb: "create", Resource: "serviceaccounts", Subresource: "token", Namespace: ns, Name: throwawayName,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(allowed).To(BeTrue())
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())

		request := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: ptr.To(int64(600))}}
		_, err := plugin.CoreV1().ServiceAccounts(ns).CreateToken(ctx, throwawayName, request, metav1.CreateOptions{})
		Expect(err).To(denied())
	})

	It("leaves other users alone", func(ctx SpecContext) {
		opaque := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "opaque"}, Type: corev1.SecretTypeOpaque}
		Expect(c.Create(ctx, opaque)).To(Succeed())
	})
})
