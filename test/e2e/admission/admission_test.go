//go:build e2e

package admission_test

import (
	"encoding/json"
	"os"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	policyName        = "plugin-telemetry-identities"
	rolesPolicy       = "plugin-telemetry-roles"
	reservedPolicy    = "plugin-telemetry-reserved-names"
	clusterName       = "adm"
	exporterName      = "default"
	pluginAccount     = "plugin-telemetry"
	pluginNS          = "cnpg-system"
	grantName         = "plugin-telemetry-admission-e2e"
	tokenGrantName    = "plugin-telemetry-admission-token-e2e"
	escalateGrantName = "plugin-telemetry-admission-escalate-e2e"
	throwawayName     = "throwaway-telemetry"
	telemetryName     = "adm-telemetry"
	telemetryDotName  = "adm.telemetry"
	telemetryToken    = "adm-telemetry-token"
	otherToken        = "other-token"
	probeName         = "probe-telemetry"
	chainName         = "chain-telemetry"
	clusterRoleName   = "admission-e2e-telemetry"
	timeout           = 2 * time.Minute

	// Parts of the roles policy's messages, one per rule.
	nameRule     = "named *-telemetry"
	verbsRule    = "only get, list and watch"
	roleRefRule  = "only to the Role of the same name"
	subjectsRule = "only the ServiceAccount of the same name"
)

var (
	dryRun      = []string{metav1.DryRunAll}
	identities  = []string{"secrets", "serviceaccounts"}
	writeVerbs  = []string{"create", "update", "patch", "delete", "deletecollection"}
	pluginUser  = framework.ServiceAccountUser(pluginNS, pluginAccount)
	secretCheck = authorizationv1.ResourceAttributes{Verb: "create", Resource: "secrets"}

	readConfigMaps = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}
	createSecrets  = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create"}}
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

// deniedBy matches a Forbidden error raised by the roles policy for the rule named by part of its message.
func deniedBy(rule string) OmegaMatcher {
	return SatisfyAll(Satisfy(apierrors.IsForbidden),
		MatchError(ContainSubstring(rolesPolicy)), MatchError(ContainSubstring(rule)))
}

func pooler(namespace, name string) *apiv1.Pooler {
	return &apiv1.Pooler{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: apiv1.PoolerSpec{
			Cluster:   apiv1.LocalObjectReference{Name: clusterName},
			Instances: ptr.To(int32(1)),
			PgBouncer: &apiv1.PgBouncerSpec{PoolMode: apiv1.PgBouncerPoolModeSession},
		},
	}
}

// reserved matches an Invalid error raised by the reserved-names policy.
func reserved() OmegaMatcher {
	return SatisfyAll(Satisfy(apierrors.IsInvalid), MatchError(ContainSubstring(reservedPolicy)))
}

func serviceAccountSubject(namespace, name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: namespace, Name: name}
}

func roleRef(kind, name string) rbacv1.RoleRef {
	return rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: kind, Name: name}
}

// grantPlugin gives the plugin's account rule in every namespace until the current node ends.
func grantPlugin(ctx SpecContext, c client.Client, name string, rule rbacv1.PolicyRule) {
	GinkgoHelper()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: []rbacv1.PolicyRule{*rule.DeepCopy()}}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    roleRef("ClusterRole", name),
		Subjects:   []rbacv1.Subject{serviceAccountSubject(pluginNS, pluginAccount)},
	}
	for _, obj := range []client.Object{role, binding} {
		Expect(c.Create(ctx, obj)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, obj))).To(Succeed())
		})
	}
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

		grantPlugin(ctx, c, grantName, rbacv1.PolicyRule{APIGroups: []string{""}, Resources: identities, Verbs: writeVerbs})

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
		grantPlugin(ctx, c, tokenGrantName, rbacv1.PolicyRule{
			APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{"create"},
		})

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

	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }
	meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: ns, Name: name} }

	desired := func(ctx SpecContext, refs clusterconfig.Refs) identity.Objects {
		GinkgoHelper()
		var cluster apiv1.Cluster
		Expect(c.Get(ctx, key(clusterName), &cluster)).To(Succeed())
		want, err := identity.Desired(&cluster, refs)
		Expect(err).NotTo(HaveOccurred())

		return want
	}

	// shape returns the identity's Role and RoleBinding for adm without the plugin's labels and owner reference,
	// so the deployed plugin, which removes the identity of a Cluster that doesn't use it, never sees them.
	shape := func(ctx SpecContext, refs clusterconfig.Refs) (*rbacv1.Role, *rbacv1.RoleBinding) {
		GinkgoHelper()
		want := desired(ctx, refs)
		for _, obj := range []client.Object{want.Role, want.RoleBinding} {
			obj.SetLabels(nil)
			obj.SetOwnerReferences(nil)
		}
		want.Role.TypeMeta = metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"}
		want.RoleBinding.TypeMeta = metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "RoleBinding"}

		return want.Role, want.RoleBinding
	}

	// patchAs patches a Role or RoleBinding as the plugin, with its field manager.
	patchAs := func(ctx SpecContext, obj client.Object, pt types.PatchType, data []byte, opts metav1.PatchOptions) error {
		var err error
		opts.FieldManager = identity.FieldOwner
		if _, ok := obj.(*rbacv1.Role); ok {
			_, err = plugin.RbacV1().Roles(ns).Patch(ctx, obj.GetName(), pt, data, opts)
		} else {
			_, err = plugin.RbacV1().RoleBindings(ns).Patch(ctx, obj.GetName(), pt, data, opts)
		}

		return err
	}

	// apply server-side applies a Role or RoleBinding as the plugin does, with force.
	apply := func(ctx SpecContext, obj client.Object, dry []string) error {
		data, err := json.Marshal(obj)
		Expect(err).NotTo(HaveOccurred())

		return patchAs(ctx, obj, types.ApplyPatchType, data, metav1.PatchOptions{Force: ptr.To(true), DryRun: dry})
	}

	// disown removes the owner references of a Role or RoleBinding as the plugin's back-off does, by a merge
	// patch with optimistic lock.
	disown := func(ctx SpecContext, obj, orig client.Object) error {
		obj.SetOwnerReferences(nil)
		patch := client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})
		data, err := patch.Data(obj)
		Expect(err).NotTo(HaveOccurred())

		return patchAs(ctx, obj, patch.Type(), data, metav1.PatchOptions{})
	}

	createRole := func(ctx SpecContext, role *rbacv1.Role, dry []string) error {
		_, err := plugin.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{DryRun: dry})

		return err
	}

	// createBinding dry-runs the creation as the plugin, retrying while RBAC can't yet resolve the roleRef.
	createBinding := func(ctx SpecContext, binding *rbacv1.RoleBinding) AsyncAssertion {
		return Eventually(func(ctx SpecContext) error {
			_, err := plugin.RbacV1().RoleBindings(ns).Create(ctx, binding, metav1.CreateOptions{DryRun: dryRun})

			return err
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second)
	}

	baseRefs := clusterconfig.Refs{Cluster: clusterName, Exporter: exporterName}

	It("allows the identity's Role and RoleBinding for adm, applied as the plugin applies them", func(ctx SpecContext) {
		role, binding := shape(ctx, baseRefs)
		Expect(apply(ctx, role, nil)).To(Succeed())
		Expect(apply(ctx, binding, nil)).To(Succeed())
	})

	It("allows applying the identity's Role again with a Secret to read", func(ctx SpecContext) {
		refs := baseRefs
		refs.Secrets = []string{"adm-app"}
		role, _ := shape(ctx, refs)
		Expect(apply(ctx, role, nil)).To(Succeed())
	})

	It("allows a Role named *-telemetry with no rules", func(ctx SpecContext) {
		Expect(createRole(ctx, &rbacv1.Role{ObjectMeta: meta(probeName)}, nil)).To(Succeed())
	})

	It("denies a Role granting create on secrets, the first step to a token for any account", func(ctx SpecContext) {
		role := &rbacv1.Role{ObjectMeta: meta(chainName), Rules: []rbacv1.PolicyRule{createSecrets, readConfigMaps}}
		Expect(createRole(ctx, role, dryRun)).To(deniedBy(verbsRule))
	})

	It("denies a Role not named *-telemetry", func(ctx SpecContext) {
		role := &rbacv1.Role{ObjectMeta: meta("chain"), Rules: []rbacv1.PolicyRule{readConfigMaps}}
		Expect(createRole(ctx, role, dryRun)).To(deniedBy(nameRule))
	})

	It("denies an update that adds a write verb to the identity's Role", func(ctx SpecContext) {
		role, _ := shape(ctx, baseRefs)
		role.Rules = append(role.Rules, createSecrets)
		Expect(apply(ctx, role, dryRun)).To(deniedBy(verbsRule))
	})

	It("denies a RoleBinding to another Role", func(ctx SpecContext) {
		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(probeName),
			RoleRef:    roleRef("Role", telemetryName),
			Subjects:   []rbacv1.Subject{serviceAccountSubject(ns, probeName)},
		}
		createBinding(ctx, binding).Should(deniedBy(roleRefRule))
	})

	It("denies a RoleBinding to a ClusterRole", func(ctx SpecContext) {
		// The ClusterRole has the binding's name and a rule the plugin holds, so only the kind is wrong.
		clusterRole := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName}, Rules: []rbacv1.PolicyRule{createSecrets},
		}
		Expect(c.Create(ctx, clusterRole)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, clusterRole))).To(Succeed())
		})

		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(clusterRoleName),
			RoleRef:    roleRef("ClusterRole", clusterRoleName),
			Subjects:   []rbacv1.Subject{serviceAccountSubject(ns, clusterRoleName)},
		}
		createBinding(ctx, binding).Should(deniedBy(roleRefRule))
	})

	It("denies a RoleBinding with a second subject", func(ctx SpecContext) {
		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(probeName),
			RoleRef:    roleRef("Role", probeName),
			Subjects:   []rbacv1.Subject{serviceAccountSubject(ns, probeName), serviceAccountSubject(ns, "default")},
		}
		createBinding(ctx, binding).Should(deniedBy(subjectsRule))
	})

	It("denies a RoleBinding whose subject is in another namespace", func(ctx SpecContext) {
		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(probeName),
			RoleRef:    roleRef("Role", probeName),
			Subjects:   []rbacv1.Subject{serviceAccountSubject("kube-system", probeName)},
		}
		createBinding(ctx, binding).Should(deniedBy(subjectsRule))
	})

	It("allows an update that keeps CloudNativePG's write rules in a Role, as the collision back-off does", func(ctx SpecContext) {
		var role rbacv1.Role
		Expect(c.Get(ctx, key(telemetryName), &role)).To(Succeed())
		// CloudNativePG v1.30.1 pkg/specs/roles.go:95-111, a rule the plugin doesn't hold.
		role.Rules = append(role.Rules, rbacv1.PolicyRule{
			APIGroups:     []string{apiv1.SchemeGroupVersion.Group},
			Resources:     []string{"clusters/status"},
			Verbs:         []string{"get", "patch", "update", "watch"},
			ResourceNames: []string{clusterName},
		})
		role.OwnerReferences = desired(ctx, baseRefs).Role.OwnerReferences
		Expect(c.Update(ctx, &role)).To(Succeed())

		Expect(disown(ctx, role.DeepCopy(), &role)).To(Succeed())
	})

	It("allows an update that keeps a RoleBinding's subjects, as the collision back-off does", func(ctx SpecContext) {
		var binding rbacv1.RoleBinding
		Expect(c.Get(ctx, key(telemetryName), &binding)).To(Succeed())
		// A second subject, which only an unchanged list lets through.
		binding.Subjects = append(binding.Subjects, serviceAccountSubject(ns, clusterName))
		binding.OwnerReferences = desired(ctx, baseRefs).RoleBinding.OwnerReferences
		Expect(c.Update(ctx, &binding)).To(Succeed())

		Expect(disown(ctx, binding.DeepCopy(), &binding)).To(Succeed())
	})

	It("denies a Role with * verbs", func(ctx SpecContext) {
		// RBAC refuses a * verb before admission unless the plugin may escalate.
		grantPlugin(ctx, c, escalateGrantName, rbacv1.PolicyRule{
			APIGroups: []string{rbacv1.GroupName}, Resources: []string{"roles"}, Verbs: []string{"escalate"},
		})
		Eventually(func(g Gomega, ctx SpecContext) {
			allowed, err := framework.CanI(ctx, cfg, pluginUser, authorizationv1.ResourceAttributes{
				Verb: "escalate", Group: rbacv1.GroupName, Resource: "roles", Namespace: ns,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(allowed).To(BeTrue())
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())

		role := &rbacv1.Role{ObjectMeta: meta(chainName), Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"*"},
		}}}
		Expect(createRole(ctx, role, dryRun)).To(deniedBy(verbsRule))
	})

	It("denies deleting a Role or RoleBinding not named *-telemetry", func(ctx SpecContext) {
		// CloudNativePG names the Role and RoleBinding of a Cluster after it.
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(c.Get(ctx, key(clusterName), &rbacv1.Role{})).To(Succeed())
			g.Expect(c.Get(ctx, key(clusterName), &rbacv1.RoleBinding{})).To(Succeed())
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())

		deleteOpts := metav1.DeleteOptions{DryRun: dryRun}
		Expect(plugin.RbacV1().Roles(ns).Delete(ctx, clusterName, deleteOpts)).To(deniedBy(nameRule))
		Expect(plugin.RbacV1().RoleBindings(ns).Delete(ctx, clusterName, deleteOpts)).To(deniedBy(nameRule))
	})

	It("allows deleting the Roles and RoleBindings named *-telemetry", func(ctx SpecContext) {
		Expect(plugin.RbacV1().RoleBindings(ns).Delete(ctx, telemetryName, metav1.DeleteOptions{})).To(Succeed())
		Expect(plugin.RbacV1().Roles(ns).Delete(ctx, telemetryName, metav1.DeleteOptions{})).To(Succeed())
		Expect(plugin.RbacV1().Roles(ns).Delete(ctx, probeName, metav1.DeleteOptions{})).To(Succeed())
	})

	It("denies any user a Pooler named adm.telemetry", func(ctx SpecContext) {
		Expect(c.Create(ctx, pooler(ns, telemetryDotName), client.DryRunAll)).To(reserved())
	})

	It("allows a Pooler named adm-pooler", func(ctx SpecContext) {
		Expect(c.Create(ctx, pooler(ns, "adm-pooler"), client.DryRunAll)).To(Succeed())
	})

	It("the API server rejects a Cluster and a Pooler whose spec.serviceAccountName ends in .telemetry, before admission", func(ctx SpecContext) {
		rejected := SatisfyAll(Satisfy(apierrors.IsInvalid),
			MatchError(ContainSubstring("spec.serviceAccountName")), Not(MatchError(ContainSubstring(reservedPolicy))))

		cluster := framework.NewCluster(ns, "adm-sa", "unused")
		cluster.Spec.ServiceAccountName = telemetryDotName
		Expect(c.Create(ctx, cluster, client.DryRunAll)).To(rejected)

		p := pooler(ns, "adm-sa-pooler")
		p.Spec.ServiceAccountName = telemetryDotName
		Expect(c.Create(ctx, p, client.DryRunAll)).To(rejected)
	})

	It("leaves other users alone", func(ctx SpecContext) {
		opaque := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "opaque"}, Type: corev1.SecretTypeOpaque}
		Expect(c.Create(ctx, opaque)).To(Succeed())
	})
})
