//go:build e2e

package admission_test

import (
	"encoding/json"
	"os"
	"strings"
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
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
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
	throwawayName     = "throwaway.telemetry"
	telemetryName     = "adm.telemetry"
	oldTelemetryName  = "adm-telemetry"
	telemetryToken    = "adm.telemetry-token"
	otherToken        = "other-token"
	dashedName        = "probe-telemetry"
	probeName         = "probe.telemetry"
	chainName         = "chain.telemetry"
	clusterRoleName   = "admission-e2e.telemetry"
	legacyName        = "legacy.telemetry"
	timeout           = 2 * time.Minute

	// Parts of the roles policy's messages, one per rule.
	nameRule     = "named *.telemetry may be"
	verbsRule    = "only get, list and watch"
	roleRefRule  = "only to the Role of its own name"
	subjectsRule = "only the ServiceAccount of its own name"

	// Parts of the identities policy's messages.
	writeRule   = "only ServiceAccounts named *.telemetry"
	requestRule = "may not be requested (TokenRequest)"
)

var (
	labelPatch    = []byte(`{"metadata":{"labels":{"label-only":"patch"}}}`)
	dryRun        = []string{metav1.DryRunAll}
	identities    = []string{"secrets", "serviceaccounts"}
	writeVerbs    = []string{"create", "update", "patch", "delete", "deletecollection"}
	pluginUser    = framework.ServiceAccountUser(pluginNS, pluginAccount)
	pluginSubject = serviceAccountSubject(pluginNS, pluginAccount)

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

// denied matches a Forbidden error raised by the identities policy for the rule named by part of its message.
func denied(rule string) OmegaMatcher {
	return SatisfyAll(Satisfy(apierrors.IsForbidden),
		MatchError(ContainSubstring(policyName)), MatchError(ContainSubstring(rule)))
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

// denialText returns what a policy wrote in a denial, after the API server's own "denied request:" prefix.
func denialText(err error) string {
	GinkgoHelper()
	Expect(err).To(HaveOccurred())
	_, text, found := strings.Cut(err.Error(), "denied request: ")
	Expect(found).To(BeTrue(), "not a policy denial: %v", err)

	return text
}

func serviceAccountSubject(namespace, name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: namespace, Name: name}
}

func roleRef(kind, name string) rbacv1.RoleRef {
	return rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: kind, Name: name}
}

// grant gives subject rule in namespace, in every namespace when it is empty, until the current node ends,
// and waits until the API server authorizes every group, resource, verb and name the rule lists.
func grant(ctx SpecContext, c client.Client, cfg *rest.Config, namespace, name string, subject rbacv1.Subject, rule rbacv1.PolicyRule) {
	GinkgoHelper()
	Expect(subject.Kind).To(Equal(rbacv1.ServiceAccountKind), "grant waits as a ServiceAccount user")
	checks := ruleAttributes(namespace, rule)
	Expect(checks).NotTo(BeEmpty(), "the rule lists nothing to wait for")
	rules := []rbacv1.PolicyRule{*rule.DeepCopy()}
	var objs []client.Object
	if namespace == "" {
		objs = []client.Object{
			&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules},
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				RoleRef:    roleRef("ClusterRole", name),
				Subjects:   []rbacv1.Subject{subject},
			},
		}
	} else {
		meta := metav1.ObjectMeta{Namespace: namespace, Name: name}
		objs = []client.Object{
			&rbacv1.Role{ObjectMeta: meta, Rules: rules},
			&rbacv1.RoleBinding{ObjectMeta: meta, RoleRef: roleRef("Role", name), Subjects: []rbacv1.Subject{subject}},
		}
	}
	for _, obj := range objs {
		Expect(c.Create(ctx, obj)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, obj))).To(Succeed())
		})
	}

	user := framework.ServiceAccountUser(subject.Namespace, subject.Name)
	for _, attrs := range checks {
		Eventually(func(g Gomega, ctx SpecContext) {
			allowed, err := framework.CanI(ctx, cfg, user, attrs)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(allowed).To(BeTrue(), "%s may not yet %s %s", user, attrs.Verb, attrs.Resource)
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())
	}
}

// ruleAttributes lists the access checks that tell a rule is in force: one per group, resource, verb and name.
func ruleAttributes(namespace string, rule rbacv1.PolicyRule) []authorizationv1.ResourceAttributes {
	names := rule.ResourceNames
	if len(names) == 0 {
		names = []string{""}
	}
	var checks []authorizationv1.ResourceAttributes
	for _, group := range rule.APIGroups {
		for _, resource := range rule.Resources {
			base, subresource, _ := strings.Cut(resource, "/")
			for _, verb := range rule.Verbs {
				for _, name := range names {
					checks = append(checks, authorizationv1.ResourceAttributes{
						Namespace: namespace, Verb: verb, Group: group, Resource: base, Subresource: subresource, Name: name,
					})
				}
			}
		}
	}

	return checks
}

var _ = Describe("admission policy", Ordered, ContinueOnFailure, func() {
	var (
		c      client.Client
		plugin *kubernetes.Clientset
		cfg    *rest.Config
		ns     string
		image  string
	)

	BeforeAll(func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		image = env.PostgresImage
		c, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())

		namespace, err := framework.CreateNamespace(ctx, c, "admission")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
		})
		ns = namespace.Name

		Expect(c.Create(ctx, framework.NewCluster(ns, clusterName, env.PostgresImage))).To(Succeed())

		grant(ctx, c, cfg, "", grantName, pluginSubject, rbacv1.PolicyRule{APIGroups: []string{""}, Resources: identities, Verbs: writeVerbs})

		plugin, err = framework.Impersonate(cfg, pluginUser)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(ctx SpecContext) error {
			return c.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName}, &corev1.ServiceAccount{})
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())
	})

	dryRunCreateSecret := func(ctx SpecContext, namespace string, secret *corev1.Secret) error {
		_, err := plugin.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{DryRun: dryRun})

		return err
	}

	It("denies a token Secret for the namespace's default account", func(ctx SpecContext) {
		Expect(dryRunCreateSecret(ctx, ns, tokenSecret(ns, "default-token", "default"))).To(denied(writeRule))
	})

	It("denies a token Secret for CNPG's adm account", func(ctx SpecContext) {
		Expect(dryRunCreateSecret(ctx, ns, tokenSecret(ns, "adm-token", clusterName))).To(denied(writeRule))
	})

	It("denies a token Secret for kube-system/default", func(ctx SpecContext) {
		Expect(dryRunCreateSecret(ctx, "kube-system", tokenSecret("kube-system", "default-token", "default"))).To(denied(writeRule))
	})

	It("denies an Opaque Secret named adm.telemetry-token", func(ctx SpecContext) {
		secret := tokenSecret(ns, telemetryToken, telemetryName)
		secret.Type = corev1.SecretTypeOpaque
		Expect(dryRunCreateSecret(ctx, ns, secret)).To(denied(writeRule))
	})

	It("denies a token Secret whose name isn't <account>-token", func(ctx SpecContext) {
		Expect(dryRunCreateSecret(ctx, ns, tokenSecret(ns, otherToken, telemetryName))).To(denied(writeRule))
	})

	It("denies a ServiceAccount not ending in .telemetry", func(ctx SpecContext) {
		_, err := plugin.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, "other"), metav1.CreateOptions{DryRun: dryRun})
		Expect(err).To(denied(writeRule))
	})

	It("denies a ServiceAccount named adm-telemetry, the 0.4.0 name", func(ctx SpecContext) {
		_, err := plugin.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, oldTelemetryName), metav1.CreateOptions{DryRun: dryRun})
		Expect(err).To(denied(writeRule))
	})

	It("denies deleting the plugin's own ServiceAccount in cnpg-system", func(ctx SpecContext) {
		err := plugin.CoreV1().ServiceAccounts(pluginNS).Delete(ctx, pluginAccount, metav1.DeleteOptions{DryRun: dryRun})
		Expect(err).To(denied(writeRule))
	})

	It("allows the ServiceAccount adm.telemetry and the Secret adm.telemetry-token", func(ctx SpecContext) {
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

	It("refuses an apply pinned to a uid the stored ServiceAccount doesn't have, and one whose object is gone", func(ctx SpecContext) {
		applyPinned := func(name string) error {
			_, err := plugin.CoreV1().ServiceAccounts(ns).Apply(ctx,
				corev1ac.ServiceAccount(name, ns).WithUID("00000000-0000-0000-0000-000000000000"),
				metav1.ApplyOptions{FieldManager: identity.FieldOwner, Force: true, DryRun: dryRun})

			return err
		}

		err := applyPinned(telemetryName)
		Expect(err).To(Satisfy(apierrors.IsInvalid))
		Expect(err).To(MatchError(ContainSubstring("metadata.uid")))

		err = applyPinned("gone.telemetry")
		Expect(err).To(Satisfy(apierrors.IsConflict))
		Expect(err).To(MatchError(ContainSubstring("uid mismatch")))
	})

	It("refuses an apply whose body carries a stale resourceVersion, and accepts the stored one", func(ctx SpecContext) {
		var stored corev1.ServiceAccount
		Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: telemetryName}, &stored)).To(Succeed())

		applyAt := func(version string) error {
			_, err := plugin.CoreV1().ServiceAccounts(ns).Apply(ctx,
				corev1ac.ServiceAccount(telemetryName, ns).WithUID(stored.UID).WithResourceVersion(version),
				metav1.ApplyOptions{FieldManager: identity.FieldOwner, Force: true, DryRun: dryRun})

			return err
		}

		Expect(applyAt(stored.ResourceVersion)).To(Succeed())
		err := applyAt("1")
		Expect(err).To(Satisfy(apierrors.IsConflict))
		Expect(err).To(MatchError(ContainSubstring("the object has been modified")))
	})

	It("denies an update that points adm.telemetry-token at another account", func(ctx SpecContext) {
		var secret corev1.Secret
		Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: telemetryToken}, &secret)).To(Succeed())
		secret.Annotations[corev1.ServiceAccountNameKey] = clusterName

		_, err := plugin.CoreV1().Secrets(ns).Update(ctx, &secret, metav1.UpdateOptions{DryRun: dryRun})
		Expect(err).To(denied(writeRule))
	})

	It("allows a label-only patch of adm.telemetry-token", func(ctx SpecContext) {
		_, err := plugin.CoreV1().Secrets(ns).Patch(ctx, telemetryToken, types.MergePatchType, labelPatch,
			metav1.PatchOptions{DryRun: dryRun})
		Expect(err).NotTo(HaveOccurred())
	})

	It("denies repointing a token Secret of another account at a .telemetry account", func(ctx SpecContext) {
		// An admin made it, for an account the plugin may not write, under a name the plugin may.
		foreign := tokenSecret(ns, "legacy.telemetry-token", "default")
		Expect(c.Create(ctx, foreign)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, foreign))).To(Succeed())
		})

		repoint := []byte(`{"metadata":{"annotations":{"` + corev1.ServiceAccountNameKey + `":"legacy.telemetry"}}}`)
		_, err := plugin.CoreV1().Secrets(ns).Patch(ctx, foreign.Name, types.MergePatchType, repoint,
			metav1.PatchOptions{DryRun: dryRun})
		Expect(err).To(denied(writeRule))
	})

	It("denies deleting a Secret or ServiceAccount it may not write, one by one and as a collection", func(ctx SpecContext) {
		plain := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "plain"}}
		Expect(c.Create(ctx, plain)).To(Succeed())

		deleteOpts := metav1.DeleteOptions{DryRun: dryRun}
		Expect(plugin.CoreV1().Secrets(ns).Delete(ctx, "plain", deleteOpts)).To(denied(writeRule))
		Expect(plugin.CoreV1().ServiceAccounts(ns).Delete(ctx, clusterName, deleteOpts)).To(denied(writeRule))
		Expect(plugin.CoreV1().Secrets(ns).DeleteCollection(ctx, deleteOpts, metav1.ListOptions{})).To(denied(writeRule))
		Expect(plugin.CoreV1().ServiceAccounts(ns).DeleteCollection(ctx, deleteOpts, metav1.ListOptions{})).To(denied(writeRule))
	})

	It("allows deleting adm.telemetry-token and adm.telemetry", func(ctx SpecContext) {
		Expect(plugin.CoreV1().Secrets(ns).Delete(ctx, telemetryToken, metav1.DeleteOptions{})).To(Succeed())
		Expect(plugin.CoreV1().ServiceAccounts(ns).Delete(ctx, telemetryName, metav1.DeleteOptions{})).To(Succeed())
	})

	It("denies the plugin's account a TokenRequest for a .telemetry account", func(ctx SpecContext) {
		grant(ctx, c, cfg, ns, tokenGrantName, pluginSubject, rbacv1.PolicyRule{
			APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{"create"},
			ResourceNames: []string{throwawayName},
		})

		throwaway := serviceAccount(ns, throwawayName)
		Expect(c.Create(ctx, throwaway)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, throwaway))).To(Succeed())
		})

		request := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: ptr.To(int64(600))}}
		_, err := plugin.CoreV1().ServiceAccounts(ns).CreateToken(ctx, throwawayName, request, metav1.CreateOptions{})
		Expect(err).To(denied(requestRule))
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
		GinkgoHelper()
		data, err := json.Marshal(obj)
		Expect(err).NotTo(HaveOccurred())

		return patchAs(ctx, obj, types.ApplyPatchType, data, metav1.PatchOptions{Force: ptr.To(true), DryRun: dry})
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
		// RBAC may not yet resolve the new Role the binding refers to.
		Eventually(func(ctx SpecContext) error {
			err := apply(ctx, binding, nil)
			if err != nil && !apierrors.IsNotFound(err) {
				StopTrying("the RoleBinding apply failed").Wrap(err).Now()
			}

			return err
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())
	})

	It("allows applying the identity's Role again with a Secret to read", func(ctx SpecContext) {
		refs := baseRefs
		refs.Secrets = []string{"adm-app"}
		role, _ := shape(ctx, refs)
		Expect(apply(ctx, role, nil)).To(Succeed())
	})

	It("allows a Role named *.telemetry with no rules", func(ctx SpecContext) {
		Expect(createRole(ctx, &rbacv1.Role{ObjectMeta: meta(probeName)}, nil)).To(Succeed())
	})

	It("denies a Role granting create on secrets, the first step to a token for any account", func(ctx SpecContext) {
		role := &rbacv1.Role{ObjectMeta: meta(chainName), Rules: []rbacv1.PolicyRule{createSecrets, readConfigMaps}}
		Expect(createRole(ctx, role, dryRun)).To(deniedBy(verbsRule))
	})

	It("denies a Role not named *.telemetry", func(ctx SpecContext) {
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

	It("denies a RoleBinding whose subject is a User", func(ctx SpecContext) {
		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(probeName),
			RoleRef:    roleRef("Role", probeName),
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: probeName}},
		}
		createBinding(ctx, binding).Should(deniedBy(subjectsRule))
	})

	It("denies a RoleBinding whose ServiceAccount subject has no namespace", func(ctx SpecContext) {
		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(probeName),
			RoleRef:    roleRef("Role", probeName),
			Subjects:   []rbacv1.Subject{serviceAccountSubject("", probeName)},
		}
		createBinding(ctx, binding).Should(deniedBy(subjectsRule))
	})

	// The objects below are made by an admin, so they hold what the plugin's rules forbid.
	createAsAdmin := func(ctx SpecContext, obj client.Object) {
		GinkgoHelper()
		Expect(c.Create(ctx, obj)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, obj))).To(Succeed())
		})
	}
	It("denies a label-only patch of a Role that holds a write rule", func(ctx SpecContext) {
		role := &rbacv1.Role{ObjectMeta: meta(legacyName), Rules: []rbacv1.PolicyRule{createSecrets}}
		createAsAdmin(ctx, role)

		err := patchAs(ctx, role, types.MergePatchType, labelPatch, metav1.PatchOptions{DryRun: dryRun})
		Expect(err).To(deniedBy(verbsRule))
	})

	It("denies a label-only patch of a RoleBinding with a second subject", func(ctx SpecContext) {
		// An update resolves the Role the binding refers to, and the plugin holds its rule through the grant.
		createAsAdmin(ctx, &rbacv1.Role{ObjectMeta: meta(legacyName), Rules: []rbacv1.PolicyRule{createSecrets}})
		binding := &rbacv1.RoleBinding{
			ObjectMeta: meta(legacyName),
			RoleRef:    roleRef("Role", legacyName),
			Subjects:   []rbacv1.Subject{serviceAccountSubject(ns, legacyName), serviceAccountSubject(ns, "default")},
		}
		createAsAdmin(ctx, binding)

		err := patchAs(ctx, binding, types.MergePatchType, labelPatch, metav1.PatchOptions{DryRun: dryRun})
		Expect(err).To(deniedBy(subjectsRule))
	})

	It("denies a Role with * verbs", func(ctx SpecContext) {
		// RBAC refuses a * verb before admission unless the plugin may escalate.
		grant(ctx, c, cfg, "", escalateGrantName, pluginSubject, rbacv1.PolicyRule{
			APIGroups: []string{rbacv1.GroupName}, Resources: []string{"roles"}, Verbs: []string{"escalate"},
		})
		role := &rbacv1.Role{ObjectMeta: meta(chainName), Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"*"},
		}}}
		Expect(createRole(ctx, role, dryRun)).To(deniedBy(verbsRule))
	})

	It("denies deleting a Role or RoleBinding not named *.telemetry", func(ctx SpecContext) {
		// CloudNativePG names the Role and RoleBinding of a Cluster after it.
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(c.Get(ctx, key(clusterName), &rbacv1.Role{})).To(Succeed())
			g.Expect(c.Get(ctx, key(clusterName), &rbacv1.RoleBinding{})).To(Succeed())
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(time.Second).Should(Succeed())

		deleteOpts := metav1.DeleteOptions{DryRun: dryRun}
		Expect(plugin.RbacV1().Roles(ns).Delete(ctx, clusterName, deleteOpts)).To(deniedBy(nameRule))
		Expect(plugin.RbacV1().RoleBindings(ns).Delete(ctx, clusterName, deleteOpts)).To(deniedBy(nameRule))
	})

	It("allows deleting the Roles and RoleBindings named *.telemetry", func(ctx SpecContext) {
		Expect(plugin.RbacV1().RoleBindings(ns).Delete(ctx, telemetryName, metav1.DeleteOptions{})).To(Succeed())
		Expect(plugin.RbacV1().Roles(ns).Delete(ctx, telemetryName, metav1.DeleteOptions{})).To(Succeed())
		Expect(plugin.RbacV1().Roles(ns).Delete(ctx, probeName, metav1.DeleteOptions{})).To(Succeed())
	})

	Context("accounts named after the plugin's", func() {
		var dotted, dashed *kubernetes.Clientset

		BeforeAll(func(ctx SpecContext) {
			rules := []struct {
				suffix string
				rule   rbacv1.PolicyRule
			}{
				{"write-core", rbacv1.PolicyRule{APIGroups: []string{""}, Verbs: []string{"create"}, Resources: identities}},
				{"write-rbac", rbacv1.PolicyRule{
					APIGroups: []string{rbacv1.GroupName}, Verbs: []string{"create"}, Resources: []string{"roles", "rolebindings"},
				}},
				{"read", readConfigMaps},
			}
			for _, name := range []string{probeName, dashedName} {
				account := serviceAccount(ns, name)
				Expect(c.Create(ctx, account)).To(Succeed())
				DeferCleanup(func(ctx SpecContext) {
					Expect(client.IgnoreNotFound(c.Delete(ctx, account))).To(Succeed())
				})
				for _, r := range rules {
					grant(ctx, c, cfg, ns, name+"-"+r.suffix, serviceAccountSubject(ns, name), r.rule)
				}
			}

			var err error
			dotted, err = framework.Impersonate(cfg, framework.ServiceAccountUser(ns, probeName))
			Expect(err).NotTo(HaveOccurred())
			dashed, err = framework.Impersonate(cfg, framework.ServiceAccountUser(ns, dashedName))
			Expect(err).NotTo(HaveOccurred())
		})

		role := func(name string, rules ...rbacv1.PolicyRule) *rbacv1.Role {
			return &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Rules: rules}
		}

		It("holds a *.telemetry account to the identities policy", func(ctx SpecContext) {
			_, err := dotted.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, "other"), metav1.CreateOptions{DryRun: dryRun})
			Expect(err).To(denied(writeRule))

			_, err = dotted.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, "other.telemetry"), metav1.CreateOptions{DryRun: dryRun})
			Expect(err).NotTo(HaveOccurred())
		})

		It("holds a *.telemetry account to the roles policy", func(ctx SpecContext) {
			_, err := dotted.RbacV1().Roles(ns).Create(ctx, role(chainName, createSecrets, readConfigMaps), metav1.CreateOptions{DryRun: dryRun})
			Expect(err).To(deniedBy(verbsRule))

			_, err = dotted.RbacV1().Roles(ns).Create(ctx, role("read.telemetry", readConfigMaps), metav1.CreateOptions{DryRun: dryRun})
			Expect(err).NotTo(HaveOccurred())
		})

		It("holds an account named probe-telemetry, without the dot, to neither policy", func(ctx SpecContext) {
			_, err := dashed.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, "other"), metav1.CreateOptions{DryRun: dryRun})
			Expect(err).NotTo(HaveOccurred())

			_, err = dashed.RbacV1().Roles(ns).Create(ctx, role(chainName, createSecrets, readConfigMaps), metav1.CreateOptions{DryRun: dryRun})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("denies any user a Pooler named adm.telemetry", func(ctx SpecContext) {
		Expect(c.Create(ctx, pooler(ns, telemetryName), client.DryRunAll)).To(reserved())
	})

	It("allows a Pooler named adm-pooler", func(ctx SpecContext) {
		Expect(c.Create(ctx, pooler(ns, "adm-pooler"), client.DryRunAll)).To(Succeed())
	})

	It("the API server rejects a Cluster and a Pooler whose spec.serviceAccountName ends in .telemetry, before admission", func(ctx SpecContext) {
		rejected := SatisfyAll(Satisfy(apierrors.IsInvalid),
			MatchError(ContainSubstring("spec.serviceAccountName")), MatchError(ContainSubstring("should match")),
			Not(MatchError(ContainSubstring(reservedPolicy))))

		cluster := framework.NewCluster(ns, "adm-sa", image)
		cluster.Spec.ServiceAccountName = telemetryName
		Expect(c.Create(ctx, cluster, client.DryRunAll)).To(rejected)

		p := pooler(ns, "adm-sa-pooler")
		p.Spec.ServiceAccountName = telemetryName
		Expect(c.Create(ctx, p, client.DryRunAll)).To(rejected)
	})

	It("never repeats the policy name in a denial message", func(ctx SpecContext) {
		_, identitiesErr := plugin.CoreV1().ServiceAccounts(ns).Create(ctx, serviceAccount(ns, "other"), metav1.CreateOptions{DryRun: dryRun})
		rolesErr := createRole(ctx, &rbacv1.Role{ObjectMeta: meta("chain"), Rules: []rbacv1.PolicyRule{readConfigMaps}}, dryRun)
		reservedErr := c.Create(ctx, pooler(ns, telemetryName), client.DryRunAll)

		for policy, err := range map[string]error{policyName: identitiesErr, rolesPolicy: rolesErr, reservedPolicy: reservedErr} {
			Expect(err).To(MatchError(ContainSubstring("'"+policy+"'")), "the API server names the policy")
			Expect(denialText(err)).NotTo(ContainSubstring(policy))
		}
	})

	It("leaves other users alone", func(ctx SpecContext) {
		opaque := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "opaque"}, Type: corev1.SecretTypeOpaque}
		Expect(c.Create(ctx, opaque)).To(Succeed())
	})
})
