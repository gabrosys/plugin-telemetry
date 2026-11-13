package framework

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

const tokenTimeout = time.Minute

// ErrIncompleteRules is returned when the API server cannot list every rule of a user.
var ErrIncompleteRules = errors.New("rules review is incomplete")

var readVerbs = []string{"get", "list", "watch"}

type groupResource struct{ group, resource string }

// selfReviews are the resources system:basic-user may create.
var selfReviews = []groupResource{
	{"authorization.k8s.io", "selfsubjectaccessreviews"},
	{"authorization.k8s.io", "selfsubjectrulesreviews"},
	{"authentication.k8s.io", "selfsubjectreviews"},
}

// SidecarAccess lists the object names the sidecar may read.
type SidecarAccess struct{ ConfigMaps, Secrets []string }

// ServiceAccountUser returns the user name of a ServiceAccount.
func ServiceAccountUser(namespace, serviceAccount string) string {
	return "system:serviceaccount:" + namespace + ":" + serviceAccount
}

// CreateSidecarIdentity creates <cluster>-telemetry ServiceAccount, Role (get, list, watch on its
// Cluster and the named objects), RoleBinding and the token Secret, and waits until the token is set.
func CreateSidecarIdentity(
	ctx context.Context, c client.Client, namespace, cluster string, access SidecarAccess,
) error {
	name := metadata.ServiceAccountName(cluster)
	meta := func(objName string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: namespace, Name: objName} }

	// Create decodes the response into the object, so no slice here may alias shared state.
	read := func(group, resource string, names []string) rbacv1.PolicyRule {
		return rbacv1.PolicyRule{
			APIGroups:     []string{group},
			Resources:     []string{resource},
			ResourceNames: slices.Clone(names),
			Verbs:         slices.Clone(readVerbs),
		}
	}
	rules := []rbacv1.PolicyRule{read(apiv1.SchemeGroupVersion.Group, "clusters", []string{cluster})}
	if len(access.ConfigMaps) > 0 {
		rules = append(rules, read("", "configmaps", access.ConfigMaps))
	}
	if len(access.Secrets) > 0 {
		rules = append(rules, read("", "secrets", access.Secrets))
	}

	token := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        metadata.TokenSecretName(cluster),
			Annotations: map[string]string{corev1.ServiceAccountNameKey: name},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}

	objects := []struct {
		kind string
		obj  client.Object
	}{
		{"serviceaccount", &corev1.ServiceAccount{ObjectMeta: meta(name)}},
		{"role", &rbacv1.Role{ObjectMeta: meta(name), Rules: rules}},
		{"rolebinding", &rbacv1.RoleBinding{
			ObjectMeta: meta(name),
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: namespace, Name: name}},
		}},
		{"secret", token},
	}
	for _, o := range objects {
		if err := c.Create(ctx, o.obj); err != nil {
			return fmt.Errorf("create %s %s/%s: %w", o.kind, namespace, o.obj.GetName(), err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, tokenTimeout)
	defer cancel()

	key := client.ObjectKeyFromObject(token)
	err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		var s corev1.Secret
		if err := c.Get(ctx, key, &s); err != nil {
			return false, fmt.Errorf("get secret %s: %w", key, err)
		}

		return len(s.Data[corev1.ServiceAccountTokenKey]) > 0, nil
	})
	if err != nil {
		return fmt.Errorf("wait for token in secret %s: %w", key, err)
	}

	return nil
}

// CanI is `kubectl auth can-i --as=<user>`: a SelfSubjectAccessReview under impersonation.
func CanI(ctx context.Context, cfg *rest.Config, user string, attrs authorizationv1.ResourceAttributes) (bool, error) {
	cs, err := impersonate(cfg, user)
	if err != nil {
		return false, err
	}

	review, err := cs.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &attrs},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, fmt.Errorf("create self subject access review as %s: %w", user, err)
	}

	return review.Status.Allowed, nil
}

// Rules is `kubectl auth can-i --list --as=<user> -n <namespace>`. It fails
// with ErrIncompleteRules when the API server cannot list every rule.
func Rules(ctx context.Context, cfg *rest.Config, user, namespace string) ([]authorizationv1.ResourceRule, error) {
	cs, err := impersonate(cfg, user)
	if err != nil {
		return nil, err
	}

	review, err := cs.AuthorizationV1().SelfSubjectRulesReviews().Create(ctx, &authorizationv1.SelfSubjectRulesReview{
		Spec: authorizationv1.SelfSubjectRulesReviewSpec{Namespace: namespace},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create self subject rules review as %s: %w", user, err)
	}
	if review.Status.Incomplete {
		return nil, fmt.Errorf("rules of %s in %s: %w: %s", user, namespace, ErrIncompleteRules, review.Status.EvaluationError)
	}

	return review.Status.ResourceRules, nil
}

// UnexpectedRules returns the rules that grant a verb other than get, list
// and watch, except create on the self reviews system:basic-user grants.
func UnexpectedRules(rules []authorizationv1.ResourceRule) []authorizationv1.ResourceRule {
	var unexpected []authorizationv1.ResourceRule
	for _, r := range rules {
		if !expectedRule(r) {
			unexpected = append(unexpected, r)
		}
	}

	return unexpected
}

func expectedRule(r authorizationv1.ResourceRule) bool {
	for _, verb := range r.Verbs {
		if slices.Contains(readVerbs, verb) || (verb == "create" && onlySelfReviews(r)) {
			continue
		}

		return false
	}

	return true
}

func onlySelfReviews(r authorizationv1.ResourceRule) bool {
	if len(r.APIGroups) == 0 || len(r.Resources) == 0 {
		return false
	}

	for _, group := range r.APIGroups {
		for _, resource := range r.Resources {
			if !slices.Contains(selfReviews, groupResource{group, resource}) {
				return false
			}
		}
	}

	return true
}

func impersonate(cfg *rest.Config, user string) (*kubernetes.Clientset, error) {
	impersonated := rest.CopyConfig(cfg)
	impersonated.Impersonate = rest.ImpersonationConfig{UserName: user}

	cs, err := kubernetes.NewForConfig(impersonated)
	if err != nil {
		return nil, fmt.Errorf("create clientset as %s: %w", user, err)
	}

	return cs, nil
}
