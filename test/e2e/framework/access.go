package framework

import (
	"context"
	"errors"
	"fmt"
	"slices"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

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

// ServiceAccountUser returns the user name of a ServiceAccount.
func ServiceAccountUser(namespace, serviceAccount string) string {
	return "system:serviceaccount:" + namespace + ":" + serviceAccount
}

// CanI is `kubectl auth can-i --as=<user>`: a SelfSubjectAccessReview under impersonation.
func CanI(ctx context.Context, cfg *rest.Config, user string, attrs authorizationv1.ResourceAttributes) (bool, error) {
	cs, err := Impersonate(cfg, user)
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
func Rules(
	ctx context.Context, cfg *rest.Config, user, namespace string,
) ([]authorizationv1.ResourceRule, []authorizationv1.NonResourceRule, error) {
	cs, err := Impersonate(cfg, user)
	if err != nil {
		return nil, nil, err
	}

	review, err := cs.AuthorizationV1().SelfSubjectRulesReviews().Create(ctx, &authorizationv1.SelfSubjectRulesReview{
		Spec: authorizationv1.SelfSubjectRulesReviewSpec{Namespace: namespace},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("create self subject rules review as %s: %w", user, err)
	}
	if review.Status.Incomplete {
		return nil, nil, fmt.Errorf("rules of %s in %s: %w: %s", user, namespace, ErrIncompleteRules, review.Status.EvaluationError)
	}

	return review.Status.ResourceRules, review.Status.NonResourceRules, nil
}

// UnexpectedNonResourceRules returns the non-resource rules with a verb other than get.
func UnexpectedNonResourceRules(rules []authorizationv1.NonResourceRule) []authorizationv1.NonResourceRule {
	var unexpected []authorizationv1.NonResourceRule
	for _, r := range rules {
		if slices.ContainsFunc(r.Verbs, func(verb string) bool { return verb != "get" }) {
			unexpected = append(unexpected, r)
		}
	}

	return unexpected
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

// Impersonate returns a clientset that acts as user.
func Impersonate(cfg *rest.Config, user string) (*kubernetes.Clientset, error) {
	impersonated := rest.CopyConfig(cfg)
	impersonated.Impersonate = rest.ImpersonationConfig{UserName: user}

	cs, err := kubernetes.NewForConfig(impersonated)
	if err != nil {
		return nil, fmt.Errorf("create clientset as %s: %w", user, err)
	}

	return cs, nil
}
