package framework_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authorizationv1 "k8s.io/api/authorization/v1"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

func rule(verbs []string, group string, resources ...string) authorizationv1.ResourceRule {
	return authorizationv1.ResourceRule{Verbs: verbs, APIGroups: []string{group}, Resources: resources}
}

var _ = Describe("UnexpectedRules", func() {
	create := []string{"create"}

	DescribeTable("accepts",
		func(r authorizationv1.ResourceRule) {
			Expect(framework.UnexpectedRules([]authorizationv1.ResourceRule{r})).To(BeEmpty())
		},
		Entry("reads", rule([]string{"get", "list", "watch"}, "", "secrets")),
		Entry("create on access and rules self reviews",
			rule(create, "authorization.k8s.io", "selfsubjectaccessreviews", "selfsubjectrulesreviews")),
		Entry("create on self subject reviews", rule(create, "authentication.k8s.io", "selfsubjectreviews")),
	)

	DescribeTable("flags",
		func(r authorizationv1.ResourceRule) {
			Expect(framework.UnexpectedRules([]authorizationv1.ResourceRule{r})).To(HaveExactElements(r))
		},
		Entry("create on secrets", rule(create, "", "secrets")),
		Entry("every verb", rule([]string{"*"}, "", "configmaps")),
		Entry("a read verb next to a write verb", rule([]string{"get", "delete"}, "", "pods")),
		Entry("update on a self review", rule([]string{"update"}, "authorization.k8s.io", "selfsubjectaccessreviews")),
		Entry("create on a review of another subject", rule(create, "authorization.k8s.io", "subjectaccessreviews")),
		Entry("create on a self review in another group", rule(create, "authentication.k8s.io", "selfsubjectaccessreviews")),
		Entry("create on every resource of a review group", rule(create, "authorization.k8s.io", "*")),
		Entry("create on a self review and on another resource",
			rule(create, "authorization.k8s.io", "selfsubjectaccessreviews", "subjectaccessreviews")),
		Entry("create with no API group", authorizationv1.ResourceRule{Verbs: create, Resources: []string{"selfsubjectreviews"}}),
	)

	It("keeps the flagged rules in order", func() {
		del := rule([]string{"delete"}, "", "pods")
		patch := rule([]string{"patch"}, "", "secrets")
		rules := []authorizationv1.ResourceRule{
			del,
			rule([]string{"get"}, "", "configmaps"),
			patch,
		}
		Expect(framework.UnexpectedRules(rules)).To(HaveExactElements(del, patch))
	})
})

var _ = Describe("UnexpectedNonResourceRules", func() {
	It("drops the get rules and returns the rest", func() {
		get := authorizationv1.NonResourceRule{Verbs: []string{"get"}, NonResourceURLs: []string{"/healthz"}}
		post := authorizationv1.NonResourceRule{Verbs: []string{"post"}, NonResourceURLs: []string{"/metrics"}}
		Expect(framework.UnexpectedNonResourceRules([]authorizationv1.NonResourceRule{get, post})).To(HaveExactElements(post))
	})

	It("flags a rule that holds get next to another verb", func() {
		mixed := authorizationv1.NonResourceRule{Verbs: []string{"get", "put"}, NonResourceURLs: []string{"/version"}}
		Expect(framework.UnexpectedNonResourceRules([]authorizationv1.NonResourceRule{mixed})).To(HaveExactElements(mixed))
	})
})
