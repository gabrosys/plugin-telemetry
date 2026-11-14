package identity

import (
	"context"
	"fmt"
	"slices"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

// Ensurer applies and removes the identity objects.
type Ensurer struct {
	Reader client.Reader // the label-limited cache
	Writer client.Client
}

// Result reports what Ensure applied.
type Result struct {
	// Applied lists the objects applied, as Kind/name.
	Applied []string
	// Ready is true when all four objects are cached and the token has data.token.
	Ready bool
}

// step is one object: the cache copy to compare against, and its apply configuration.
type step struct {
	label string
	live  client.Object
	same  func() bool
	apply runtime.ApplyConfiguration
}

// Ensure applies, in order ServiceAccount, Role, RoleBinding and token Secret, each object that differs
// from the cache, by server-side apply with FieldOwner and ForceOwnership.
func (e *Ensurer) Ensure(ctx context.Context, c *apiv1.Cluster, refs clusterconfig.Refs) (Result, error) {
	want, err := Desired(c, refs)
	if err != nil {
		return Result{}, err
	}

	var res Result
	for _, s := range steps(want) {
		current, err := e.matches(ctx, s)
		if err != nil {
			return res, err
		}
		if current {
			continue
		}
		if err := e.Writer.Apply(ctx, s.apply, client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
			return res, fmt.Errorf("apply %s in %s: %w", s.label, c.Namespace, err)
		}
		res.Applied = append(res.Applied, s.label)
	}

	res.Ready, err = e.Ready(ctx, c.Namespace, c.Name)
	if err != nil {
		return res, err
	}

	return res, nil
}

// matches reports whether the cached object equals the desired one.
func (e *Ensurer) matches(ctx context.Context, s step) (bool, error) {
	err := e.Reader.Get(ctx, client.ObjectKeyFromObject(s.live), s.live)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get %s: %w", s.label, err)
	}

	return s.same(), nil
}

// Members returns the cluster's four identity objects, keyed by namespace and name: the token Secret,
// the RoleBinding, the Role and the ServiceAccount.
func Members(namespace, cluster string) []client.Object {
	account := metadata.ServiceAccountName(cluster)
	meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: namespace, Name: name} }

	return []client.Object{
		&corev1.Secret{ObjectMeta: meta(metadata.TokenSecretName(cluster))},
		&rbacv1.RoleBinding{ObjectMeta: meta(account)},
		&rbacv1.Role{ObjectMeta: meta(account)},
		&corev1.ServiceAccount{ObjectMeta: meta(account)},
	}
}

// Remove deletes the four objects; NotFound counts as done.
func (e *Ensurer) Remove(ctx context.Context, namespace, cluster string) error {
	for _, obj := range Members(namespace, cluster) {
		if err := e.Writer.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete %T %s/%s: %w", obj, namespace, obj.GetName(), err)
		}
	}

	return nil
}

// Present reports from the cache whether any of the four objects exists.
func (e *Ensurer) Present(ctx context.Context, namespace, cluster string) (bool, error) {
	for _, obj := range Members(namespace, cluster) {
		found, err := e.exists(ctx, client.ObjectKeyFromObject(obj), obj)
		if err != nil || found {
			return found, err
		}
	}

	return false, nil
}

// Ready reports from the cache whether the four objects exist and the token Secret has data.token.
func (e *Ensurer) Ready(ctx context.Context, namespace, cluster string) (bool, error) {
	var token *corev1.Secret
	for _, obj := range Members(namespace, cluster) {
		found, err := e.exists(ctx, client.ObjectKeyFromObject(obj), obj)
		if err != nil || !found {
			return false, err
		}
		if secret, ok := obj.(*corev1.Secret); ok {
			token = secret
		}
	}

	return token != nil && len(token.Data[corev1.ServiceAccountTokenKey]) > 0, nil
}

func (e *Ensurer) exists(ctx context.Context, key client.ObjectKey, obj client.Object) (bool, error) {
	err := e.Reader.Get(ctx, key, obj)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get %T %s: %w", obj, key, err)
	}

	return true, nil
}

func steps(w Objects) []step {
	sa := &corev1.ServiceAccount{ObjectMeta: key(w.ServiceAccount.ObjectMeta)}
	role := &rbacv1.Role{ObjectMeta: key(w.Role.ObjectMeta)}
	binding := &rbacv1.RoleBinding{ObjectMeta: key(w.RoleBinding.ObjectMeta)}
	token := &corev1.Secret{ObjectMeta: key(w.Token.ObjectMeta)}

	return []step{
		{
			label: "ServiceAccount/" + w.ServiceAccount.Name,
			live:  sa,
			same:  func() bool { return sameMeta(w.ServiceAccount.ObjectMeta, sa.ObjectMeta) },
			apply: corev1ac.ServiceAccount(w.ServiceAccount.Name, w.ServiceAccount.Namespace).
				WithLabels(w.ServiceAccount.Labels).WithOwnerReferences(ownerRefs(w.ServiceAccount.OwnerReferences)...),
		},
		{
			label: "Role/" + w.Role.Name,
			live:  role,
			same: func() bool {
				return sameMeta(w.Role.ObjectMeta, role.ObjectMeta) && equality.Semantic.DeepEqual(w.Role.Rules, role.Rules)
			},
			apply: rbacv1ac.Role(w.Role.Name, w.Role.Namespace).
				WithLabels(w.Role.Labels).WithOwnerReferences(ownerRefs(w.Role.OwnerReferences)...).
				WithRules(policyRules(w.Role.Rules)...),
		},
		{
			label: "RoleBinding/" + w.RoleBinding.Name,
			live:  binding,
			same: func() bool {
				return sameMeta(w.RoleBinding.ObjectMeta, binding.ObjectMeta) &&
					w.RoleBinding.RoleRef == binding.RoleRef && slices.Equal(w.RoleBinding.Subjects, binding.Subjects)
			},
			apply: rbacv1ac.RoleBinding(w.RoleBinding.Name, w.RoleBinding.Namespace).
				WithLabels(w.RoleBinding.Labels).WithOwnerReferences(ownerRefs(w.RoleBinding.OwnerReferences)...).
				WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(w.RoleBinding.RoleRef.APIGroup).
					WithKind(w.RoleBinding.RoleRef.Kind).WithName(w.RoleBinding.RoleRef.Name)).
				WithSubjects(subjects(w.RoleBinding.Subjects)...),
		},
		{
			label: "Secret/" + w.Token.Name,
			live:  token,
			same: func() bool {
				return sameMeta(w.Token.ObjectMeta, token.ObjectMeta) && w.Token.Type == token.Type &&
					token.Annotations[corev1.ServiceAccountNameKey] == w.Token.Annotations[corev1.ServiceAccountNameKey]
			},
			apply: corev1ac.Secret(w.Token.Name, w.Token.Namespace).
				WithType(w.Token.Type).WithAnnotations(w.Token.Annotations).
				WithLabels(w.Token.Labels).WithOwnerReferences(ownerRefs(w.Token.OwnerReferences)...),
		},
	}
}

func key(m metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: m.Namespace, Name: m.Name}
}

// sameMeta reports whether live carries every desired label and the desired owner references.
func sameMeta(want, live metav1.ObjectMeta) bool {
	for k, v := range want.Labels {
		if got, ok := live.Labels[k]; !ok || got != v {
			return false
		}
	}

	return slices.EqualFunc(want.OwnerReferences, live.OwnerReferences, func(a, b metav1.OwnerReference) bool {
		return equality.Semantic.DeepEqual(a, b)
	})
}

func ownerRefs(in []metav1.OwnerReference) []*metav1ac.OwnerReferenceApplyConfiguration {
	out := make([]*metav1ac.OwnerReferenceApplyConfiguration, 0, len(in))
	for _, o := range in {
		out = append(out, metav1ac.OwnerReference().WithAPIVersion(o.APIVersion).WithKind(o.Kind).
			WithName(o.Name).WithUID(o.UID).WithController(*o.Controller).WithBlockOwnerDeletion(*o.BlockOwnerDeletion))
	}

	return out
}

func policyRules(in []rbacv1.PolicyRule) []*rbacv1ac.PolicyRuleApplyConfiguration {
	out := make([]*rbacv1ac.PolicyRuleApplyConfiguration, 0, len(in))
	for _, r := range in {
		out = append(out, rbacv1ac.PolicyRule().WithAPIGroups(r.APIGroups...).WithResources(r.Resources...).
			WithResourceNames(r.ResourceNames...).WithVerbs(r.Verbs...))
	}

	return out
}

func subjects(in []rbacv1.Subject) []*rbacv1ac.SubjectApplyConfiguration {
	out := make([]*rbacv1ac.SubjectApplyConfiguration, 0, len(in))
	for _, s := range in {
		out = append(out, rbacv1ac.Subject().WithKind(s.Kind).WithName(s.Name).WithNamespace(s.Namespace))
	}

	return out
}
