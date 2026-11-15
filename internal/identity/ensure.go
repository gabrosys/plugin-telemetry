package identity

import (
	"context"
	"errors"
	"fmt"
	"slices"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

// Ensurer creates, applies and removes the identity objects.
type Ensurer struct {
	Reader client.Reader // the label-limited cache
	Writer client.Client
}

// ErrInTheWay means an identity object exists that the cache doesn't hold, or the cached token has another type.
var ErrInTheWay = errors.New("in the way of the identity")

// step is one object: its empty cache copy, the desired object, whether the copy matches it, and its apply
// configuration pinned to a uid and resourceVersion.
type step struct {
	cached client.Object
	want   client.Object
	same   func() bool
	apply  func(uid types.UID, version string) runtime.ApplyConfiguration
}

// Ensure visits ServiceAccount, Role, RoleBinding and token Secret in order. An object the cache lacks gets a
// plain Create with FieldOwner; AlreadyExists ends the pass with ErrInTheWay. A cached object that differs is
// applied with FieldOwner, ForceOwnership, its cached uid and its cached resourceVersion. It returns what it wrote, as Kind/name.
func (e *Ensurer) Ensure(ctx context.Context, c *apiv1.Cluster, refs clusterconfig.Refs) ([]string, error) {
	want, err := Desired(c, refs)
	if err != nil {
		return nil, err
	}

	var wrote []string
	for _, s := range steps(want) {
		name := label(s.cached)
		found, err := e.cachedGet(ctx, s.cached)
		if err != nil {
			return wrote, err
		}
		switch {
		case !found:
			err = e.Writer.Create(ctx, s.want, client.FieldOwner(FieldOwner))
			if apierrors.IsAlreadyExists(err) {
				return wrote, fmt.Errorf("%w: %s: %w", ErrInTheWay, name, err)
			}
			if err != nil {
				return wrote, fmt.Errorf("create %s in %s: %w", name, c.Namespace, err)
			}
		case wrongToken(s.cached):
			return wrote, fmt.Errorf("%w: %s has another type than %s", ErrInTheWay, name, corev1.SecretTypeServiceAccountToken)
		case s.same():
			continue
		default:
			err = e.Writer.Apply(ctx, s.apply(s.cached.GetUID(), s.cached.GetResourceVersion()),
				client.FieldOwner(FieldOwner), client.ForceOwnership)
			if err != nil {
				return wrote, fmt.Errorf("apply %s in %s: %w", name, c.Namespace, err)
			}
		}
		wrote = append(wrote, name)
	}

	return wrote, nil
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

// Remove deletes the cached objects but a wrong-typed token, with uid and resourceVersion preconditions,
// NotFound as done; it returns the count and errors.Join of the failures.
func (e *Ensurer) Remove(ctx context.Context, namespace, cluster string) (int, error) {
	var errs []error
	removed := 0
	for _, obj := range Members(namespace, cluster) {
		found, err := e.cachedGet(ctx, obj)
		if err != nil {
			errs = append(errs, err)

			continue
		}
		if !found || wrongToken(obj) {
			continue
		}
		err = e.deleteExact(ctx, obj)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			errs = append(errs, err)
		default:
			removed++
		}
	}

	return removed, errors.Join(errs...)
}

// Pending returns, from the cache, one line per reason the identity isn't ready; none means ready.
func (e *Ensurer) Pending(ctx context.Context, namespace, cluster string) ([]string, error) {
	var lines []string
	for _, obj := range Members(namespace, cluster) {
		name := label(obj)
		found, err := e.cachedGet(ctx, obj)
		if err != nil {
			return nil, err
		}
		secret, isSecret := obj.(*corev1.Secret)
		switch {
		case !found:
			lines = append(lines, name+" is not in the plugin's cache: not created yet, or not the plugin's")
		case wrongToken(obj):
			lines = append(lines, fmt.Sprintf("%s has type %s, not %s: delete it", name, secret.Type, corev1.SecretTypeServiceAccountToken))
		case isSecret && len(secret.Data[corev1.ServiceAccountTokenKey]) == 0:
			lines = append(lines, name+" has no data.token yet")
		}
	}

	return lines, nil
}

// cachedGet reads obj from the cache; found is false for NotFound.
func (e *Ensurer) cachedGet(ctx context.Context, obj client.Object) (bool, error) {
	err := e.Reader.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get %s in %s: %w", label(obj), obj.GetNamespace(), err)
	}

	return true, nil
}

// wrongToken reports whether obj is a Secret that is not a service-account token.
func wrongToken(obj client.Object) bool {
	secret, ok := obj.(*corev1.Secret)

	return ok && secret.Type != corev1.SecretTypeServiceAccountToken
}

// deleteExact deletes obj only while it has the UID and resourceVersion it was read with.
func (e *Ensurer) deleteExact(ctx context.Context, obj client.Object) error {
	uid, version := obj.GetUID(), obj.GetResourceVersion()
	if err := e.Writer.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &version}); err != nil {
		return fmt.Errorf("delete %s in %s: %w", label(obj), obj.GetNamespace(), err)
	}

	return nil
}

// label returns Kind/name for one of the four identity objects.
func label(obj client.Object) string {
	kind := "Secret"
	switch obj.(type) {
	case *corev1.ServiceAccount:
		kind = "ServiceAccount"
	case *rbacv1.Role:
		kind = "Role"
	case *rbacv1.RoleBinding:
		kind = "RoleBinding"
	}

	return kind + "/" + obj.GetName()
}

func steps(w Objects) []step {
	sa := &corev1.ServiceAccount{ObjectMeta: key(w.ServiceAccount.ObjectMeta)}
	role := &rbacv1.Role{ObjectMeta: key(w.Role.ObjectMeta)}
	binding := &rbacv1.RoleBinding{ObjectMeta: key(w.RoleBinding.ObjectMeta)}
	token := &corev1.Secret{ObjectMeta: key(w.Token.ObjectMeta)}

	return []step{
		{
			cached: sa,
			want:   w.ServiceAccount,
			same:   func() bool { return sameMeta(w.ServiceAccount.ObjectMeta, sa.ObjectMeta) },
			apply: func(uid types.UID, version string) runtime.ApplyConfiguration {
				return corev1ac.ServiceAccount(w.ServiceAccount.Name, w.ServiceAccount.Namespace).WithUID(uid).WithResourceVersion(version).
					WithLabels(w.ServiceAccount.Labels).WithOwnerReferences(ownerRefs(w.ServiceAccount.OwnerReferences)...)
			},
		},
		{
			cached: role,
			want:   w.Role,
			same: func() bool {
				return sameMeta(w.Role.ObjectMeta, role.ObjectMeta) && equality.Semantic.DeepEqual(w.Role.Rules, role.Rules)
			},
			apply: func(uid types.UID, version string) runtime.ApplyConfiguration {
				return rbacv1ac.Role(w.Role.Name, w.Role.Namespace).WithUID(uid).WithResourceVersion(version).
					WithLabels(w.Role.Labels).WithOwnerReferences(ownerRefs(w.Role.OwnerReferences)...).
					WithRules(policyRules(w.Role.Rules)...)
			},
		},
		{
			cached: binding,
			want:   w.RoleBinding,
			same: func() bool {
				return sameMeta(w.RoleBinding.ObjectMeta, binding.ObjectMeta) &&
					w.RoleBinding.RoleRef == binding.RoleRef && slices.Equal(w.RoleBinding.Subjects, binding.Subjects)
			},
			apply: func(uid types.UID, version string) runtime.ApplyConfiguration {
				return rbacv1ac.RoleBinding(w.RoleBinding.Name, w.RoleBinding.Namespace).WithUID(uid).WithResourceVersion(version).
					WithLabels(w.RoleBinding.Labels).WithOwnerReferences(ownerRefs(w.RoleBinding.OwnerReferences)...).
					WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(w.RoleBinding.RoleRef.APIGroup).
						WithKind(w.RoleBinding.RoleRef.Kind).WithName(w.RoleBinding.RoleRef.Name)).
					WithSubjects(subjects(w.RoleBinding.Subjects)...)
			},
		},
		{
			cached: token,
			want:   w.Token,
			same: func() bool {
				return sameMeta(w.Token.ObjectMeta, token.ObjectMeta) &&
					token.Annotations[corev1.ServiceAccountNameKey] == w.Token.Annotations[corev1.ServiceAccountNameKey]
			},
			apply: func(uid types.UID, version string) runtime.ApplyConfiguration {
				return corev1ac.Secret(w.Token.Name, w.Token.Namespace).WithUID(uid).WithResourceVersion(version).
					WithType(w.Token.Type).WithAnnotations(w.Token.Annotations).
					WithLabels(w.Token.Labels).WithOwnerReferences(ownerRefs(w.Token.OwnerReferences)...)
			},
		},
	}
}

func key(m metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: m.Namespace, Name: m.Name}
}

// sameMeta reports whether cached carries every desired label and contains the desired owner reference.
func sameMeta(want, cached metav1.ObjectMeta) bool {
	for k, v := range want.Labels {
		if got, ok := cached.Labels[k]; !ok || got != v {
			return false
		}
	}

	return !slices.ContainsFunc(want.OwnerReferences, func(w metav1.OwnerReference) bool {
		return !slices.ContainsFunc(cached.OwnerReferences, func(ref metav1.OwnerReference) bool {
			return equality.Semantic.DeepEqual(ref, w)
		})
	})
}

func ownerRefs(in []metav1.OwnerReference) []*metav1ac.OwnerReferenceApplyConfiguration {
	out := make([]*metav1ac.OwnerReferenceApplyConfiguration, 0, len(in))
	for _, o := range in {
		out = append(out, metav1ac.OwnerReference().WithAPIVersion(o.APIVersion).WithKind(o.Kind).
			WithName(o.Name).WithUID(o.UID).WithController(ptr.Deref(o.Controller, false)).
			WithBlockOwnerDeletion(ptr.Deref(o.BlockOwnerDeletion, false)))
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
