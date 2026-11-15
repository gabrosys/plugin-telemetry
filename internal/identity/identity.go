// Package identity computes and applies each Cluster's sidecar identity: a ServiceAccount, a Role,
// a RoleBinding and a ServiceAccount token Secret, all owned by the Cluster.
package identity

import (
	"errors"
	"fmt"
	"slices"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
)

const (
	// ManagedByLabel is the label the manager's cache selects on.
	ManagedByLabel = metadata.ManagedByLabel
	// ManagedBy is the value of ManagedByLabel.
	ManagedBy = metadata.ManagedByValue
	// ClusterLabel names the Cluster an object belongs to.
	ClusterLabel = metadata.PluginName + "/cluster"
	// FieldOwner is the server-side apply field manager.
	FieldOwner = "plugin-telemetry"
)

// ErrNoUID is returned for a Cluster without a UID, which cannot own anything.
var ErrNoUID = errors.New("cluster has no uid")

// Objects are one Cluster's desired identity objects.
type Objects struct {
	ServiceAccount *corev1.ServiceAccount
	Role           *rbacv1.Role
	RoleBinding    *rbacv1.RoleBinding
	// Token is a kubernetes.io/service-account-token Secret with only the service-account.name annotation.
	Token *corev1.Secret
}

// Desired computes the four objects. It fails when the Cluster has no UID.
func Desired(c *apiv1.Cluster, refs clusterconfig.Refs) (Objects, error) {
	if c.UID == "" {
		return Objects{}, fmt.Errorf("cluster %s/%s: %w", c.Namespace, c.Name, ErrNoUID)
	}

	account := metadata.ServiceAccountName(c.Name)
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name:      name,
			Namespace: c.Namespace,
			Labels:    map[string]string{ManagedByLabel: ManagedBy, ClusterLabel: c.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         apiv1.SchemeGroupVersion.String(),
				Kind:               apiv1.ClusterKind,
				Name:               c.Name,
				UID:                c.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(false),
			}},
		}
	}

	tokenMeta := meta(metadata.TokenSecretName(c.Name))
	tokenMeta.Annotations = map[string]string{corev1.ServiceAccountNameKey: account}

	return Objects{
		ServiceAccount: &corev1.ServiceAccount{ObjectMeta: meta(account)},
		Role:           &rbacv1.Role{ObjectMeta: meta(account), Rules: rules(refs)},
		RoleBinding: &rbacv1.RoleBinding{
			ObjectMeta: meta(account),
			RoleRef:    ownRoleRef(account),
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: account, Namespace: c.Namespace}},
		},
		Token: &corev1.Secret{ObjectMeta: tokenMeta, Type: corev1.SecretTypeServiceAccountToken},
	}, nil
}

// ownRoleRef refers to the Role of the account's own name.
func ownRoleRef(account string) rbacv1.RoleRef {
	return rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: account}
}

// rules has one read rule per kind that has names; an empty resourceNames, or an empty name, would grant every name.
func rules(refs clusterconfig.Refs) []rbacv1.PolicyRule {
	var out []rbacv1.PolicyRule
	add := func(group, resource string, names ...string) {
		names = slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == "" })
		if len(names) == 0 {
			return
		}
		out = append(out, rbacv1.PolicyRule{
			APIGroups:     []string{group},
			Resources:     []string{resource},
			ResourceNames: names,
			Verbs:         []string{"get", "list", "watch"},
		})
	}

	add(apiv1.SchemeGroupVersion.Group, "clusters", refs.Cluster)
	add(v1alpha1.GroupVersion.Group, "telemetryexporters", refs.Exporter)
	add(v1alpha1.GroupVersion.Group, "metricquerysets", refs.QuerySets...)
	add("", "configmaps", refs.ConfigMaps...)
	add("", "secrets", refs.Secrets...)

	return out
}
