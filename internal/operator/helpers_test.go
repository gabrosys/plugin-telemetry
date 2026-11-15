package operator_test

import (
	"context"
	"errors"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/identity"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

var errGet = errors.New("get refused")

func newClient(objs []client.Object, funcs interceptor.Funcs) client.Client {
	GinkgoHelper()

	scheme, err := operator.NewScheme()
	Expect(err).NotTo(HaveOccurred())

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
}

func newCluster(namespace, name string, plugins ...apiv1.PluginConfiguration) *apiv1.Cluster {
	return &apiv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: apiv1.SchemeGroupVersion.String(), Kind: apiv1.ClusterKind},
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(name + "-uid"), Generation: 1},
		Spec:       apiv1.ClusterSpec{Plugins: plugins},
	}
}

func disabled(params map[string]string) apiv1.PluginConfiguration {
	p := ours(params)
	p.Enabled = ptr.To(false)

	return p
}

func newExporter(namespace, name string) *v1alpha1.TelemetryExporter {
	return &v1alpha1.TelemetryExporter{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// identityExists reports whether each identity object of a Cluster in namespace "ns" is present.
func identityExists(ctx context.Context, c client.Reader, cluster string) [4]bool {
	GinkgoHelper()

	var found [4]bool
	for i, obj := range identity.Members("ns", cluster) {
		err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		if err != nil {
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), err.Error())
		}
		found[i] = err == nil
	}

	return found
}

var allFour = [4]bool{true, true, true, true}
