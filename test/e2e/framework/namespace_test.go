package framework_test

import (
	"context"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

var _ = Describe("DeleteNamespace", func() {
	const (
		ns   = "test"
		hold = "test.example/hold"
		poll = time.Millisecond
	)

	var (
		instance  *corev1.Pod
		collector *corev1.Pod
		calls     []string
	)

	newClient := func(funcs interceptor.Funcs) client.WithWatch {
		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(apiv1.AddToScheme(scheme)).To(Succeed())

		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Finalizers: []string{hold}}},
			&apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pg"}},
			instance,
			collector,
		).WithInterceptorFuncs(funcs).Build()
	}

	BeforeEach(func() {
		calls = nil
		instance = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: "pg-1", Labels: map[string]string{"cnpg.io/cluster": "pg"},
		}}
		collector = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "collector"}}
	})

	It("deletes the Clusters, waits for their Pods, then deletes the namespace and waits until it is gone",
		func(ctx SpecContext) {
			c := newClient(interceptor.Funcs{
				DeleteAllOf: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteAllOfOption) error {
					calls = append(calls, "delete clusters")

					return c.DeleteAllOf(ctx, obj, opts...)
				},
				// The first Pod list still sees the instance Pod; it is gone by the second.
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					err := c.List(ctx, list, opts...)
					if pods, ok := list.(*corev1.PodList); ok && len(pods.Items) > 0 && pods.Items[0].Name == instance.Name {
						calls = append(calls, "instance pod listed")
						Expect(c.Delete(ctx, instance)).To(Succeed())
					}

					return err
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*corev1.Namespace); ok {
						calls = append(calls, "delete namespace")
					}

					return c.Delete(ctx, obj, opts...)
				},
				// The first Get after the delete still sees the namespace; releasing the finalizer removes it.
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					err := c.Get(ctx, key, obj, opts...)
					if namespace, ok := obj.(*corev1.Namespace); ok && err == nil && namespace.DeletionTimestamp != nil {
						calls = append(calls, "namespace terminating")
						namespace.Finalizers = nil
						Expect(c.Update(ctx, namespace)).To(Succeed())
					}

					return err
				},
			})

			Expect(framework.DeleteNamespace(ctx, c, ns, poll)).To(Succeed())
			Expect(calls).To(Equal([]string{
				"delete clusters", "instance pod listed", "delete namespace", "namespace terminating",
			}))
			Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{}))).To(BeTrue())
			var clusters apiv1.ClusterList
			Expect(c.List(ctx, &clusters, client.InNamespace(ns))).To(Succeed())
			Expect(clusters.Items).To(BeEmpty())
		})

	It("returns the context's error and keeps the namespace while a Cluster's Pod remains", func(ctx SpecContext) {
		c := newClient(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*corev1.Namespace); ok {
					calls = append(calls, "delete namespace")
				}

				return c.Delete(ctx, obj, opts...)
			},
		})

		short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		Expect(framework.DeleteNamespace(short, c, ns, poll)).To(MatchError(context.DeadlineExceeded))
		Expect(calls).To(BeEmpty())
		Expect(c.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{})).To(Succeed())
	})
})
