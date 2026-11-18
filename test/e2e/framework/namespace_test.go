package framework_test

import (
	"context"
	"errors"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		namespace *corev1.Namespace
		cluster   *apiv1.Cluster
		instance  *corev1.Pod
		pooler    *corev1.Pod
		collector *corev1.Pod
		calls     []string
	)

	newClient := func(funcs interceptor.Funcs, objs ...client.Object) client.WithWatch {
		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(apiv1.AddToScheme(scheme)).To(Succeed())

		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
	}

	// cleanup releases the Cluster's finalizer on the first Cluster list and deletes the instance Pod on the first
	// Pod list that sees it, so each half of the wait polls once before it sees nothing left.
	cleanup := func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if err := c.List(ctx, list, opts...); err != nil {
			return err
		}
		switch l := list.(type) {
		case *apiv1.ClusterList:
			if len(l.Items) > 0 {
				calls = append(calls, "cluster listed")
				l.Items[0].Finalizers = nil
				Expect(c.Update(ctx, &l.Items[0])).To(Succeed())
			}
		case *corev1.PodList:
			for i := range l.Items {
				if l.Items[i].Name == instance.Name {
					calls = append(calls, "instance pod listed")
					Expect(c.Delete(ctx, instance)).To(Succeed())
				}
			}
		}

		return nil
	}

	BeforeEach(func() {
		calls = nil
		namespace = &corev1.Namespace{Name: ns, Finalizers: []string{hold}}
		cluster = &apiv1.Cluster{Namespace: ns, Name: "pg", Finalizers: []string{hold}}
		instance = &corev1.Pod{Namespace: ns, Name: "pg-1", Labels: map[string]string{
			"cnpg.io/cluster": "pg", "cnpg.io/podRole": "instance",
		}}
		pooler = &corev1.Pod{Namespace: ns, Name: "pooler-1", Labels: map[string]string{
			"cnpg.io/cluster": "pg", "cnpg.io/podRole": "pooler",
		}}
		collector = &corev1.Pod{Namespace: ns, Name: "collector"}
	})

	It("deletes the Clusters, waits for them and their instance Pods, then deletes the namespace and waits until it is gone",
		func(ctx SpecContext) {
			c := newClient(interceptor.Funcs{
				DeleteAllOf: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteAllOfOption) error {
					calls = append(calls, "delete clusters")

					return c.DeleteAllOf(ctx, obj, opts...)
				},
				List: cleanup,
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*corev1.Namespace); ok {
						calls = append(calls, "delete namespace")
					}

					return c.Delete(ctx, obj, opts...)
				},
				// The first Get after the delete still sees the namespace; releasing the finalizer removes it.
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					err := c.Get(ctx, key, obj, opts...)
					if got, ok := obj.(*corev1.Namespace); ok && err == nil && got.DeletionTimestamp != nil {
						calls = append(calls, "namespace terminating")
						got.Finalizers = nil
						Expect(c.Update(ctx, got)).To(Succeed())
					}

					return err
				},
			}, namespace, cluster, instance, pooler, collector)

			Expect(framework.DeleteNamespace(ctx, c, ns, poll)).To(Succeed())
			Expect(calls).To(Equal([]string{
				"delete clusters", "cluster listed", "instance pod listed", "delete namespace", "namespace terminating",
			}))
			Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{}))).To(BeTrue())
			var clusters apiv1.ClusterList
			Expect(c.List(ctx, &clusters, client.InNamespace(ns))).To(Succeed())
			Expect(clusters.Items).To(BeEmpty())
		}, SpecTimeout(10*time.Second))

	It("keeps polling after a failed list", func(ctx SpecContext) {
		failed := false
		c := newClient(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok && !failed {
					failed = true

					return errors.New("injected")
				}

				return cleanup(ctx, c, list, opts...)
			},
		}, &corev1.Namespace{Name: ns}, cluster, instance)

		Expect(framework.DeleteNamespace(ctx, c, ns, poll)).To(Succeed())
		Expect(failed).To(BeTrue())
	}, SpecTimeout(10*time.Second))

	It("succeeds when the namespace is already gone", func(ctx SpecContext) {
		Expect(framework.DeleteNamespace(ctx, newClient(interceptor.Funcs{}), ns, poll)).To(Succeed())
	}, SpecTimeout(10*time.Second))

	It("returns the context's error and what is left, and keeps the namespace, while an instance Pod remains",
		func(ctx SpecContext) {
			c := newClient(interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*corev1.Namespace); ok {
						calls = append(calls, "delete namespace")
					}

					return c.Delete(ctx, obj, opts...)
				},
			}, namespace, instance, pooler, collector)

			short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			err := framework.DeleteNamespace(short, c, ns, poll)
			Expect(err).To(MatchError(context.DeadlineExceeded))
			Expect(err).To(MatchError(ContainSubstring("left: 0 clusters, 1 pods")))
			Expect(calls).To(BeEmpty())
			Expect(c.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{})).To(Succeed())
		}, SpecTimeout(10*time.Second))

	It("returns the last failed read when every Pod list fails", func(ctx SpecContext) {
		injected := errors.New("injected")
		c := newClient(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					return injected
				}

				return c.List(ctx, list, opts...)
			},
		}, namespace)

		short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := framework.DeleteNamespace(short, c, ns, poll)
		Expect(err).To(MatchError(context.DeadlineExceeded))
		Expect(err).To(MatchError(injected))
		Expect(err).To(MatchError(ContainSubstring("last error: list pods: injected")))
		Expect(err).NotTo(MatchError(ContainSubstring("left:")))
	}, SpecTimeout(10*time.Second))

	It("reports the namespace's remaining content and finalizers when it does not go", func(ctx SpecContext) {
		namespace.Status.Conditions = []corev1.NamespaceCondition{
			{Type: corev1.NamespaceContentRemaining, Status: corev1.ConditionTrue, Message: "Some resources are remaining: pods. has 1 resource instances"},
			{Type: corev1.NamespaceFinalizersRemaining, Status: corev1.ConditionTrue, Message: "Some content in the namespace has finalizers remaining: test.example/hold in 1 resource instances"},
			{Type: corev1.NamespaceDeletionDiscoveryFailure, Status: corev1.ConditionFalse, Message: "All resources successfully discovered"},
			{Type: corev1.NamespaceDeletionContentFailure, Status: corev1.ConditionTrue, Message: "content deletion failed"},
		}
		c := newClient(interceptor.Funcs{}, namespace)

		short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := framework.DeleteNamespace(short, c, ns, poll)
		Expect(err).To(MatchError(context.DeadlineExceeded))
		Expect(err).To(MatchError(ContainSubstring("Some resources are remaining: pods. has 1 resource instances")))
		Expect(err).To(MatchError(ContainSubstring("finalizers remaining: test.example/hold in 1 resource instances")))
		Expect(err).NotTo(MatchError(ContainSubstring("content deletion failed")))
		Expect(err).NotTo(MatchError(ContainSubstring("successfully discovered")))
	}, SpecTimeout(10*time.Second))
})
