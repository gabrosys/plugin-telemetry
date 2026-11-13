package operator_test

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/gabrosys/plugin-telemetry/internal/operator"
)

var hex64 = strings.Repeat("a1b2c3d4", 8)

func configMap(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "cnpg-system", Name: "sidecar-image"},
		Data:       data,
	}
}

var _ = DescribeTable("ParseImageRef",
	func(in string, ok bool) {
		_, err := operator.ParseImageRef(in)
		if ok {
			Expect(err).NotTo(HaveOccurred())
		} else {
			Expect(err).To(MatchError(operator.ErrInvalidImageRef))
		}
	},
	Entry("a digest reference", "ghcr.io/gabrosys/plugin-telemetry@sha256:"+hex64, true),
	Entry("a registry with a port", "localhost:5001/plugin-telemetry@sha256:"+hex64, true),
	Entry("a tag", "ghcr.io/gabrosys/plugin-telemetry:v0.1.0", false),
	Entry("a tag and a digest", "ghcr.io/gabrosys/plugin-telemetry:v0.1.0@sha256:"+hex64, false),
	Entry("uppercase hex", "ghcr.io/x@sha256:"+strings.ToUpper(hex64), false),
	Entry("a short digest", "ghcr.io/x@sha256:abc", false),
	Entry("surrounding space", " ghcr.io/x@sha256:"+hex64, false),
	Entry("empty", "", false),
)

var _ = Describe("ImageStore", func() {
	valid := "ghcr.io/gabrosys/plugin-telemetry@sha256:" + hex64

	It("waits for the first valid image", func() {
		store := operator.NewImageStore()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		Expect(store.Wait(ctx)).To(MatchError(context.DeadlineExceeded))

		Expect(store.Set(configMap(map[string]string{operator.SidecarImageKey: valid}))).To(Succeed())
		Expect(store.Wait(context.Background())).To(Succeed())
		Expect(store.Image()).To(Equal(valid))
	})

	It("ignores an invalid first value", func() {
		store := operator.NewImageStore()
		err := store.Set(configMap(map[string]string{operator.SidecarImageKey: "ghcr.io/x:v1"}))
		Expect(err).To(MatchError(operator.ErrInvalidImageRef))
		Expect(store.Image()).To(BeEmpty())
	})

	It("keeps the last valid image", func() {
		store := operator.NewImageStore()
		Expect(store.Set(configMap(map[string]string{operator.SidecarImageKey: valid}))).To(Succeed())

		Expect(store.Set(configMap(map[string]string{"other": "x"}))).To(MatchError(operator.ErrInvalidImageRef))
		Expect(store.Set(nil)).To(MatchError(operator.ErrInvalidImageRef))
		Expect(store.Image()).To(Equal(valid))
	})

	It("is safe for concurrent use", func() {
		store := operator.NewImageStore()
		cm := configMap(map[string]string{operator.SidecarImageKey: valid})

		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				for range 1000 {
					Expect(store.Set(cm)).To(Succeed())
					Expect(store.Image()).To(Equal(valid))
				}
			}()
		}
		wg.Wait()
	})
})

var _ = Describe("image event handler", func() {
	var (
		store   *operator.ImageStore
		handler toolscache.ResourceEventHandlerFuncs
		logs    func() []map[string]any
	)

	validImage := "ghcr.io/x@sha256:" + hex64
	otherImage := "ghcr.io/y@sha256:" + strings.Repeat("0", 64)

	BeforeEach(func(ctx context.Context) {
		var logCtx context.Context
		logCtx, logs = captureAll(ctx)
		store = operator.NewImageStore()
		handler = operator.ImageEventHandler(log.FromContext(logCtx), store)
	})

	It("sets the image on add", func() {
		handler.OnAdd(configMap(map[string]string{operator.SidecarImageKey: validImage}), true)

		Expect(store.Image()).To(Equal(validImage))
		Expect(logs()).NotTo(ContainElement(HaveKey("error")))
	})

	It("replaces the image on a valid update", func() {
		handler.OnAdd(configMap(map[string]string{operator.SidecarImageKey: validImage}), true)
		handler.OnUpdate(nil, configMap(map[string]string{operator.SidecarImageKey: otherImage}))

		Expect(store.Image()).To(Equal(otherImage))
	})

	It("keeps the previous image and logs an error on an update to an invalid value", func() {
		handler.OnAdd(configMap(map[string]string{operator.SidecarImageKey: validImage}), true)
		handler.OnUpdate(nil, configMap(map[string]string{operator.SidecarImageKey: "ghcr.io/x:latest"}))

		Expect(store.Image()).To(Equal(validImage))
		Expect(logs()).To(ContainElement(HaveKey("error")))
	})

	It("logs an error on an unexpected object type", func() {
		handler.OnAdd("not a configmap", true)

		Expect(store.Image()).To(BeEmpty())
		Expect(logs()).To(ContainElement(HaveKeyWithValue("type", "string")))
	})

	DescribeTable("keeps the previous image and logs an error on a delete",
		func(deleted func(*corev1.ConfigMap) any) {
			handler.OnAdd(configMap(map[string]string{operator.SidecarImageKey: validImage}), true)
			handler.OnDelete(deleted(configMap(nil)))

			Expect(store.Image()).To(Equal(validImage))
			Expect(logs()).To(ContainElement(HaveKey("error")))
		},
		Entry("of the object", func(cm *corev1.ConfigMap) any { return cm }),
		Entry("seen as a tombstone", func(cm *corev1.ConfigMap) any {
			return toolscache.DeletedFinalStateUnknown{Key: "cnpg-system/sidecar-image", Obj: cm}
		}),
	)
})
