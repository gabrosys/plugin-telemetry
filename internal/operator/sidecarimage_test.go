package operator_test

import (
	"context"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
