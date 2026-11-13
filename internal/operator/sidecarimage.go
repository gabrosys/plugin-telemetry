// Package operator holds the plugin's runtime state shared by the lifecycle and operator servers.
package operator

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SidecarImageKey is the ConfigMap data key holding the sidecar image reference.
const SidecarImageKey = "image"

// ErrInvalidImageRef is returned for a sidecar image that is not a digest reference.
var ErrInvalidImageRef = errors.New("sidecar image must be <repository>@sha256:<64 lowercase hex>")

var imageRefPattern = regexp.MustCompile(
	`^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*@sha256:[0-9a-f]{64}$`)

// ParseImageRef returns s when it is a digest reference, and ErrInvalidImageRef otherwise.
func ParseImageRef(s string) (string, error) {
	if !imageRefPattern.MatchString(s) {
		return "", fmt.Errorf("parse image reference %q: %w", s, ErrInvalidImageRef)
	}
	return s, nil
}

// ImageStore holds the current sidecar image and signals when the first valid one arrives.
type ImageStore struct {
	mu    sync.RWMutex // guards image
	image string
	ready chan struct{}
	once  sync.Once
}

// NewImageStore returns an empty store.
func NewImageStore() *ImageStore {
	return &ImageStore{ready: make(chan struct{})}
}

// Set stores the image from cm. An invalid ConfigMap leaves the previous value in place.
func (s *ImageStore) Set(cm *corev1.ConfigMap) error {
	if cm == nil {
		return fmt.Errorf("sidecar image configmap is nil: %w", ErrInvalidImageRef)
	}
	raw, ok := cm.Data[SidecarImageKey]
	if !ok {
		return fmt.Errorf("configmap %s/%s has no key %q: %w", cm.Namespace, cm.Name, SidecarImageKey, ErrInvalidImageRef)
	}
	image, err := ParseImageRef(raw)
	if err != nil {
		return fmt.Errorf("configmap %s/%s key %q: %w", cm.Namespace, cm.Name, SidecarImageKey, err)
	}

	s.mu.Lock()
	s.image = image
	s.mu.Unlock()
	s.once.Do(func() { close(s.ready) })
	return nil
}

// Image returns the current image, or "" until the first valid Set.
func (s *ImageStore) Image() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.image
}

// Wait blocks until a valid image is stored, or returns ctx.Err().
func (s *ImageStore) Wait(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for sidecar image: %w", ctx.Err())
	}
}

// WatchSidecarImage runs a cache for one ConfigMap and calls store.Set on add and update.
// A delete keeps the stored value and logs. It returns when ctx ends.
func WatchSidecarImage(
	ctx context.Context,
	cfg *rest.Config,
	namespace, name string,
	store *ImageStore,
) error {
	logger := log.FromContext(ctx).WithValues("namespace", namespace, "configmap", name)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register core/v1 scheme: %w", err)
	}

	c, err := cache.New(cfg, cache.Options{
		Scheme:            scheme,
		DefaultNamespaces: map[string]cache.Config{namespace: {}},
		ByObject: map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {Field: fields.OneTermEqualSelector("metadata.name", name)},
		},
	})
	if err != nil {
		return fmt.Errorf("create configmap cache: %w", err)
	}

	informer, err := c.GetInformer(ctx, &corev1.ConfigMap{})
	if err != nil {
		return fmt.Errorf("get configmap informer: %w", err)
	}

	set := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok {
			logger.Error(nil, "unexpected object type", "type", fmt.Sprintf("%T", obj))
			return
		}
		if err := store.Set(cm); err != nil {
			logger.Error(err, "ignoring sidecar image configmap")
			return
		}
		logger.Info("sidecar image updated", "image", store.Image())
	}
	_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    set,
		UpdateFunc: func(_, obj any) { set(obj) },
		DeleteFunc: func(any) {
			logger.Info("sidecar image configmap deleted, keeping the last valid image")
		},
	})
	if err != nil {
		return fmt.Errorf("add configmap event handler: %w", err)
	}

	if err := c.Start(ctx); err != nil {
		return fmt.Errorf("run configmap cache: %w", err)
	}
	return nil
}
