// Package operator serves the plugin's gRPC identity and lifecycle services and tracks the sidecar image they inject.
package operator

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// SidecarImageKey is the ConfigMap data key holding the sidecar image reference.
const SidecarImageKey = "image"

// ErrInvalidImageRef is returned for a sidecar image that is not a digest reference.
var ErrInvalidImageRef = errors.New("sidecar image must be <repository>@sha256:<64 lowercase hex>")

var errUnexpectedObject = errors.New("unexpected object type")

var errConfigMapDeleted = errors.New("sidecar image configmap deleted, a restarted replica will not serve")

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

// WatchSidecarImage feeds the sidecar image ConfigMap informer to store: it calls store.Set on add and
// update, and a delete keeps the stored value and logs. It does not wait for the informer to sync; the
// cache that owns it must be started by the caller.
func WatchSidecarImage(ctx context.Context, informers cache.Informers, store *ImageStore) error {
	informer, err := informers.GetInformer(ctx, &corev1.ConfigMap{}, cache.BlockUntilSynced(false))
	if err != nil {
		return fmt.Errorf("get configmap informer: %w", err)
	}

	if _, err := informer.AddEventHandler(imageEventHandler(log.FromContext(ctx), store)); err != nil {
		return fmt.Errorf("add configmap event handler: %w", err)
	}

	return nil
}

// imageEventHandler feeds ConfigMap events to store. An invalid ConfigMap or a
// delete (including a DeletedFinalStateUnknown tombstone) keeps the stored image.
func imageEventHandler(logger log.Logger, store *ImageStore) toolscache.ResourceEventHandlerFuncs {
	set := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok {
			logger.Error(errUnexpectedObject, "ignoring event", "type", fmt.Sprintf("%T", obj))
			return
		}
		logger := logger.WithValues("namespace", cm.Namespace, "configmap", cm.Name)
		if err := store.Set(cm); err != nil {
			logger.Error(err, "ignoring sidecar image configmap")
			return
		}
		logger.Info("sidecar image updated", "image", store.Image())
	}

	return toolscache.ResourceEventHandlerFuncs{
		AddFunc:    set,
		UpdateFunc: func(_, obj any) { set(obj) },
		DeleteFunc: func(obj any) {
			namespace, name := deletedConfigMap(obj)
			logger.WithValues("namespace", namespace, "configmap", name).
				Error(errConfigMapDeleted, "keeping the last valid sidecar image")
		},
	}
}

// deletedConfigMap names the ConfigMap of a delete event, reading a tombstone's object or key.
func deletedConfigMap(obj any) (namespace, name string) {
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		if cm, ok := tombstone.Obj.(*corev1.ConfigMap); ok {
			return cm.Namespace, cm.Name
		}
		namespace, name, err := toolscache.SplitMetaNamespaceKey(tombstone.Key)
		if err != nil {
			return "", tombstone.Key
		}

		return namespace, name
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok {
		return cm.Namespace, cm.Name
	}

	return "", ""
}
