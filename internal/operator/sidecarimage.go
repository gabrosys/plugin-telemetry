// Package operator serves the plugin's gRPC identity and lifecycle services and tracks the sidecar image they inject.
package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/distribution/reference"
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

// digestRef reports whether image is a repository with a sha256 digest and no tag, by the distribution reference grammar.
func digestRef(image string) bool {
	ref, err := reference.Parse(image)
	if err != nil {
		return false
	}
	c, canonical := ref.(reference.Canonical)
	_, tagged := ref.(reference.Tagged)
	return canonical && !tagged && c.Digest().Algorithm() == "sha256"
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
	image, ok := cm.Data[SidecarImageKey]
	if !ok {
		return fmt.Errorf("configmap %s/%s has no key %q: %w", cm.Namespace, cm.Name, SidecarImageKey, ErrInvalidImageRef)
	}
	if !digestRef(image) {
		return fmt.Errorf("configmap %s/%s key %q is %q: %w", cm.Namespace, cm.Name, SidecarImageKey, image, ErrInvalidImageRef)
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

// WatchSidecarImage feeds the sidecar image ConfigMap informer, whose cache holds only the ConfigMap
// namespace/name, to store: it calls store.Set on add and update, and a delete keeps the stored value and
// logs. It does not wait for the informer to sync; the cache that owns it must be started by the caller.
func WatchSidecarImage(ctx context.Context, informers cache.Informers, store *ImageStore, namespace, name string) error {
	informer, err := informers.GetInformer(ctx, &corev1.ConfigMap{}, cache.BlockUntilSynced(false))
	if err != nil {
		return fmt.Errorf("get configmap informer: %w", err)
	}

	if _, err := informer.AddEventHandler(imageEventHandler(log.FromContext(ctx), store, namespace, name)); err != nil {
		return fmt.Errorf("add configmap event handler: %w", err)
	}

	return nil
}

// imageEventHandler feeds ConfigMap events to store; an invalid ConfigMap or a delete keeps the stored image.
func imageEventHandler(logger log.Logger, store *ImageStore, namespace, name string) toolscache.ResourceEventHandlerFuncs {
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
		logger.Info("sidecar image updated", "image", cm.Data[SidecarImageKey])
	}

	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: set,
		UpdateFunc: func(oldObj, newObj any) {
			if sameImage(oldObj, newObj) {
				return
			}
			set(newObj)
		},
		DeleteFunc: func(any) {
			logger.WithValues("namespace", namespace, "configmap", name).
				Error(errConfigMapDeleted, "keeping the last valid sidecar image")
		},
	}
}

// sameImage reports whether two ConfigMaps hold the same image reference, as on a resync.
func sameImage(a, b any) bool {
	oldCM, okOld := a.(*corev1.ConfigMap)
	newCM, okNew := b.(*corev1.ConfigMap)

	return okOld && okNew && oldCM.Data[SidecarImageKey] == newCM.Data[SidecarImageKey]
}
