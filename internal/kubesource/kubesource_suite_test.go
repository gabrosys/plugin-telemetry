package kubesource_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/gabrosys/plugin-telemetry/internal/kubesource"
	"github.com/gabrosys/plugin-telemetry/internal/safe"
)

func TestKubesource(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kubesource Suite")
}

type fakeWatch struct{ h kubesource.Handler }

// fakeWatcher records the watches a Source starts and lets specs feed them.
type fakeWatcher struct {
	mu       sync.Mutex
	handlers map[string]*fakeWatch
	panics   map[string]bool // the watches whose next start panics
	starts   map[string]int  // how often each watch was started
	stops    map[string]bool // the watches whose stop panics
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{handlers: map[string]*fakeWatch{}, panics: map[string]bool{}, starts: map[string]int{}, stops: map[string]bool{}}
}

func watchKey(resource, name string) string { return resource + "/" + name }

func (f *fakeWatcher) watch(
	_ context.Context, gvr schema.GroupVersionResource, namespace, name string, h kubesource.Handler,
) func() {
	Expect(namespace).To(Equal("ns"))
	f.mu.Lock()
	defer f.mu.Unlock()
	k := watchKey(gvr.Resource, name)
	if f.panics[k] {
		delete(f.panics, k)
		panic("watch start broke")
	}
	f.starts[k]++
	fw := &fakeWatch{h: h}
	f.handlers[k] = fw
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.handlers[k] == fw {
			delete(f.handlers, k)
		}
		if f.stops[k] {
			panic("watch stop broke")
		}
	}
}

// panicOnce makes the next start of the watch on resource/name panic.
func (f *fakeWatcher) panicOnce(resource, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.panics[watchKey(resource, name)] = true
}

// panicOnStop makes the stop of the watch on resource/name panic.
func (f *fakeWatcher) panicOnStop(resource, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops[watchKey(resource, name)] = true
}

// started returns how often the watch on resource/name was started.
func (f *fakeWatcher) started(resource, name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts[watchKey(resource, name)]
}

// active returns the watched objects as "<resource>/<name>".
func (f *fakeWatcher) active() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.handlers))
	for k := range f.handlers {
		out = append(out, k)
	}
	return out
}

// handler waits for the watch on resource/name, which the Source starts on its own goroutine.
func (f *fakeWatcher) handler(resource, name string) kubesource.Handler {
	GinkgoHelper()
	var h kubesource.Handler
	Eventually(func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		w, ok := f.handlers[watchKey(resource, name)]
		if ok {
			h = w.h
		}
		return ok
	}).Should(BeTrue(), func() string {
		f.mu.Lock()
		defer f.mu.Unlock()
		return fmt.Sprintf("no watch on %s/%s; active: %v", resource, name, f.handlersLocked())
	})
	return h
}

func (f *fakeWatcher) handlersLocked() []string {
	out := make([]string, 0, len(f.handlers))
	for k := range f.handlers {
		out = append(out, k)
	}
	return out
}

func (f *fakeWatcher) set(resource, name string, obj *unstructured.Unstructured) {
	GinkgoHelper()
	f.handler(resource, name).Set(obj)
}

func (f *fakeWatcher) fail(resource, name string, err error) {
	GinkgoHelper()
	f.handler(resource, name).Fail(err)
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

var errForbidden = errors.New("RBAC: access denied")

func metaCreate() metav1.CreateOptions { return metav1.CreateOptions{} }

var _ = BeforeEach(safe.Reset)
