// Package kubesource is the sidecar's engine.Config: it watches the Kubernetes objects the
// configuration comes from, each by name, and serves their contents as virtual files.
package kubesource

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/machinery/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

const (
	debounce      = time.Second
	tokenPollTime = 5 * time.Second
)

var errNotSynced = errors.New("watch has not synced")

// Options configure a Source.
type Options struct {
	Namespace, Cluster string // NAMESPACE, CLUSTER_NAME
	APIServer          string // "https://" + KUBERNETES_SERVICE_HOST:PORT
	TokenDir           string // metadata.SidecarTokenDir: token and ca.crt
	Clock              clock.Clock
}

// subscriber is a Watch call.
type subscriber struct {
	wake    chan struct{}
	onError func(error)
}

// Source implements engine.Config over named watches. Start must run before the engine uses it.
type Source struct {
	o Options

	mu          sync.Mutex // covers every field below
	entries     map[key]*entry
	subs        map[*subscriber]struct{}
	w           watcher
	ctx         context.Context //nolint:containedctx // the lifetime of the watches, set once by begin
	log         log.Logger
	reconciling bool // a goroutine is running reconcile
	dirty       bool // an event arrived while it was
}

// New returns a Source; it fails only when an option is empty.
func New(o Options) (*Source, error) {
	switch {
	case o.Namespace == "":
		return nil, errors.New("new kubernetes source: empty namespace")
	case o.Cluster == "":
		return nil, errors.New("new kubernetes source: empty cluster name")
	case o.APIServer == "":
		return nil, errors.New("new kubernetes source: empty API server address")
	case o.TokenDir == "":
		return nil, errors.New("new kubernetes source: empty token directory")
	}
	if o.Clock == nil {
		o.Clock = clock.RealClock{}
	}

	return &Source{
		o:       o,
		entries: map[key]*entry{},
		subs:    map[*subscriber]struct{}{},
		log:     log.GetLogger(),
	}, nil
}

// Name returns "kubernetes".
func (*Source) Name() string { return "kubernetes" }

// Start waits for the token, builds the client and watches until ctx ends.
func (s *Source) Start(ctx context.Context) error {
	client, err := s.client(ctx)
	if err != nil {
		return err
	}
	if client == nil {
		return nil
	}
	s.begin(ctx, dynamicWatcher(client))
	<-ctx.Done()
	s.stopAll()

	return nil
}

// Snapshot gets every referenced object once, with no watch, for the instance tools.
func Snapshot(ctx context.Context, o Options) (*Source, error) {
	s, err := New(o)
	if err != nil {
		return nil, err
	}
	client, err := s.client(ctx)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, fmt.Errorf("snapshot: %w", ctx.Err())
	}

	var (
		mu   sync.Mutex // covers errs
		errs []error
	)
	get := func(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string, h handler) func() {
		obj, err := client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		switch {
		case err == nil:
			h.set(obj)
		case apierrors.IsNotFound(err):
			h.set(nil)
		case apierrors.IsForbidden(err):
			h.fail(fmt.Errorf("%w: %w", sources.ErrDenied, err))
		default:
			mu.Lock()
			errs = append(errs, fmt.Errorf("get %s %s: %w", gvr.Resource, name, err))
			mu.Unlock()
			h.fail(err)
		}
		return func() {}
	}
	s.begin(ctx, get)

	mu.Lock()
	defer mu.Unlock()
	if len(errs) > 0 {
		return nil, fmt.Errorf("snapshot: %w", errors.Join(errs...))
	}

	return s, nil
}

// client waits for the token and returns the dynamic client; nil when ctx ended first.
func (s *Source) client(ctx context.Context) (dynamic.Interface, error) {
	if !s.waitForToken(ctx) {
		return nil, nil //nolint:nilnil // the context ended while waiting, which is not an error
	}
	cfg := &rest.Config{
		Host:            s.o.APIServer,
		BearerTokenFile: filepath.Join(s.o.TokenDir, "token"),
		TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(s.o.TokenDir, "ca.crt")},
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	return client, nil
}

// waitForToken polls for the token file, which kubelet fills after the Pod starts; false when ctx ends first.
func (s *Source) waitForToken(ctx context.Context) bool {
	logger := log.FromContext(ctx)
	path := filepath.Join(s.o.TokenDir, "token")
	for waited := false; ; waited = true {
		_, err := os.Stat(path)
		if err == nil {
			return true
		}
		if !waited {
			logger.Info("waiting for the sidecar token", "path", path)
		}
		select {
		case <-ctx.Done():
			return false
		case <-s.o.Clock.After(tokenPollTime):
		}
	}
}

// begin starts watching the Cluster with w and returns.
func (s *Source) begin(ctx context.Context, w watcher) {
	s.mu.Lock()
	s.ctx, s.w, s.log = ctx, w, log.FromContext(ctx)
	s.mu.Unlock()

	s.reconcile()
}

func (s *Source) stopAll() {
	s.mu.Lock()
	var stops []func()
	for k, e := range s.entries {
		e.gone = true
		if e.stop != nil {
			stops = append(stops, e.stop)
		}
		delete(s.entries, k)
	}
	s.mu.Unlock()

	for _, stop := range stops {
		stop()
	}
}

type start struct {
	k key
	e *entry
}

// reconcile starts the watches the Cluster and the exporter call for and stops the others. Events that
// arrive while it runs, including those a watcher reports synchronously, make it plan again.
func (s *Source) reconcile() {
	s.mu.Lock()
	if s.reconciling {
		s.dirty = true
		s.mu.Unlock()
		return
	}
	s.reconciling = true
	for {
		s.dirty = false
		starts, stops := s.planLocked()
		w, ctx := s.w, s.ctx
		s.mu.Unlock()

		for _, stop := range stops {
			stop()
		}
		for _, st := range starts {
			stop := w(ctx, resources[st.k.kind], s.o.Namespace, st.k.name, s.handler(st.k, st.e))
			s.mu.Lock()
			if st.e.gone {
				s.mu.Unlock()
				stop()
				continue
			}
			st.e.stop = stop
			s.mu.Unlock()
		}

		s.mu.Lock()
		if !s.dirty {
			s.reconciling = false
			s.mu.Unlock()
			return
		}
	}
}

func (s *Source) planLocked() ([]start, []func()) {
	wanted := s.wantedLocked()

	var stops []func()
	for k, e := range s.entries {
		if _, ok := wanted[k]; ok {
			continue
		}
		e.gone = true
		if e.stop != nil {
			stops = append(stops, e.stop)
		}
		delete(s.entries, k)
	}

	var starts []start
	for k := range wanted {
		if _, ok := s.entries[k]; ok {
			continue
		}
		e := &entry{}
		s.entries[k] = e
		starts = append(starts, start{k: k, e: e})
	}

	return starts, stops
}

// wantedLocked is the Cluster, plus the exporter and everything References lists once the Cluster is known.
func (s *Source) wantedLocked() map[key]struct{} {
	wanted := map[key]struct{}{{kind: kindCluster, name: s.o.Cluster}: {}}
	if s.ctx == nil || s.ctx.Err() != nil {
		return map[key]struct{}{}
	}

	cluster, err := s.clusterLocked()
	if err != nil || cluster == nil {
		return wanted
	}
	plugin, ok := clusterconfig.EnabledPlugin(cluster)
	if !ok {
		return wanted
	}
	parsed, err := podpatch.ParseParams(plugin.Parameters)
	if err != nil {
		return wanted
	}
	exporter, _ := s.exporterLocked(parsed.Params.Exporter)

	refs := clusterconfig.References(cluster, parsed.Params, exporter)
	wanted[key{kind: kindExporter, name: refs.Exporter}] = struct{}{}
	for _, n := range refs.ConfigMaps {
		wanted[key{kind: kindConfigMap, name: n}] = struct{}{}
	}
	for _, n := range refs.Secrets {
		wanted[key{kind: kindSecret, name: n}] = struct{}{}
	}
	for _, n := range refs.QuerySets {
		wanted[key{kind: kindQuerySet, name: n}] = struct{}{}
	}

	return wanted
}

// clusterLocked returns the decoded Cluster; nil when it isn't known or doesn't exist.
func (s *Source) clusterLocked() (*apiv1.Cluster, error) {
	obj, err := s.objectLocked(key{kind: kindCluster, name: s.o.Cluster})
	if err != nil || obj == nil {
		return nil, err
	}
	c := &apiv1.Cluster{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, c); err != nil {
		return nil, fmt.Errorf("decode cluster %q: %w", s.o.Cluster, err)
	}

	return c, nil
}

// exporterLocked returns the decoded exporter; nil when it isn't known or doesn't exist.
func (s *Source) exporterLocked(name string) (*v1alpha1.TelemetryExporter, error) {
	obj, err := s.objectLocked(key{kind: kindExporter, name: name})
	if err != nil || obj == nil {
		return nil, err
	}
	e := &v1alpha1.TelemetryExporter{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, e); err != nil {
		return nil, fmt.Errorf("decode telemetryexporter %q: %w", name, err)
	}

	return e, nil
}

// objectLocked returns a watched object: nil when it doesn't exist; an error when its watch has
// not synced or was denied.
func (s *Source) objectLocked(k key) (*unstructured.Unstructured, error) {
	e := s.entries[k]
	switch {
	case e == nil || !e.synced && e.denied == nil:
		return nil, fmt.Errorf("%s %q: %w", k.kind, k.name, errNotSynced)
	case e.denied != nil:
		return nil, fmt.Errorf("%s %q: %w", k.kind, k.name, e.denied)
	}

	return e.obj, nil
}

// handler returns what a watch on k reports to; reports from a watch that was stopped are dropped.
func (s *Source) handler(k key, e *entry) handler {
	return handler{
		set: func(obj *unstructured.Unstructured) {
			s.mu.Lock()
			if s.entries[k] != e {
				s.mu.Unlock()
				return
			}
			e.synced, e.obj, e.denied = true, obj, nil
			s.wakeLocked()
			s.mu.Unlock()

			if k.kind == kindCluster || k.kind == kindExporter {
				s.reconcile()
			}
		},
		fail: func(err error) {
			s.mu.Lock()
			if s.entries[k] != e {
				s.mu.Unlock()
				return
			}
			if errors.Is(err, sources.ErrDenied) {
				changed := e.denied == nil
				e.denied = err
				if changed {
					s.log.Info("access to object denied", "kind", k.kind, "name", k.name)
					s.wakeLocked()
				}
				s.mu.Unlock()
				return
			}
			s.log.Error(err, "watch failed", "kind", k.kind, "name", k.name)
			var handlers []func(error)
			for sub := range s.subs {
				handlers = append(handlers, sub.onError)
			}
			s.mu.Unlock()

			for _, h := range handlers {
				h(fmt.Errorf("watch %s %q: %w", k.kind, k.name, err))
			}
		},
	}
}

func (s *Source) wakeLocked() {
	for sub := range s.subs {
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

// Watch calls notify, debounced, after any event on a watched object. It returns nil when ctx ends.
func (s *Source) Watch(ctx context.Context, _ []string, notify func(), opts ...sources.WatchOption) error {
	ready, onError := sources.WatchHooks(opts...)
	sub := &subscriber{wake: make(chan struct{}, 1), onError: onError}
	s.mu.Lock()
	s.subs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
	}()

	timer := s.o.Clock.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C()
	}
	defer timer.Stop()

	ready()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sub.wake:
			timer.Reset(debounce)
		case <-timer.C():
			notify()
		}
	}
}

// ReadFile serves a ConfigMap key, a Secret key or a MetricQuerySet from the watched objects.
func (s *Source) ReadFile(name string) ([]byte, error) {
	k, dataKey, ok := virtualFile(name)
	if !ok {
		return nil, fmt.Errorf("file %q: %w", name, fs.ErrNotExist)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries[k] == nil {
		return nil, fmt.Errorf("file %q: %w", name, fs.ErrNotExist)
	}
	obj, err := s.objectLocked(k)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}

	return content(k, dataKey, obj)
}

// Settings builds the settings from the watched Cluster, exporter and the objects they name.
func (s *Source) Settings() (settings.Settings, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	in, err := s.inputsLocked()
	if err != nil {
		return settings.Settings{}, nil, err
	}
	data, err := canonical(in)
	if err != nil {
		return settings.Settings{}, nil, err
	}
	built, warnings, err := clusterconfig.Build(in)
	if err != nil {
		return settings.Settings{}, data, fmt.Errorf("settings: %w", err)
	}
	for _, w := range warnings {
		s.log.Info("settings warning", "warning", w)
	}

	return built, data, nil
}

func noConfiguration(format string, args ...any) error {
	return fmt.Errorf("%w: %s", engine.ErrNoConfiguration, fmt.Sprintf(format, args...))
}

func (s *Source) inputsLocked() (clusterconfig.Inputs, error) {
	cluster, err := s.clusterLocked()
	switch {
	case errors.Is(err, errNotSynced):
		return clusterconfig.Inputs{}, noConfiguration("cluster %q is not known yet", s.o.Cluster)
	case err != nil:
		return clusterconfig.Inputs{}, err
	case cluster == nil:
		return clusterconfig.Inputs{}, noConfiguration("cluster %q does not exist", s.o.Cluster)
	}

	plugin, ok := clusterconfig.EnabledPlugin(cluster)
	if !ok {
		return clusterconfig.Inputs{}, noConfiguration("the plugin is not enabled for cluster %q", s.o.Cluster)
	}
	parsed, err := podpatch.ParseParams(plugin.Parameters)
	if err != nil {
		return clusterconfig.Inputs{}, noConfiguration("%v", err)
	}

	in := clusterconfig.Inputs{Cluster: cluster, Params: parsed.Params}
	if in.Exporter, err = s.exporterLocked(parsed.Params.Exporter); err != nil {
		return clusterconfig.Inputs{}, err
	}
	if in.Exporter == nil {
		return clusterconfig.Inputs{}, noConfiguration("telemetryexporter %q does not exist", parsed.Params.Exporter)
	}

	if env := in.Exporter.Spec.EnvFrom; env != nil {
		if env.ConfigMap != nil {
			if in.EnvConfigMap, err = s.dataLocked(kindConfigMap, env.ConfigMap.Name); err != nil {
				return clusterconfig.Inputs{}, err
			}
		}
		if env.Secret != nil {
			if in.EnvSecret, err = s.dataLocked(kindSecret, env.Secret.Name); err != nil {
				return clusterconfig.Inputs{}, err
			}
		}
	}
	if parsed.Params.Auth == podpatch.AuthPassword && parsed.Params.PasswordSecret != "" {
		pw, err := s.dataLocked(kindSecret, parsed.Params.PasswordSecret)
		if err != nil {
			return clusterconfig.Inputs{}, err
		}
		in.PasswordUsername = pw["username"]
	}

	return in, nil
}

// dataLocked returns the data of a watched ConfigMap or Secret as strings; nil when it doesn't exist.
func (s *Source) dataLocked(kd kind, name string) (map[string]string, error) {
	k := key{kind: kd, name: name}
	obj, err := s.objectLocked(k)
	if err != nil {
		return nil, err
	}
	data, err := objectData(k, obj)
	if err != nil {
		return nil, err
	}

	return stringData(data), nil
}
