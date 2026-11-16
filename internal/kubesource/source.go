// Package kubesource is the sidecar's engine.Config: it watches the Kubernetes objects the
// configuration comes from, each by name, and serves their contents as virtual files.
package kubesource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/internal/safe"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

const (
	debounce       = time.Second
	tokenPollTime  = 5 * time.Second
	startRetry     = 5 * time.Second // the wait before a watch whose start panicked, or whose informer died, is started again
	snapshotRounds = 3               // the Cluster; the exporter and the Cluster's references; the exporter's references
)

var errNotSynced = errors.New("watch has not synced")

// Options configure a Source; the CLI checks that none is empty.
type Options struct {
	Namespace, Cluster string // NAMESPACE, CLUSTER_NAME
	APIServer          string // "https://" + KUBERNETES_SERVICE_HOST:PORT
	TokenDir           string // metadata.SidecarTokenDir: token and ca.crt
	Clock              clock.Clock
}

// Source implements engine.Config over named watches. Start must run before the engine uses it.
type Source struct {
	o      Options
	replan chan struct{} // cap 1: a Cluster or exporter event, or new lastGood refs, asks run to reconcile

	mu       sync.Mutex // covers every field below
	entries  map[key]*entry
	subs     map[chan struct{}]struct{} // the wake channel of each Watch call
	lastGood clusterconfig.Refs         // the references of the last Settings that built
	log      log.Logger
	warned   []string // the Build warnings of the last Settings that built
}

// New returns a Source.
func New(o Options) *Source {
	if o.Clock == nil {
		o.Clock = clock.RealClock{}
	}

	return &Source{
		o:       o,
		replan:  make(chan struct{}, 1),
		entries: map[key]*entry{},
		subs:    map[chan struct{}]struct{}{},
		log:     log.GetLogger(),
	}
}

// Name returns "kubernetes".
func (*Source) Name() string { return "kubernetes" }

// Start waits for the token, builds the client and watches until ctx ends; it returns nil then.
func (s *Source) Start(ctx context.Context) error {
	client, err := s.client(ctx)
	if err != nil {
		return err
	}
	if client == nil {
		return nil
	}
	s.run(ctx, dynamicWatcher(client))

	return nil
}

// Snapshot gets every referenced object once, with no watch, for the instance tools; ctx bounds the
// token wait and the Gets.
func Snapshot(ctx context.Context, o Options) (*Source, error) {
	s := New(o)
	client, err := s.client(ctx)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, fmt.Errorf("wait for the sidecar token: %w", ctx.Err())
	}

	return s.snapshot(ctx, client)
}

// snapshot gets the wanted objects in rounds: the Cluster, then the exporter and the Cluster's references,
// then the exporter's. A 404 is a missing object and a 403 a denied one; other errors, and the end of
// ctx, are returned.
func (s *Source) snapshot(ctx context.Context, client dynamic.Interface) (*Source, error) {
	s.mu.Lock()
	s.log = log.FromContext(ctx)
	s.mu.Unlock()

	var failures []error
	for range snapshotRounds {
		if err := ctx.Err(); err != nil {
			failures = append(failures, fmt.Errorf("get the referenced objects: %w", err))
			break
		}
		s.mu.Lock()
		var gets []key
		for k := range s.wantedLocked() {
			if s.entries[k] == nil {
				gets = append(gets, k)
			}
		}
		s.mu.Unlock()
		if len(gets) == 0 {
			break
		}

		for _, k := range gets {
			gvr := resources[k.kind]
			obj, err := client.Resource(gvr).Namespace(s.o.Namespace).Get(ctx, k.name, metav1.GetOptions{})
			e := &entry{}
			switch {
			case err == nil:
				e.synced, e.obj = true, obj
			case apierrors.IsNotFound(err):
				e.synced = true
			case apierrors.IsForbidden(err):
				e.denied = fmt.Errorf("%w: %w", sources.ErrDenied, err)
			default:
				failures = append(failures, fmt.Errorf("get %s %s: %w", gvr.Resource, k.name, err))
			}
			s.mu.Lock()
			s.entries[k] = e
			s.mu.Unlock()
		}
	}
	if err := errors.Join(failures...); err != nil {
		return nil, err
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
			logger.Info("waiting for the sidecar token", "path", path, "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return false
		case <-s.o.Clock.After(tokenPollTime):
		}
	}
}

// run alone starts and stops the watches: it reconciles at start, on each replan and startRetry after a
// watch start panicked, and stops every watch when ctx ends.
func (s *Source) run(ctx context.Context, w watcher) {
	s.mu.Lock()
	s.log = log.FromContext(ctx)
	s.mu.Unlock()

	for {
		var retry <-chan time.Time
		if s.reconcile(ctx, w) {
			retry = s.o.Clock.After(startRetry)
		}
		select {
		case <-ctx.Done():
			s.reconcile(ctx, nil)
			return
		case <-s.replan:
		case <-retry:
		}
	}
}

// reconcile starts with w the watches the wanted set adds and stops those it drops; a nil w stops all.
// It reports whether to run again later: a panic in it, or in a watch start, which then has no entry
// so that the next reconcile starts it again.
func (s *Source) reconcile(ctx context.Context, w watcher) (retry bool) {
	logger := s.logger()
	panicked := safe.Call(logger, "reconcile", func() {
		stops, starts := s.diff(w)
		for _, stop := range stops {
			// One stop that panics must not leave the others, or the starts, undone.
			safe.Call(logger, "stop of a watch", stop)
		}
		for k, e := range starts {
			var stop func()
			if safe.Call(logger, "start of the watch on "+string(k.kind)+" "+k.name, func() {
				stop = w(ctx, resources[k.kind], s.o.Namespace, k.name, s.handler(ctx, k, e))
			}) {
				retry = true
				s.forget(k, e)
				continue
			}
			s.mu.Lock()
			e.stop = stop
			s.mu.Unlock()
		}
	})
	return retry || panicked
}

// diff drops the entries the wanted set no longer has, returning their stops, and adds entries for the
// watches to start.
func (s *Source) diff(w watcher) (stops []func(), starts map[key]*entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wanted := map[key]struct{}{}
	if w != nil {
		wanted = s.wantedLocked()
	}
	for k, e := range s.entries {
		if _, ok := wanted[k]; ok {
			continue
		}
		if e.stop != nil {
			stops = append(stops, e.stop)
		}
		delete(s.entries, k)
	}
	starts = map[key]*entry{}
	for k := range wanted {
		if _, ok := s.entries[k]; !ok {
			e := &entry{}
			s.entries[k] = e
			starts[k] = e
		}
	}
	return stops, starts
}

// forget removes e, the entry of a watch that did not start.
func (s *Source) forget(k key, e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries[k] == e {
		delete(s.entries, k)
	}
}

// logger returns the logger set by run or snapshot.
func (s *Source) logger() logr.Logger {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.GetLogger()
}

// wantedLocked is the Cluster, the references of the newest Cluster and exporter, and lastGood. While
// the Cluster or the exporter is denied or can't be decoded, it also keeps every current watch.
func (s *Source) wantedLocked() map[key]struct{} {
	wanted := map[key]struct{}{{kind: kindCluster, name: s.o.Cluster}: {}}
	addRefs(wanted, s.lastGood)

	cluster, params, exporter, err := s.resolveLocked()
	if err != nil && !errors.Is(err, engine.ErrNoConfiguration) && !errors.Is(err, errNotSynced) {
		for k := range s.entries {
			wanted[k] = struct{}{}
		}
		return wanted
	}
	if cluster != nil {
		addRefs(wanted, clusterconfig.References(cluster, params, exporter))
	}

	return wanted
}

func addRefs(wanted map[key]struct{}, refs clusterconfig.Refs) {
	if refs.Exporter != "" {
		wanted[key{kind: kindExporter, name: refs.Exporter}] = struct{}{}
	}
	for _, n := range refs.ConfigMaps {
		wanted[key{kind: kindConfigMap, name: n}] = struct{}{}
	}
	for _, n := range refs.Secrets {
		wanted[key{kind: kindSecret, name: n}] = struct{}{}
	}
	for _, n := range refs.QuerySets {
		wanted[key{kind: kindQuerySet, name: n}] = struct{}{}
	}
}

// resolveLocked decodes the Cluster, its plugin parameters and the exporter they name. The Cluster is
// nil unless the plugin is enabled with valid parameters; the exporter is nil while it isn't known
// (errNotSynced), doesn't exist (engine.ErrNoConfiguration), is denied or can't be decoded.
func (s *Source) resolveLocked() (*apiv1.Cluster, podpatch.Params, *v1alpha1.TelemetryExporter, error) {
	obj, err := s.objectLocked(key{kind: kindCluster, name: s.o.Cluster})
	switch {
	case errors.Is(err, errNotSynced):
		return nil, podpatch.Params{}, nil, noConfiguration("cluster %q is not known yet", s.o.Cluster)
	case err != nil:
		return nil, podpatch.Params{}, nil, err
	case obj == nil:
		return nil, podpatch.Params{}, nil, noConfiguration("cluster %q does not exist", s.o.Cluster)
	}
	cluster := &apiv1.Cluster{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, cluster); err != nil {
		return nil, podpatch.Params{}, nil, fmt.Errorf("decode cluster %q: %w", s.o.Cluster, err)
	}

	plugin, ok := clusterconfig.EnabledPlugin(cluster)
	if !ok {
		return nil, podpatch.Params{}, nil, noConfiguration("the plugin is not enabled for cluster %q", s.o.Cluster)
	}
	parsed, err := podpatch.ParseParams(plugin.Parameters)
	if err != nil {
		return nil, podpatch.Params{}, nil, noConfiguration("%v", err)
	}
	params := parsed.Params

	obj, err = s.objectLocked(key{kind: kindExporter, name: params.Exporter})
	switch {
	case err != nil:
		return cluster, params, nil, err
	case obj == nil:
		return cluster, params, nil, noConfiguration("telemetryexporter %q does not exist", params.Exporter)
	}
	exporter := &v1alpha1.TelemetryExporter{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, exporter); err != nil {
		return cluster, params, nil, fmt.Errorf("decode telemetryexporter %q: %w", params.Exporter, err)
	}

	return cluster, params, exporter, nil
}

// objectLocked returns a watched object: nil when it doesn't exist; an error when its watch has
// not reported yet or was denied.
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
// Watch errors are only logged: the informer lists again and reports what it finds. A panic in a
// report is logged and the next report is handled.
func (s *Source) handler(ctx context.Context, k key, e *entry) handler {
	watchOn := string(k.kind) + " " + k.name
	return handler{
		set: func(obj *unstructured.Unstructured) {
			safe.Call(s.logger(), "report of the watch on "+watchOn, func() { s.set(k, e, obj) })
		},
		fail: func(err error) {
			safe.Call(s.logger(), "error of the watch on "+watchOn, func() { s.fail(k, e, err) })
		},
		crashed: func() { s.crashed(ctx, k, e) },
	}
}

// crashed forgets the watch whose informer died and asks for a reconcile startRetry later, which starts it again.
func (s *Source) crashed(ctx context.Context, k key, e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries[k] != e {
		return
	}
	delete(s.entries, k)
	retry := s.o.Clock.After(startRetry)
	go func() {
		select {
		case <-retry:
			s.askReplan()
		case <-ctx.Done():
		}
	}()
}

func (s *Source) set(k key, e *entry, obj *unstructured.Unstructured) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries[k] != e {
		return
	}
	e.synced, e.obj, e.denied, e.lastErr = true, obj, nil, ""
	s.wakeLocked()
	if k.kind == kindCluster || k.kind == kindExporter {
		s.askReplan()
	}
}

func (s *Source) fail(k key, e *entry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries[k] != e {
		return
	}
	if errors.Is(err, sources.ErrDenied) {
		e.lastErr = ""
		if e.denied == nil {
			s.log.Info("access to object denied", "kind", k.kind, "name", k.name)
			s.wakeLocked()
		}
		e.denied = err
		return
	}
	if err.Error() == e.lastErr {
		return
	}
	e.lastErr = err.Error()
	s.log.Error(err, "watch failed", "kind", k.kind, "name", k.name)
}

// askReplan wakes run without blocking; a pending request already covers this one.
func (s *Source) askReplan() {
	select {
	case s.replan <- struct{}{}:
	default:
	}
}

func (s *Source) wakeLocked() {
	for wake := range s.subs {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// Watch calls notify once it is watching, then once debounce after the first event of a burst on a
// watched object, so a steady stream of events cannot postpone it. It ignores files: the objects it watches follow the settings. It returns nil when ctx ends.
func (s *Source) Watch(ctx context.Context, _ func() []string, notify func()) error {
	wake := make(chan struct{}, 1)
	s.mu.Lock()
	s.subs[wake] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, wake)
		s.mu.Unlock()
	}()

	timer := s.o.Clock.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C()
	}
	defer timer.Stop()

	pending := false // the timer is armed
	notify()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
			if !pending {
				timer.Reset(debounce)
				pending = true
			}
		case <-timer.C():
			pending = false
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
	obj, err := s.objectLocked(k)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}

	return content(k, dataKey, obj)
}

// Sensitive reports whether the virtual file holds Secret data.
func (*Source) Sensitive(name string) bool { return strings.HasPrefix(name, "secret/") }

// Settings builds the settings from the watched Cluster, exporter and the objects they name, and keeps
// their references watched until settings that name others build or there is no configuration. The
// bytes are the built settings as JSON, which only the engine's digest reads.
func (s *Source) Settings() (settings.Settings, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	in, err := s.inputsLocked()
	if errors.Is(err, errNotSynced) || errors.Is(err, sources.ErrDenied) {
		err = fmt.Errorf("%w: %w", engine.ErrNotReady, err)
	}
	if err != nil {
		if errors.Is(err, engine.ErrNoConfiguration) && s.lastGood.Cluster != "" {
			s.lastGood = clusterconfig.Refs{}
			s.askReplan()
		}
		return settings.Settings{}, nil, err
	}
	built, warnings, err := clusterconfig.Build(in)
	if err != nil {
		return settings.Settings{}, nil, fmt.Errorf("settings: %w", err)
	}
	data, err := json.Marshal(built)
	if err != nil {
		return settings.Settings{}, nil, fmt.Errorf("encode settings: %w", err)
	}
	for _, w := range warnings {
		if !slices.Contains(s.warned, w) {
			s.log.Warning("settings warning", "warning", w)
		}
	}
	s.warned = warnings
	if refs := clusterconfig.References(in.Cluster, in.Params, in.Exporter); !reflect.DeepEqual(refs, s.lastGood) {
		s.lastGood = refs
		s.askReplan()
	}

	return built, data, nil
}

func noConfiguration(format string, args ...any) error {
	return fmt.Errorf("%w: %s", engine.ErrNoConfiguration, fmt.Sprintf(format, args...))
}

func (s *Source) inputsLocked() (clusterconfig.Inputs, error) {
	cluster, params, exporter, err := s.resolveLocked()
	if err != nil {
		return clusterconfig.Inputs{}, err
	}
	in := clusterconfig.Inputs{Cluster: cluster, Params: params, Exporter: exporter}

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
	if params.Auth == podpatch.AuthPassword && params.PasswordSecret != "" {
		pw, err := s.dataLocked(kindSecret, params.PasswordSecret)
		if err != nil {
			return clusterconfig.Inputs{}, err
		}
		in.PasswordUsername = pw[clusterconfig.PasswordUsernameKey]
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
