package kubesource

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
)

// kind is a watched resource; the ConfigMap, Secret and MetricQuerySet values are also the prefixes of their virtual files.
type kind string

const (
	kindCluster   kind = "cluster"
	kindExporter  kind = "telemetryexporter"
	kindConfigMap kind = "configmap"
	kindSecret    kind = "secret"
	kindQuerySet  kind = "metricqueryset"
)

var resources = map[kind]schema.GroupVersionResource{
	kindCluster:   {Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"},
	kindExporter:  {Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version, Resource: "telemetryexporters"},
	kindConfigMap: {Version: "v1", Resource: "configmaps"},
	kindSecret:    {Version: "v1", Resource: "secrets"},
	kindQuerySet:  {Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version, Resource: "metricquerysets"},
}

type key struct {
	kind kind
	name string
}

// entry is the state of one watch.
type entry struct {
	synced  bool
	obj     *unstructured.Unstructured // nil when the object doesn't exist
	denied  error                      // the last watch error was a 403
	lastErr string                     // text of the last other watch error, cleared by an event
	stop    func()
}

// virtualFile splits a virtual file name into the watch it reads and the key within the object.
func virtualFile(name string) (key, string, bool) {
	prefix, rest, ok := strings.Cut(name, "/")
	if !ok {
		return key{}, "", false
	}
	switch k := kind(prefix); k {
	case kindConfigMap, kindSecret:
		object, dataKey, ok := strings.Cut(rest, "/")
		if !ok || object == "" || dataKey == "" {
			return key{}, "", false
		}
		return key{kind: k, name: object}, dataKey, true
	case kindQuerySet:
		if rest == "" {
			return key{}, "", false
		}
		return key{kind: k, name: rest}, "", true
	case kindCluster, kindExporter:
		return key{}, "", false
	default:
		return key{}, "", false
	}
}

// content returns the bytes of one virtual file out of its object, or fs.ErrNotExist.
func content(k key, dataKey string, obj *unstructured.Unstructured) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("%s %q: %w", k.kind, k.name, fs.ErrNotExist)
	}
	if k.kind == kindQuerySet {
		queries, found, err := unstructured.NestedFieldNoCopy(obj.Object, "spec", "queries")
		if err != nil {
			return nil, fmt.Errorf("metricqueryset %q: read spec.queries: %w", k.name, err)
		}
		if !found {
			return nil, fmt.Errorf("metricqueryset %q has no spec.queries: %w", k.name, fs.ErrNotExist)
		}
		b, err := json.Marshal(queries)
		if err != nil {
			return nil, fmt.Errorf("metricqueryset %q: encode spec.queries: %w", k.name, err)
		}
		return b, nil
	}

	data, err := objectData(k, obj)
	if err != nil {
		return nil, err
	}
	v, ok := data[dataKey]
	if !ok {
		return nil, fmt.Errorf("%s %q has no key %q: %w", k.kind, k.name, dataKey, fs.ErrNotExist)
	}
	return v, nil
}

// objectData returns the data of a ConfigMap, or the decoded data of a Secret; nil for a missing object.
func objectData(k key, obj *unstructured.Unstructured) (map[string][]byte, error) {
	if obj == nil {
		return nil, nil
	}
	raw, _, err := unstructured.NestedMap(obj.Object, "data")
	if err != nil {
		return nil, fmt.Errorf("%s %q: read data: %w", k.kind, k.name, err)
	}
	out := make(map[string][]byte, len(raw))
	for dataKey, v := range raw {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%s %q: key %q is not a string", k.kind, k.name, dataKey)
		}
		if k.kind != kindSecret {
			out[dataKey] = []byte(s)
			continue
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("secret %q: key %q is not valid base64", k.name, dataKey)
		}
		out[dataKey] = b
	}
	return out, nil
}

func stringData(data map[string][]byte) map[string]string {
	if data == nil {
		return nil
	}
	out := make(map[string]string, len(data))
	for k, v := range data {
		out[k] = string(v)
	}
	return out
}
