package kubesource

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
)

// readSet is what clusterconfig.Build reads; Secret values enter as their sha256.
type readSet struct {
	Cluster          string                         `json:"cluster"`
	Namespace        string                         `json:"namespace"`
	Plugins          []apiv1.PluginConfiguration    `json:"plugins,omitempty"`
	Monitoring       *apiv1.MonitoringConfiguration `json:"monitoring,omitempty"`
	Databases        [3]string                      `json:"databases"`
	Labels           map[string]string              `json:"labels,omitempty"`
	Exporter         v1alpha1.TelemetryExporterSpec `json:"exporter"`
	EnvConfigMap     map[string]string              `json:"envConfigMap,omitempty"`
	EnvSecret        map[string]string              `json:"envSecret,omitempty"`
	PasswordUsername string                         `json:"passwordUsername,omitempty"`
}

// canonical returns the JSON of the parts of in that Build reads, so changes to anything else,
// such as the Cluster status, leave it unchanged.
func canonical(in clusterconfig.Inputs) ([]byte, error) {
	c := in.Cluster
	r := readSet{
		Cluster:          c.Name,
		Namespace:        c.Namespace,
		Plugins:          c.Spec.Plugins,
		Monitoring:       c.Spec.Monitoring,
		Exporter:         in.Exporter.Spec,
		EnvConfigMap:     in.EnvConfigMap,
		EnvSecret:        hashed(in.EnvSecret),
		PasswordUsername: hash(in.PasswordUsername),
	}
	if b := c.Spec.Bootstrap; b != nil {
		if b.Recovery != nil {
			r.Databases[0] = b.Recovery.Database
		}
		if b.PgBaseBackup != nil {
			r.Databases[1] = b.PgBaseBackup.Database
		}
		if b.InitDB != nil {
			r.Databases[2] = b.InitDB.Database
		}
	}
	for _, key := range in.Exporter.Spec.ResourceLabels {
		if v, ok := c.Labels[key]; ok {
			if r.Labels == nil {
				r.Labels = map[string]string{}
			}
			r.Labels[key] = v
		}
	}

	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode settings inputs: %w", err)
	}

	return b, nil
}

func hash(v string) string {
	if v == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(v))

	return hex.EncodeToString(sum[:])
}

func hashed(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = hash(v)
	}

	return out
}
