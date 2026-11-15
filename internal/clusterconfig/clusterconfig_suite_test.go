package clusterconfig_test

import (
	"testing"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
)

func TestClusterconfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Clusterconfig Suite")
}

func newCluster() *apiv1.Cluster {
	return &apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"}}
}

func newExporter() *v1alpha1.TelemetryExporter {
	e := &v1alpha1.TelemetryExporter{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns"}}
	e.Spec.Exporter.Endpoint = "http://c:4318"
	return e
}

func peerParams() podpatch.Params {
	return podpatch.Params{Exporter: podpatch.DefaultExporter, Auth: podpatch.AuthPeer}
}

func newInputs() clusterconfig.Inputs {
	return clusterconfig.Inputs{Cluster: newCluster(), Params: peerParams(), Exporter: newExporter()}
}

// withEnv returns inputs whose exporter reads envFrom objects named "envcm" and "envsecret".
func withEnv(cm, secret map[string]string) clusterconfig.Inputs {
	in := newInputs()
	in.Exporter.Spec.EnvFrom = &v1alpha1.EnvFromSource{
		ConfigMap: &v1alpha1.LocalObjectReference{Name: "envcm"},
		Secret:    &v1alpha1.LocalObjectReference{Name: "envsecret"},
	}
	in.EnvConfigMap = cm
	in.EnvSecret = secret
	return in
}
