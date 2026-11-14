package identity_test

import (
	"testing"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
)

func TestIdentity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Identity Suite")
}

const liveUID = types.UID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")

func newCluster() *apiv1.Cluster {
	return &apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns", UID: liveUID}}
}

func refs() clusterconfig.Refs {
	return clusterconfig.Refs{
		Cluster:    "pg",
		Exporter:   "default",
		ConfigMaps: []string{"cnpg-default-monitoring", "collector-ca"},
		Secrets:    []string{"collector-headers"},
		QuerySets:  []string{"core", "extra"},
	}
}
