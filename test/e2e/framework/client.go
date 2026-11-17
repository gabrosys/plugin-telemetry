package framework

import (
	"fmt"
	"sync"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/onsi/ginkgo/v2"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
)

var setLogger sync.Once

// NewClients returns a client and REST config for kubeContext. It also sets controller-runtime's logger to
// GinkgoLogr, once.
func NewClients(kubeContext string) (client.Client, *rest.Config, error) {
	setLogger.Do(func() { ctrllog.SetLogger(ginkgo.GinkgoLogr) })

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: kubeContext},
	).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("load kubeconfig context %q: %w", kubeContext, err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("register client-go types: %w", err)
	}
	if err := apiv1.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("register CNPG types: %w", err)
	}

	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("register telemetry types: %w", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, fmt.Errorf("create client: %w", err)
	}

	return c, cfg, nil
}
