package preflight

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	kubeTimeout = 10 * time.Second
	passwordKey = "password"
)

var (
	errSecretNeedsKube = errors.New("read password secret: kubernetes checks are disabled")
	errNoAPIServer     = errors.New("api server address is empty: KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT is unset")
	errNoPasswordKey   = errors.New("secret has no password key")
)

// checkKube runs the Kubernetes checks with the sidecar token. The returned
// password is the one read from cfg.Postgres.PasswordSecret, and secretErr
// is set when that read was requested and failed.
func checkKube(ctx context.Context, cfg Config) (rep *KubeReport, password string, secretErr error) {
	rep = &KubeReport{}
	wantSecret := cfg.Postgres.PasswordSecret != ""

	fail := func(err error) (*KubeReport, string, error) {
		rep.Error = err.Error()
		if wantSecret {
			return rep, "", fmt.Errorf("read password secret: %w", err)
		}

		return rep, "", nil
	}

	if cfg.APIServer == "" {
		return fail(errNoAPIServer)
	}

	ctx, cancel := context.WithTimeout(ctx, kubeTimeout)
	defer cancel()

	restCfg := &rest.Config{
		Host:            cfg.APIServer,
		BearerTokenFile: filepath.Join(cfg.TokenDir, "token"),
		TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(cfg.TokenDir, "ca.crt")},
	}

	if tok, err := os.ReadFile(restCfg.BearerTokenFile); err == nil {
		if exp, ok := TokenExpiry(string(tok)); ok {
			rep.TokenExpiresAt = exp.UTC().Format(time.RFC3339)
		}
	}

	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fail(fmt.Errorf("create clientset: %w", err))
	}

	review, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return fail(fmt.Errorf("create self subject review: %w", err))
	}
	rep.Username = review.Status.UserInfo.Username

	scheme := runtime.NewScheme()
	if err := apiv1.AddToScheme(scheme); err != nil {
		return fail(fmt.Errorf("register CNPG types: %w", err))
	}

	c, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fail(fmt.Errorf("create client: %w", err))
	}

	var cluster apiv1.Cluster
	if err := c.Get(ctx, client.ObjectKey{Namespace: cfg.Namespace, Name: cfg.Cluster}, &cluster); err != nil {
		rep.Error = fmt.Sprintf("get cluster %s/%s: %v", cfg.Namespace, cfg.Cluster, err)
	} else {
		rep.ClusterReadable = true
	}

	if !wantSecret {
		return rep, "", nil
	}

	name := cfg.Postgres.PasswordSecret
	secret, err := cs.CoreV1().Secrets(cfg.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return rep, "", fmt.Errorf("get secret %s/%s: %w", cfg.Namespace, name, err)
	}

	pw, ok := secret.Data[passwordKey]
	if !ok {
		return rep, "", fmt.Errorf("secret %s/%s: %w", cfg.Namespace, name, errNoPasswordKey)
	}

	return rep, string(pw), nil
}
