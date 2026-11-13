SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

GO ?= go
BIN := $(CURDIR)/bin
MODULE := github.com/gabrosys/plugin-telemetry

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X $(MODULE)/internal/version.Version=$(VERSION)

GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

GOLANGCI_LINT_VERSION := v2.15.0
GOVULNCHECK_VERSION := v1.9.0
ACTIONLINT_VERSION := v1.7.12
KIND_VERSION := v0.33.0
KUBECTL_VERSION := v1.37.2
CERT_MANAGER_VERSION := v1.21.3
CMCTL_VERSION := v2.6.2
KIND_NODE_IMAGE := kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
SHELLCHECK_IMAGE := koalaman/shellcheck:v0.11.0@sha256:61862eba1fcf09a484ebcc6feea46f1782532571a34ed51fedf90dd25f925a8d

GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
KIND := $(BIN)/kind-$(KIND_VERSION)
KUBECTL := $(BIN)/kubectl-$(KUBECTL_VERSION)
CMCTL := $(BIN)/cmctl-$(CMCTL_VERSION)

KIND_CLUSTER ?= plugin-telemetry
CNPG_VERSION ?= v1.30.1
POSTGRES_IMAGE ?= ghcr.io/cloudnative-pg/postgresql:18.7-system-trixie@sha256:5a5dfbde88d5909fd4938189c141f53fb153f53c519f14907dce9e7c04baa153
E2E_SUITES := smoke spikes admission
E2E_TARGETS := $(addprefix e2e-,$(E2E_SUITES))

DEV_ENV := KIND=$(KIND) KUBECTL=$(KUBECTL) CMCTL=$(CMCTL) KIND_CLUSTER=$(KIND_CLUSTER) KIND_NODE_IMAGE=$(KIND_NODE_IMAGE) \
	CNPG_VERSION=$(CNPG_VERSION) CERT_MANAGER_VERSION=$(CERT_MANAGER_VERSION)

IMG ?= ghcr.io/gabrosys/plugin-telemetry:$(VERSION)
PLATFORMS ?= linux/amd64,linux/arm64
# image-multiarch needs the containerd image store or a docker-container builder.
BUILDX_BUILDER ?=

.PHONY: help
help: ## Show the available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_%-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '  %-16s %s\n' 'e2e-<suite>' 'Run an e2e suite on the kind environment ($(E2E_SUITES))'

.PHONY: build
build: ## Build bin/plugin-telemetry and compile every package
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/plugin-telemetry ./cmd/plugin-telemetry
	$(GO) build ./...

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: image
image: ## Build the image for the local platform and load it into Docker
	docker buildx build --load --build-arg VERSION=$(VERSION) -t $(IMG) .

.PHONY: image-multiarch
image-multiarch: ## Build all platforms with an SBOM into bin/image-oci
	rm -rf $(BIN)/image-oci
	docker buildx build $(if $(BUILDX_BUILDER),--builder $(BUILDX_BUILDER)) \
		--platform $(PLATFORMS) --sbom=true --build-arg VERSION=$(VERSION) \
		--output type=oci,dest=$(BIN)/image-oci,tar=false .

.PHONY: dev-up
dev-up: $(KIND) $(KUBECTL) $(CMCTL) ## Create the kind environment (CNPG_VERSION=v1.30.1|main)
	$(DEV_ENV) hack/dev-env.sh up

.PHONY: dev-down
dev-down: $(KIND) ## Delete the kind environment
	$(DEV_ENV) hack/dev-env.sh down

.PHONY: kind-load
kind-load: $(KIND) ## Load the image into kind and record its digest reference in bin/image-ref
	$(KIND) load docker-image $(IMG) --name $(KIND_CLUSTER)
	KIND=$(KIND) hack/kind-image-ref.sh $(KIND_CLUSTER) $(IMG) > $(BIN)/image-ref

.PHONY: deploy
deploy: $(KUBECTL) ## Deploy the plugin to the kind environment (after image and kind-load)
	@test -s $(BIN)/image-ref || { echo "run make image kind-load first" >&2; exit 1; }
	KUBECTL=$(KUBECTL) KIND_CLUSTER=$(KIND_CLUSTER) BIN=$(BIN) hack/deploy.sh "$$(cat $(BIN)/image-ref)"

.PHONY: $(E2E_TARGETS)
$(E2E_TARGETS): e2e-%: dev-up
	E2E_KUBE_CONTEXT=kind-$(KIND_CLUSTER) CNPG_VERSION=$(CNPG_VERSION) POSTGRES_IMAGE=$(POSTGRES_IMAGE) \
		$(GO) test -tags e2e -count=1 -timeout 30m -v ./test/e2e/$*/... -ginkgo.v

e2e-spikes: image kind-load deploy
e2e-admission: image kind-load deploy

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run linters and the vulnerability scan
	$(GOLANGCI_LINT) config verify
	$(GOLANGCI_LINT) run ./...
	$(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	docker run --rm -v "$(CURDIR):/mnt:ro" -w /mnt $(SHELLCHECK_IMAGE) hack/*.sh test/devenv/testdata/*
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) -tags e2e ./...

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format the code
	$(GOLANGCI_LINT) fmt

GOLANGCI_LINT_RELEASE := https://github.com/golangci/golangci-lint/releases/download/$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_ASSET := golangci-lint-$(GOLANGCI_LINT_VERSION:v%=%)-$(GOOS)-$(GOARCH)

$(GOLANGCI_LINT):
	hack/fetch-tool.sh $(GOLANGCI_LINT_RELEASE)/$(GOLANGCI_LINT_ASSET).tar.gz \
		$(GOLANGCI_LINT_RELEASE)/golangci-lint-$(GOLANGCI_LINT_VERSION:v%=%)-checksums.txt \
		$@ $(GOLANGCI_LINT_ASSET)/golangci-lint

$(KIND):
	hack/fetch-tool.sh https://github.com/kubernetes-sigs/kind/releases/download/$(KIND_VERSION)/kind-$(GOOS)-$(GOARCH) \
		https://github.com/kubernetes-sigs/kind/releases/download/$(KIND_VERSION)/kind-$(GOOS)-$(GOARCH).sha256sum $@

$(KUBECTL):
	hack/fetch-tool.sh https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(GOOS)/$(GOARCH)/kubectl \
		https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(GOOS)/$(GOARCH)/kubectl.sha256 $@

$(CMCTL):
	hack/fetch-tool.sh https://github.com/cert-manager/cmctl/releases/download/$(CMCTL_VERSION)/cmctl_$(GOOS)_$(GOARCH) \
		https://github.com/cert-manager/cmctl/releases/download/$(CMCTL_VERSION)/checksums.txt $@
