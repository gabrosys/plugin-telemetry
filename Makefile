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
CONTROLLER_GEN_VERSION := v0.23.0
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

KIND_CLUSTER ?= plugin-telemetry-$(subst .,-,$(CNPG_VERSION))
CNPG_VERSION ?= v1.30.1
POSTGRES_IMAGE ?= ghcr.io/cloudnative-pg/postgresql:18.7-system-trixie@sha256:5a5dfbde88d5909fd4938189c141f53fb153f53c519f14907dce9e7c04baa153
COLLECTOR_IMAGE ?= otel/opentelemetry-collector:0.165.0@sha256:34b24b740c3c88d3467740e8642a397c13898ef76d7295c5425164c482ed1b73
E2E_SUITES := admission engine plugin collector-metrics
KIND_E2E_SUITES := $(filter-out engine,$(E2E_SUITES))
E2E_TARGETS := $(addprefix e2e-,$(KIND_E2E_SUITES))
# Ginkgo --timeout of a suite: E2E_TIMEOUT_<suite> when set, else 30m. E2E_PROCS_<suite> sets --procs (default 1).
E2E_TIMEOUT_plugin := 50m
E2E_TIMEOUT_engine := 20m
E2E_TIMEOUT_collector-metrics := 30m
E2E_PROCS_plugin := 3
# Regex for ginkgo --focus; empty runs every spec.
E2E_FOCUS ?=
override E2E_FOCUS := $(value E2E_FOCUS)
export E2E_FOCUS

DEV_ENV := KIND=$(KIND) KUBECTL=$(KUBECTL) CMCTL=$(CMCTL) KIND_CLUSTER=$(KIND_CLUSTER) KIND_NODE_IMAGE=$(KIND_NODE_IMAGE) \
	CNPG_VERSION=$(CNPG_VERSION) CERT_MANAGER_VERSION=$(CERT_MANAGER_VERSION)

IMG ?= ghcr.io/gabrosys/plugin-telemetry:$(VERSION)
PLATFORMS ?= linux/amd64,linux/arm64
# buildx passes SOURCE_DATE_EPOCH to the build as a build arg; with rewrite-timestamp it fixes every timestamp in the image.
export SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || echo 0)
# Builds are reproducible on the containerd image store or a non-docker builder; the classic store ignores rewrite-timestamp. image-multiarch needs the containerd store or a docker-container builder.
BUILDX_BUILDER ?=

.PHONY: help
help: ## Show the available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_%-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '  %-16s %s\n' 'e2e-<suite>' 'Run an e2e suite on the kind environment ($(KIND_E2E_SUITES)); E2E_FOCUS=<regex> runs matching specs only'

.PHONY: build
build: ## Build bin/plugin-telemetry and compile every package
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/plugin-telemetry ./cmd/plugin-telemetry
	$(GO) build ./...

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run the tests that need Docker (PostgreSQL in a container)
	POSTGRES_IMAGE=$(POSTGRES_IMAGE) $(GO) test -race -count=1 -tags integration ./...

.PHONY: generate
generate: ## Regenerate docs/self-metrics.md, the deepcopy code and the CRDs
	$(GO) run ./internal/selfmetrics/gen > docs/self-metrics.md
	$(GO) run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) object crd:allowDangerousTypes=true paths=./api/... output:crd:dir=config/crd

.PHONY: image
image: ## Build the image for the local platform and load it into Docker
	docker buildx build --output type=docker,name=$(IMG),rewrite-timestamp=true,unpack=false \
		--provenance=false --build-arg VERSION=$(VERSION) .

.PHONY: image-multiarch
image-multiarch: ## Build all platforms with an SBOM into bin/image-oci
	rm -rf $(BIN)/image-oci
	docker buildx build $(if $(BUILDX_BUILDER),--builder $(BUILDX_BUILDER)) \
		--platform $(PLATFORMS) --sbom=true --provenance=false --build-arg VERSION=$(VERSION) \
		--output type=oci,dest=$(BIN)/image-oci,tar=false,rewrite-timestamp=true .

.PHONY: dev-up
dev-up: $(KIND) $(KUBECTL) $(CMCTL) ## Create the kind environment (CNPG_VERSION=v1.30.1|main)
	$(DEV_ENV) hack/dev-env.sh up

.PHONY: dev-down
dev-down: $(KIND) ## Delete the kind environment
	$(DEV_ENV) hack/dev-env.sh down

.PHONY: kind-load
kind-load: image dev-up $(KIND) ## Load the plugin image into kind, pull the collector image on its nodes and record the plugin digest reference in bin/image-ref
	$(KIND) load docker-image $(IMG) --name $(KIND_CLUSTER)
	for node in $$($(KIND) get nodes --name $(KIND_CLUSTER)); do docker exec $$node crictl pull $(COLLECTOR_IMAGE) || exit 1; done
	mkdir -p $(BIN)
	KIND=$(KIND) hack/kind-image-ref.sh $(KIND_CLUSTER) $(IMG) > $(BIN)/image-ref

.PHONY: deploy
deploy: kind-load $(KUBECTL) ## Build and load the image, then deploy the plugin to the kind environment
	KUBECTL=$(KUBECTL) KIND_CLUSTER=$(KIND_CLUSTER) BIN=$(BIN) COLLECTOR_IMAGE=$(COLLECTOR_IMAGE) POSTGRES_IMAGE=$(POSTGRES_IMAGE) \
		hack/deploy.sh "$$(cat $(BIN)/image-ref)"

# ginkgo-run: $(1) names the suite (timeout and procs), $(2) is the package directory.
ginkgo-run = $(GO) run github.com/onsi/ginkgo/v2/ginkgo run --tags e2e -v --fail-on-empty \
	--procs=$(or $(E2E_PROCS_$(1)),1) --timeout=$(or $(E2E_TIMEOUT_$(1)),30m) $(if $(E2E_FOCUS),--focus="$$E2E_FOCUS") ./$(2)/...

.PHONY: $(E2E_TARGETS)
$(E2E_TARGETS): e2e-%: deploy
	$(KUBECTL) --context kind-$(KIND_CLUSTER) rollout restart deployment/collector -n telemetry-e2e
	$(KUBECTL) --context kind-$(KIND_CLUSTER) rollout status deployment/collector -n telemetry-e2e --timeout=180s
	E2E_KUBE_CONTEXT=kind-$(KIND_CLUSTER) CNPG_VERSION=$(CNPG_VERSION) POSTGRES_IMAGE=$(POSTGRES_IMAGE) \
		$(call ginkgo-run,$*,test/e2e/$*)

.PHONY: e2e-engine
e2e-engine: build ## Run the engine e2e suite with Docker, without kind (E2E_FOCUS=<regex>)
	ENGINE_BIN=$(BIN)/plugin-telemetry POSTGRES_IMAGE=$(POSTGRES_IMAGE) COLLECTOR_IMAGE=$(COLLECTOR_IMAGE) \
		$(call ginkgo-run,engine,test/e2e/engine)

E2E_MATRIX_CNPG ?= v1.30.1 main
E2E_MATRIX_SUITES ?= $(KIND_E2E_SUITES)

# One lane: the suites in order on one kind cluster.
.PHONY: e2e-lane
e2e-lane: $(addprefix e2e-,$(E2E_MATRIX_SUITES))

# One lane per CNPG version, each with its own cluster and BIN; logs in bin/e2e/<v>.log.
.PHONY: e2e-matrix
e2e-matrix: image $(KIND) $(KUBECTL) $(CMCTL) ## Run the kind suites on one kind cluster per CNPG version, in parallel
	mkdir -p $(BIN)/e2e
	pids=(); failed=0; \
	for v in $(E2E_MATRIX_CNPG); do \
		( start=$$SECONDS; rc=0; \
		$(MAKE) --no-print-directory -k -o image e2e-lane CNPG_VERSION=$$v KIND_CLUSTER=plugin-telemetry-$${v//./-} \
			BIN=$(BIN)/lanes/$$v KIND=$(KIND) KUBECTL=$(KUBECTL) CMCTL=$(CMCTL) >$(BIN)/e2e/$$v.log 2>&1 || rc=$$?; \
		echo "$$v exit=$$rc $$((SECONDS - start))s"; \
		exit $$rc ) & \
		pids+=($$!); \
	done; \
	for pid in "$${pids[@]}"; do wait "$$pid" || failed=1; done; \
	exit $$failed

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run linters and the vulnerability scan
	$(GOLANGCI_LINT) config verify
	$(GOLANGCI_LINT) run ./...
	$(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	docker run --rm -v "$(CURDIR):/mnt:ro" -w /mnt $(SHELLCHECK_IMAGE) hack/*.sh test/pgtest/server.sh
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) -tags e2e,integration ./...

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
