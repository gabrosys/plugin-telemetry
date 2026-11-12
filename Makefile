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

GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)

IMG ?= ghcr.io/gabrosys/plugin-telemetry:$(VERSION)
PLATFORMS ?= linux/amd64,linux/arm64
# image-multiarch needs a docker-container builder.
BUILDX_BUILDER ?=

.PHONY: help
help: ## Show the available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_%-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

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

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run linters and the vulnerability scan
	$(GOLANGCI_LINT) config verify
	$(GOLANGCI_LINT) run ./...
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
