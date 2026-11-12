SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

GO ?= go
BIN := $(CURDIR)/bin
MODULE := github.com/gabrosys/plugin-telemetry

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X $(MODULE)/internal/version.Version=$(VERSION)

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
