# Contributing

## What you need

Go 1.27.4 (the version in `go.mod`), Docker, and GNU make. The e2e suites also use kind, kubectl and cmctl; the Makefile fetches pinned versions of them into `bin/`.

## Build and test

`make help` lists every target. Before you open a pull request, run:

```bash
make build test lint
make generate
git diff --exit-code
```

`make generate` rewrites `docs/self-metrics.md`, `docs/crd-reference.md`, the deepcopy code and the CRDs, so the last command must show no change. `make test-integration` runs the tests that need Docker.

The e2e suites run on a local kind cluster:

```bash
make dev-up
make e2e-plugin
```

The other suites are `admission`, `collector-metrics` and `resilience`. `make e2e-engine` runs without kind, and `make e2e-matrix` runs the kind suites against CloudNativePG v1.30.1 and `main` in parallel.

## Changes

Write the test first for a change in behaviour. Keep each pull request to one topic.

Commit subjects follow Conventional Commits, `type(scope): subject`, because release-please builds the changelog and the version from them. Sign off every commit with `git commit -s`.

CI runs the build and the image build, the unit and integration tests, lint and the generate check on every pull request.
