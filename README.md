# Swarm

Swarm is a Go implementation of the Swarm Platform: a declarative,
contract-driven orchestration runtime for multi-agent workflows.

The platform treats workflow contracts as the source of orchestration truth.
Contracts describe flows, agents, events, policies, and runtime surfaces; the
runtime executes those contracts through deterministic API, store, and runtime
owners.

This README is the public project setup entry point. Platform semantics remain
owned by [platform-spec.yaml](docs/specs/swarm-platform/platform/contracts/platform-spec.yaml)
and the generated OpenRPC artifact under
[docs/specs/swarm-platform/platform/contracts/](docs/specs/swarm-platform/platform/contracts/).

## Repository Status

This repository is still completing its public-release readiness work. The root
README is intentionally limited to project purpose, architecture orientation,
local setup, common commands, build/test instructions, and process-doc pointers.

The following readiness surfaces are tracked separately and are not completed by
this README:

- license and SPDX publication
- security reporting policy
- contribution guide and code of conduct
- environment example file
- secret/material scan
- generated artifact reproducibility
- Docker or compose quickstart verification

## Architecture Map

- [cmd/swarm](cmd/swarm/) contains the Cobra CLI for serving the runtime,
  verifying local contracts, and operating workflow runs through the v1 API.
- [cmd/swarm-openrpc-gen](cmd/swarm-openrpc-gen/) generates and checks the
  OpenRPC artifact from the API registry.
- [internal/apiv1](internal/apiv1/) owns the v1 JSON-RPC and WebSocket API
  handlers, registry, and API conformance tests.
- [internal/runtime](internal/runtime/) owns runtime boot, scheduling,
  orchestration, shutdown, diagnostics, and runtime invariants.
- [internal/store](internal/store/) owns persistence, read surfaces, run
  control, delivery state, and PostgreSQL-backed store behavior.
- [internal/config](internal/config/) owns runtime configuration loading.
- [internal/promptcontracts](internal/promptcontracts/) and
  [internal/apispec](internal/apispec/) support prompt contract and API-spec
  validation.
- [docs](docs/) contains process docs, watchlists, platform specs, templates,
  and implementation protocols.
- [tests](tests/) contains scoped test fixtures and test-support documents.

## Local Setup

Prerequisites:

- Go 1.23.x, matching [go.mod](go.mod).
- A PostgreSQL instance for the full test suite. CI uses PostgreSQL 16 with:

```sh
export SWARM_TEST_POSTGRES_DSN='host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres sslmode=disable'
```

Basic setup:

```sh
git clone <repo-url>
cd Swarm
go mod download
```

This README does not define a Docker or compose quickstart. Docker artifacts
exist in the repository, but quickstart verification is tracked separately from
this setup slice.

## Common Commands

Show the CLI surface:

```sh
go run ./cmd/swarm --help
```

Build all packages:

```sh
go build ./...
```

Build the Swarm CLI binary:

```sh
go build ./cmd/swarm
```

Validate local contract files:

```sh
go run ./cmd/swarm verify
```

Check that the generated OpenRPC artifact is current:

```sh
go run ./cmd/swarm-openrpc-gen --check
```

Start the runtime for local development only after configuring the required
runtime inputs for your environment:

```sh
go run ./cmd/swarm serve --help
```

## Build And Test

The CI workflow in [.github/workflows/ci.yml](.github/workflows/ci.yml) is the
current build/test command source for this repository. It runs:

```sh
gofmt -l .
go build ./...
go build ./cmd/swarm
go run ./cmd/swarm-openrpc-gen --check
go vet ./...
go test ./... -count=1
```

Run the same checks locally before opening a PR. The full test command expects
the PostgreSQL test DSN shown in [Local Setup](#local-setup) when PostgreSQL
backed tests are enabled.

## Process And Spec References

- [Implementer Guidelines](docs/IMPLEMENTER_GUIDELINES.md) define the default
  implementation, pre-audit, gate, and closure rules.
- [Semantic Drift Prevention](docs/SEMANTIC_DRIFT.md) defines the canonical-owner
  and sibling-seam rules for semantic changes.
- [Collaboration Workflow](docs/COLLABORATION_WORKFLOW.md) describes branch,
  worktree, spec-change, and review sequencing.
- [Implementer Review Checklist](docs/IMPLEMENTER_REVIEW_CHECKLIST.md) is the
  merge-readiness checklist for non-trivial changes.
- [Process Checklist Templates](docs/PROCESS_CHECKLIST_TEMPLATES.md) contains
  reusable audit, gate, and closeout templates.
- [Bug Intake And Repro](docs/BUG_INTAKE_AND_REPRO.md) describes issue intake
  and reproduction expectations.
- [Platform spec](docs/specs/swarm-platform/platform/contracts/platform-spec.yaml)
  is the merge-bearing platform specification.
- [OpenRPC artifact](docs/specs/swarm-platform/platform/contracts/openrpc.json)
  is the generated API contract artifact checked by CI.

## Boundaries

Do not treat this README as a replacement for the platform spec, OpenRPC
artifact, process docs, issue gates, or legal/security/contribution files.
When those sources disagree with this onboarding page, use the more specific
owner and update this README only as an index.
