# Contributing

Division Swarm is pre-1.0 and still changes quickly. Contributions should start
from the public surfaces in this repository and avoid private maintainer
workflows.

## Before You Start

- Read [README.md](README.md) for the supported public workflow.
- Treat [platform-spec.yaml](platform-spec.yaml) as the authoritative platform
  specification and [openrpc.json](openrpc.json) as the generated public API
  artifact.
- Use [SECURITY.md](SECURITY.md) for suspected vulnerabilities. Do not report
  security issues in public GitHub issues.
- Use [.env.example](.env.example) as the public environment template. Do not
  commit real secrets.

## Local Checks

Use direct public commands and Go tooling rather than private helper scripts:

```bash
go build ./cmd/swarm
go test ./...
go run ./cmd/swarm-openrpc-gen --check
```

If you change API/spec authority, update the authoritative root artifact in the
same pull request as the implementation that makes it true.

## Issue And PR Expectations

Open an issue before large semantic, runtime, API, CLI, or public-surface
changes. High-risk work must follow the pre-audit and proof-audit process in
[docs/IMPLEMENTER_GUIDELINES.md](docs/IMPLEMENTER_GUIDELINES.md) and the
semantic ownership rules in [docs/SEMANTIC_DRIFT.md](docs/SEMANTIC_DRIFT.md).

Small documentation or test-only changes can be simpler, but they should still
state the owner, scope, and proof clearly.

## Scope Boundaries

Do not silently change the public runtime model while editing documentation.
Docker Compose/Postgres onboarding, host workspace backend behavior, and
explicit `/data` source semantics each have their own tracked cleanup streams.

Do not reintroduce retired private Makefile or `scripts/` helper workflows as
public setup instructions.
