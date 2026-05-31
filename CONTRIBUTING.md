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
changes. High-risk work must not start coding until the issue records an
approved pre-audit gate that names the semantic concept, canonical owner,
failure class, sibling seams checked, and required proof.

For semantic changes, bind the implementation to the authoritative owner rather
than issue prose or private maintainer notes. Runtime/platform semantics belong
in [platform-spec.yaml](platform-spec.yaml); public RPC shape belongs in
[openrpc.json](openrpc.json) when generated from the spec and implementation.
If no owner exists, split or promote one before implementing.

Before review, PRs for high-risk work must include a proof audit that states the
changed concept, touched consumers, old paths made invalid, sibling contexts
checked, proof run, and any parent class left open. Do not preserve duplicate
semantic interpreters, heuristic compatibility shims, or legacy behavior "just
in case."

Small documentation or test-only changes can be simpler, but they should still
state the owner, scope, and proof clearly.

## Scope Boundaries

Do not silently change the public runtime model while editing documentation.
Docker Compose/Postgres onboarding, host workspace backend behavior, and
explicit `/data` source semantics each have their own tracked cleanup streams.

Do not reintroduce retired private Makefile or `scripts/` helper workflows as
public setup instructions.
