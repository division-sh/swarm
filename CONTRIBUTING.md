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
- Use [swarm.example.yaml](swarm.example.yaml) as the non-secret `swarm.yaml`
  setup reference. Use `swarm secrets` for contract credentials. Repo
  `.env` files are non-authoritative and are not loaded by Swarm commands.

## Local Checks

Use direct public commands and Go tooling rather than private helper scripts.
For routine local iteration, run the changed-package selector first. It uses
the git diff against `origin/master`, expands in-repository reverse
dependencies, prints the selected packages, and then runs the exact `go test`
command it reports:

```bash
go build ./cmd/swarm
go run ./cmd/swarm-test-changed
go run ./cmd/swarm-test-changed -dry-run
go test ./...
go run ./cmd/swarm-openrpc-gen --check
```

Routine PRs can cite scoped local proof from `swarm-test-changed` plus any
named package families required by the touched surface; CI remains responsible
for the full-truth push/manual/scheduled runs. Do not habitually force
`-count=1` for every local iteration because it defeats Go's local test cache.
High-risk semantic/runtime migrations still require full local
`go test ./... -count=1` when the issue gate or reviewer asks for it.

If you change API/spec authority, update the authoritative root artifact in the
same pull request as the implementation that makes it true.

## Release Build Metadata

Public release artifacts should inject binary metadata explicitly while keeping
ordinary tagged `go install` builds useful through Go build-info fallback:

```bash
go build \
  -ldflags "-X main.binaryVersion=v1.6.0 -X main.binaryCommit=$(git rev-parse HEAD) -X main.binaryDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  ./cmd/swarm
```

`swarm version --json` reports `binary_version`, `module_version`, and
`platform_version` separately. The first two describe the installed binary and
Go module ref; `platform_version` comes from root `platform-spec.yaml`.

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
Host workspace backend behavior, repo-wide SQLite-default rollout proof, and
explicit `/data` source semantics each have their own tracked owners.

Do not reintroduce retired private Makefile or `scripts/` helper workflows as
public setup instructions.
