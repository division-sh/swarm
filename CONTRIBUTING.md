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
go run ./cmd/swarm-unused
go run ./cmd/swarm-test-changed
go run ./cmd/swarm-test-changed -dry-run
go run ./cmd/swarm-test
go run ./cmd/swarm-test --tier lifecycle
go run ./cmd/swarm-test --full
go run ./cmd/swarm-openrpc-gen --check
```

`go run ./cmd/swarm-unused` defaults to native-only analysis on the current
host, not the complete CI platform union. The guard internally pins its analysis
toolchain to Go 1.26.8 and Staticcheck v0.8.1; product Go setup and `go.mod` are
unchanged. Full-tier CI collects native Linux and Darwin results with tests
enabled for default, race, and `issue2413` configurations, then uses upstream
binary merge semantics: a declaration reachable in any admitted variant is
live. Collection does not fail on U1000; the source-bound merged check does.
Missing, skipped, or failed native collection cannot satisfy required CI.
The noncompiling [#2438](https://github.com/division-sh/swarm/issues/2438)
configuration remains explicitly parked under E's
[#2496](https://github.com/division-sh/swarm/issues/2496) until compile repair.

The reviewer records exactly one `CI-Tier: core|lifecycle|full` and one
`Local-Tier: core|lifecycle|full` line in the PR body. The two decisions are
independent. Missing, invalid or duplicate CI instructions select full.
Body edits do not trigger heavy CI. The existing late five-minute summary checks
the current body/head before success; it cannot revoke a completed green.
An increase above successful current-head scope requires a new signed head and
qualifying higher-tier protected checks. Keep the PR draft/non-mergeable until
that qualification succeeds. At merge, the lead compares the current CI-Tier to
the latest successful current-head trusted-App plan/summary, including edits
after approval; old-head green and insufficient scope do not qualify. A completed
higher tier may satisfy a lower requirement. This is not a branch-freshness rule.
Local
qualification uses explicit `--tier TIER` or `--full` and retains the effective
plan and receipts under `test-results/local`. The reviewer compares that command
and receipt with Local-Tier; an absent or invalid local instruction cannot earn
thin qualification credit. No GitHub lookup or authorization sidecar is used.

`swarm-test-changed` and no-context `swarm-test` are developer feedback, not
substitutes for the reviewer-required tier. Do not habitually force
`-count=1` for every local iteration because it defeats Go's local test cache.
The no-argument runner is the bounded local tier, not an exhaustive `./...` run.
Completion requires a terminal PASS for each selected executable root and its
declared backend children. Finite profile replacements, opt-in live proofs, and
subprocess helpers are explicit non-credit deferrals in
`internal/testplanning/deferrals.go`; an undeclared SKIP fails completion.
Core owns ordinary roots and fixed safety/smoke canaries. Lifecycle additionally
owns retained both-store restart and recovery families. Full owns every admitted
root, both unchanged 900s backend soaks, and hosted native Linux/Darwin unused
union. The root census explicitly records every lower-tier deferral; omitted
proofs earn no execution credit. Schedule and unverified master use full;
manual exhaustive runs default to full. Local full cannot claim Darwin credit.
High-risk semantic/runtime migrations require full local
`go run ./cmd/swarm-test --full` when the issue
gate or reviewer asks for it.

Postgres-backed tests should use the supported host setup in
[internal/testutil/POSTGRES.md](internal/testutil/POSTGRES.md). Keep the test
DSN invocation-scoped. Whole-suite and coordinated shared-host runs must use
the canonical `swarm-test` runner shown above. Focused direct `go test ./pkg`
remains the expected small-blast-radius development loop. For a disposable
Docker-backed suite, omit `SWARM_TEST_POSTGRES_DSN`; package tests never launch
Docker directly.

`swarm-test` is single-flight per host/account by default. Deliberate local
experiments may set `SWARM_TEST_RUN_SLOTS` to one positive integer, but every
live wrapper must agree on that value and capacities above one carry no
reliability or performance guarantee.

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
