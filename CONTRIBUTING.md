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
- Use [swarm.example.yaml](swarm.example.yaml) as the non-secret setup reference.
  Swarm reads one file: `.swarm/swarm.yaml` in the current directory, or built-in
  defaults if absent. `--config <file>` replaces that selection completely;
  `SWARM_CONFIG` selects the same file when `--config` is absent. Root
  `swarm.yaml` and user-global config files are not discovered. Use `swarm secrets`
  for contract credentials. Repo `.env` files are non-authoritative and are not
  loaded by Swarm commands.

## Local Checks

### Developer Feedback

Use direct public commands and Go tooling rather than private helper scripts.
For routine local iteration, run the changed-package selector first. It uses
the git diff against `origin/master`, expands in-repository reverse
dependencies, prints the selected packages, and then runs the exact `go test`
command it reports:

```bash
go build ./cmd/swarm
go run ./cmd/swarm-test-changed -dry-run
go run ./cmd/swarm-test-changed
go run ./cmd/swarm-test
go run ./cmd/swarm-openrpc-gen --check
```

These are development commands, not a list of qualification steps to run in
sequence. The changed-package selector and no-context runner do not replace
reviewer-selected proof. Keep Go's test cache for ordinary iteration; do not
habitually force `-count=1` on every local test.

### Optional Native Unused Analysis

`go run ./cmd/swarm-unused` defaults to native-only analysis on the current
host, not the complete CI platform union. The guard internally pins its analysis
toolchain to Go 1.26.8 and Staticcheck v0.8.1. Product builds follow the root
[`go.mod`](go.mod); see the [development requirements](README.md#development)
for its minimum and preferred toolchain. Master nightly and explicit full
dispatch CI collect native Linux and Darwin results with tests enabled for
default, race, and `issue2413`
configurations, then uses upstream
binary merge semantics: a declaration reachable in any admitted variant is
live. Collection does not fail on U1000; the source-bound merged check does.
Missing, skipped, or failed native collection cannot satisfy that cadence's
required summary. PR and push CI require the three native analysis jobs to be
skipped, including full-tier PRs and verified master replay; the analyzer's
contract tests retain their existing tier membership. A branch dispatch proves
its native path but cannot qualify the master-nightly marker.

Native logs and current-run/attempt artifacts retain failures. The required
summary lists each native result and refuses unsuccessful cadence; the master
nightly marker cannot succeed. The merge orchestrator's strict nightly admission
selects the newest completed non-cancelled master cadence and must reject red,
partial or stale evidence rather than use an older green. Whole cancelled runs
are excluded from selection; cancelled partner jobs still cannot qualify the
selected run. Its deployed gate/configuration and refusal evidence
are attested separately under [#2535](https://github.com/division-sh/swarm/issues/2535)
before the [#2640](https://github.com/division-sh/swarm/issues/2640) cutover merges;
this workflow change does not install or relax an external merge gate.
The orchestrator and CI-health owner triage a nightly red: U1000 findings go to
the code owner for a small fix-forward cleanup PR, while analyzer/runner failures
remain tracked under #2535/#2353. Failed evidence is not ignored or relabeled.

### Candidate Qualification

Commit the clean candidate and upload its signed SHA to a non-PR staging branch
before running:

```bash
go run ./cmd/swarm-test preflight
```

After a complete pass, push the **same SHA** to the PR. A rebase needs a fresh
exact-head preflight; old-head status does not transfer. The command never
pushes. The [canonical runner runbook](cmd/swarm-test/README.md) owns staging,
authentication, clean-source admission, receipts and status publication.

For focused local qualification, record `Local-Tier: focused` and
`Preflight: pass (Ns)`, with the affected behavioral, both-store, race and
stateful receipts selected by the reviewer. Preflight replaces their shared
guards; it is not an additional local tier or hosted qualification. The
five-minute warm-run target is soft, never permission to truncate the selection
or relax a deadline. Native unused cadence remains separate. Retain failures
through the [canonical failure readout](cmd/swarm-test-timing/README.md#failure-evidence-readback).

The reviewer records exactly one `CI-Tier: core|lifecycle|full` line in the PR
body, independently of local qualification. An absent CI tier selects core
feedback; invalid or duplicate tier instructions select full. A reviewer may
add one `CI-Units: unit-a, unit-b` line
naming existing hosted full-plan units outside the selected tier. Empty,
malformed, duplicate, unknown, local-only or already-selected units refuse.
The planner preserves the original unit contracts and rejects overlapping work.
Body edits do not trigger heavy CI. The existing late five-minute summary checks
the current body/head before success; it cannot revoke a completed green.
Any change to the effective tier or sorted extra-unit set requires new qualifying
execution. Keep the PR non-mergeable until
that qualification succeeds. At merge, the lead compares the current CI-Tier to
the latest successful current-head trusted-App plan/summary, including edits
after approval; old-head green or a different selection digest does not qualify.
The `CI tier: <tier>; selection: <digest>; run: <id>; attempt: <n>` check and
successful Required test summary must share the trusted App, head, run, attempt
and check suite. This is not a branch-freshness rule.

Focused qualification is review metadata, not a `--tier focused` command.
When an explicit local tier is selected instead, use `--tier core`,
`--tier lifecycle` or `--full`, record the matching `Local-Tier`, and retain its
plan and receipts under `test-results/local`. Do not add a duplicate local tier
to focused proof. The runner runbook defines the source fences and receipt
requirements for each path; dirty source earns developer feedback only.

Compilation-product reuse likewise requires a clean complete worktree, including
root Go and embedded inputs. Non-ignored dirt or active Go workspace mode bypasses
reuse and compiles freshly; a workspace path does not identify external module
contents. Ignored receipt outputs remain allowed.

Completion requires a terminal PASS for each selected executable root and its
declared backend children. Finite profile replacements, opt-in live proofs, and
subprocess helpers are explicit non-credit deferrals in
`internal/testplanning/deferrals.go`; an undeclared SKIP fails completion.
Core owns ordinary roots and fixed safety/smoke canaries. Lifecycle additionally
owns retained both-store restart and recovery families. Full owns every admitted
root and both unchanged 900s backend soaks. Native Linux/Darwin unused analysis
belongs to the separate cadence above, not a full-tier PR. The root census
explicitly records every lower-tier deferral; omitted proofs earn no execution
credit. Schedule and unverified master use full;
manual dispatch is full-only. Local full cannot claim Darwin credit.
For the actual selected commands and their proof boundaries, follow the
reviewer's named obligations and the runner runbook rather than inferring a
whole local tier from the type of change.

Postgres-backed tests should use the supported host setup in
[tests/support/testutil/POSTGRES.md](tests/support/testutil/POSTGRES.md). Keep the test
DSN invocation-scoped. Whole-suite and coordinated shared-host runs must use
the canonical `swarm-test` runner shown above. Focused direct `go test ./pkg`
remains the expected small-blast-radius development loop. For a disposable
Docker-backed suite, omit `SWARM_TEST_POSTGRES_DSN`; package tests never launch
Docker directly.

`swarm-test` computes conservative unit-worker capacity from observable
host/container resources: at most four, one per four effective CPUs and eight
GiB RAM, with unknown capacity falling back to one. An explicit shared test DSN
clamps the ceiling to one; private workers own independent services. The parent
does not hold an admission slot. `SWARM_TEST_RUN_SLOTS` may reduce the computed
ceiling, never exceed it or resize an occupied registry; live wrappers must
agree on the active value. This is not a performance guarantee. See the
[runner runbook](cmd/swarm-test/README.md) for exact admission and receipt rules.

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
