# Implementer Handoff

Date: 2026-03-07  
Repo: `/Users/youmew/dev/empireai`

## 1. Current state

This codebase is in a much better state than it was originally:

- `internal/runtime` is now a composition/glue layer, not the original god package.
- `cmd/empire/main.go` is no longer a 4.5k-line monolith. Runtime wiring lives in `internal/runtime/runtime.go`; command domains are split into `*_subcommand.go`.
- `internal/dashboard` is split by domain and no longer one giant `server.go`.
- `internal/runtime/pipeline` now has a real platform/product boundary:
  - `internal/runtime/pipeline/` = generic workflow runtime
  - `internal/runtime/pipeline/empire/` = Empire-specific overlay
- Workflow structure is contract-backed and mechanically verified.
- Full test suite was green on the last committed clean state.

The architecture is now good enough that the remaining work is concentrated, not diffuse.

## 2. Active version / contracts

Current active contract version:

- `contracts/tooling.lock` -> `2.1.0`
- `contracts/verification-gates.yaml` -> `2.1.0`

Important:

- `contracts/` is the active contract set used by runtime/tests.
- `docs/specs/...` is the source archive/history.
- Do not assume `contracts/` is in sync with the latest tarball just because a tarball exists.

### Contract workflow

When a new spec drops:

1. Copy the new contract bundle into `contracts/`
2. Re-run contract compliance immediately
3. Only then start changing runtime code

Canonical first check:

```bash
go test ./internal/runtime -run TestContractCompliance -count=1
```

Then:

```bash
go test ./internal/runtime/pipeline/... -count=1
go test ./... -count=1
```

## 3. Known important nuance: `empireai-current.md`

There is a script:

```bash
./scripts/sync_current_spec.sh
```

But it currently only matches `empireai-v2_0_*.md`.

That means it is stale for `v2.1.0+`.

If you rely on `empireai-current.md`, either:

- update the script, or
- update the symlink manually

Example manual update:

```bash
ln -sfn docs/specs/empireai-v2_1_0/empireai-v2_1_0.md empireai-current.md
```

Do not forget this during future spec upgrades.

## 4. Operational deployment instructions

### Local containers

Primary services from `docker-compose.yml`:

- `postgres`
- `orchestrator`
- `dashboard`
- `workspace-base` (profile image used for dynamic workspaces)

### Standard local startup

```bash
docker compose up -d postgres
docker compose build workspace-base orchestrator dashboard
docker compose up -d orchestrator dashboard
```

Useful checks:

```bash
docker compose ps
docker compose logs -f orchestrator
docker compose logs -f dashboard
```

Dashboard URL:

```text
http://localhost:8070/dashboard/
```

### Dashboard-only rebuild

There are Make targets for dashboard-only work:

```bash
make dashboard-build
make dashboard-redeploy
make dashboard-logs
make dashboard-ps
```

### Environment variables that matter

From `docker-compose.yml`, the main runtime-sensitive ones are:

- `CLAUDE_CODE_OAUTH_TOKEN`
- `EMPIREAI_TOOL_GATEWAY_TOKEN`
- `EMPIREAI_API_KEY`
- `EMPIREAI_CLAUDE_USE_MCP`
- `EMPIREAI_CLAUDE_TIMEOUT_SECONDS`

Dynamic workspaces require:

- Docker socket mounted
- `workspace-base` built

If workspace behavior changes, rebuild `workspace-base`.

### Full reset

Be precise about what you are resetting.

Soft restart:

```bash
docker compose down
docker compose up -d postgres orchestrator dashboard
```

Hard DB reset:

This is destructive.

```bash
docker compose down
docker volume rm empireai_empireai_pgdata
docker compose up -d postgres orchestrator dashboard
```

Do not do this casually if you need prior runtime evidence.

## 5. Test commands that actually matter

Fastest meaningful checks:

```bash
go test ./internal/runtime -run TestContractCompliance -count=1
go test ./internal/runtime/pipeline/... -count=1
go test ./internal/runtime/tools -count=1
go test ./internal/dashboard -count=1
go test ./cmd/empire -count=1
```

Full suite:

```bash
go test ./... -count=1
```

Coverage guard helpers:

```bash
make check-key-package-cover
make check-runtime-cover
```

Current minimums are defined in `Makefile`.

## 6. Important lessons learned

### 6.1. Do not trust stale active contracts

This happened repeatedly.

The source bundle under `docs/specs/...` was sometimes correct while active `contracts/` was stale. Always verify both when behavior seems contradictory.

### 6.2. Do not patch contracts blindly in-place

Sometimes the right move is:

- patch active `contracts/` to get runtime/tests green
- then communicate the exact correction back to the spec writer

But do not let those local patches drift silently. They must be either:

- folded into the next tarball, or
- reverted once the source bundle catches up.

### 6.3. `contracts/test-vectors` matters

Some semantic/e2e tests depend on those fixtures being present. If they disappear during a spec sync or checkout, you can get confusing failures that look like runtime regressions.

### 6.4. Test harness was a real bottleneck

Repo-wide tests used to fail under parallel load because of Postgres lifecycle issues.

The current test harness cleans stale `empireai-test-pg-*` containers and uses per-test databases over a shared container.

If full-suite tests start failing with:

- `database system is in recovery mode`
- stale container name collisions
- random DB startup flakiness

Look first at:

- `internal/testutil/postgres.go`

before blaming runtime logic.

### 6.5. Keep the boundary real: `pipeline` vs `pipeline/empire`

This is one of the most important architectural changes in the repo.

- `internal/runtime/pipeline/` should stay generic
- `internal/runtime/pipeline/empire/` should own Empire-specific workflow policy

Do not let product-specific logic drift back into generic `pipeline/`.

There is a guard for this:

- `internal/runtime/pipeline/pipeline_architecture_test.go`

### 6.6. Workflow schema should stay at system-node / stage-transition level

Do not put agent micro-protocol into the workflow contract.

Bad examples for workflow transitions:

- `spec.requested`
- `spec_review.requested`
- `brand.requested`

Those belong to prompts / `agent-tools.yaml`, not the workflow state machine.

Good workflow-level events:

- `validation.started`
- `research.completed`
- `spec.approved`
- `spec.validation_passed`
- `cto.spec_approved`
- `validation.package_ready`
- `vertical.approved`
- `opco.ceo_ready`

### 6.7. Interceptors are transitional, not the end state

The long-term direction is:

- event bus handles transport/runtime concerns
- system nodes / workflow nodes own business orchestration

Do not deepen business logic inside interceptor paths.

### 6.8. Avoid fake genericity

The wrong move is to build a pretend generic engine that is still Empire-specific in hidden ways.

The right move is:

- generic runtime primitives in `pipeline/`
- Empire overlay in `pipeline/empire/`
- contract-backed workflow model
- code-backed guard/action implementations

This is the bridge to arbitrary YAML-configured MAS workflows.

## 7. Architecture snapshot

### Runtime

- `internal/runtime/runtime.go` is the composition root.
- Root runtime now mainly owns:
  - bootstrap/wiring
  - budget
  - inbound gateway
  - diagnostics
  - small glue/helpers

### Pipeline

- `internal/runtime/pipeline/`
  - generic workflow runtime
  - workflow loading/conformance
  - node runtime interfaces
  - state machine
  - state store
  - coordinator shell

- `internal/runtime/pipeline/empire/`
  - Empire prefilter
  - Empire scoring policy
  - Empire payload builders
  - Empire semantic tests

Current state is a strong intermediate state, not a fully generic MAS engine yet.

### Tools

`internal/runtime/tools` is much better than before, but still one of the next hotspots. The emit path especially deserves continued decomposition.

### Dashboard

The dashboard package is split by domain now. Do not reintroduce a monolithic `server.go`.

### CLI

`cmd/empire/main.go` is no longer a god file. New command families should go into domain files, not back into `main.go`.

## 8. What is still left

The remaining meaningful hotspots are:

1. `internal/runtime/pipeline`
- Coordinator is much thinner, but still central.
- Continue pushing execution into node-owned collaborators and keeping `pipeline/` generic.

2. `internal/runtime/tools`
- Continue decomposing executor dispatch and emit handling.

3. Platformization follow-through
- `v2.1.0` gives the platform spec.
- Code is now close enough that the next major step is making the generic workflow/system-node model stronger without waiting for more cleanup elsewhere.

## 9. Recommended next priorities

If you are the next implementer, do this next:

1. Keep shrinking `FactoryPipelineCoordinator`
- Move more orchestration and state mutation into node-owned collaborators.

2. Continue decomposing `internal/runtime/tools`
- Especially emit normalization/guarding/publish flow.

3. Keep the workflow engine generic
- Generic in `pipeline/`
- Empire in `pipeline/empire/`

4. Treat the next spec drop carefully
- sync active contracts
- run `TestContractCompliance`
- do not start code changes before that passes or fails clearly

## 10. Working tree caution

As of this handoff, check `git status` before doing anything large.

This repo has historically accumulated:

- local contract sync edits
- spec tarball churn under `docs/specs`
- review artifacts under `docs/toreview`

Do not accidentally commit spec archive churn with runtime changes.

Stage narrowly.

## 11. Success criteria for the next phase

You are on the right track if:

- `pipeline/` gets more generic, not less
- `pipeline/empire/` grows for Empire policy, not generic workflow machinery
- `go test ./... -count=1` stays green
- deployment stays simple:
  - rebuild image
  - restart containers
  - run compliance and full suite

