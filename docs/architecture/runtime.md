# Runtime Architecture

## Goal

`internal/runtime` is the composition root for the factory runtime, not the implementation bucket for every subsystem.

Refactors are judged on ownership and dependency direction, not on raw file-count reduction.
Moving code only to reduce the number of files is explicitly a non-goal.

The target shape is:

- `internal/runtime`
  - composition only
  - concrete assembly of subsystems
  - shared public entry points that are intentionally runtime-facing
- `internal/runtime/pipeline`
  - discovery
  - scoring
  - validation
  - campaign progression
  - stage projection
- `internal/runtime/bus`
  - publish path
  - subscriptions
  - routing
  - delivery
  - interceptor execution
- `internal/runtime/manager`
  - agent lifecycle
  - replay
  - control loop
  - receipts / dead-letter
  - OpCo bootstrap orchestration
- `internal/runtime/mcp`
  - MCP protocol surface
  - request/response adaptation
  - MCP context and diagnostics
- `internal/runtime/tools`
  - tool schemas
  - tool execution
  - emit-tool generation
  - executor policies
- `internal/runtime/contracts`
  - generated event schema registry
  - contract loaders
  - contract compliance gates
- `internal/runtime/testkit`
  - shared test helpers only
- leaf packages already extracted:
  - `internal/runtime/llm`
  - `internal/runtime/sessions`
  - `internal/runtime/workspace`

## Dependency Rules

### Allowed direction

- `internal/runtime` may import any runtime subpackage.
- runtime subpackages may import:
  - `internal/events`
  - `internal/models`
  - `internal/config`
  - `internal/store` only through narrow interfaces where needed
  - other runtime subpackages only when explicitly allowed below

### Forbidden direction

- no runtime subpackage may import `internal/runtime`
- `pipeline` must not import `manager`
- `pipeline` must not import `mcp`
- `manager` must not import `pipeline`
- `tools` must not import `manager`
- `tools` must not import `pipeline`
- `mcp` must not own tool execution logic
- `contracts` must not import implementation packages
- `testkit` must never be imported by production code

## Refactor Rules

1. Do not merge unrelated files just to reduce file count.
2. Do not extract a package unless the extracted code has a clear owner.
3. Root `internal/runtime` may expose wrappers for integration compatibility, but wrappers must stay thin.
4. New behavior belongs in the owning subpackage first, not in root runtime.
5. Test helpers must move into `internal/runtime/testkit` or the owning package; no new omnibus test files.

## Ownership Matrix

### `pipeline`

Owns:

- scan lifecycle
- discovery prefilter / dedup
- scoring accumulation / finalization
- validation gate tracking
- campaign completion semantics
- vertical stage projection

Does not own:

- protocol serving
- agent lifecycle
- raw tool execution

### `bus`

Owns:

- event persistence
- subscriptions
- direct publish
- routing tables
- delivery fanout
- interceptor execution

Does not own:

- pipeline state machine semantics
- agent lifecycle

### `manager`

Owns:

- agent spawn / reconfigure / teardown
- replay loop
- control loop
- receipts / retry / dead-letter
- OpCo bootstrap orchestration

Does not own:

- publish-path routing internals
- pipeline state machine transitions

### `mcp`

Owns:

- MCP HTTP / RPC adapter
- MCP-specific error envelopes
- MCP diagnostics

Does not own:

- tool behavior
- contract loading

### `tools`

Owns:

- executor behavior
- executor authorization
- emit tool surface
- tool schema loading

Does not own:

- agent lifecycle
- pipeline semantics
- MCP protocol serving

### `contracts`

Owns:

- generated registries
- contract compliance tests
- schema parity checks

Does not own:

- runtime loops
- event delivery
- tool execution

## Test Placement Rules

- tests live with the package that owns the behavior
- no omnibus `zzz_*` or `*_more_test` dumping grounds
- slow full-pipeline scenarios belong to integration-tier suites, not random unit files
- test helpers shared across owners live in `internal/runtime/testkit`

## Slow Test Tiers

- default lane: `go test ./internal/runtime/...`
- fast lane: `go test -short ./internal/runtime/...`
- `-short` skips:
  - canned runtime e2e suites
  - soak / long-run concurrency suites
  - postgres smoke coverage

## Placement Guide

- event transport, runtime epoch, turn context, and cycle detection: `internal/runtime/bus`
- scan scheduling, recovery, and future coordinator extractions: `internal/runtime/pipeline`
- agent/bootstrap persistence records and immutable bootstrap helpers: `internal/runtime/manager`
- MCP turn registry and future protocol-specific state: `internal/runtime/mcp`
- tool schema loading, emit helpers, and executor support code: `internal/runtime/tools`
- shared test helpers only: `internal/runtime/testkit`

## Current Top-Level Migration Targets

The remaining large root-runtime owners that still need to be drained are:

- `tool_executor*.go` -> `internal/runtime/tools`
- `tool_gateway.go` + MCP diagnostics/errors/context wrappers -> `internal/runtime/mcp`
- `pipeline_coordinator*.go`, `scan_campaign_manager.go`, `scoring_node.go`, `shard_dispatcher.go`, `sharding.go` -> `internal/runtime/pipeline`
- `manager*.go` runtime/opco/receipts ownership -> `internal/runtime/manager`
- `eventbus*.go` publish/routing/subscription ownership -> `internal/runtime/bus`

## Migration Order

1. create `testkit`
2. extract `pipeline`
3. move pipeline tests
4. extract `bus`
5. place cycle/transition primitives under one owner
6. extract `manager`
7. extract `mcp`
8. finish `tools`
9. extract `contracts`
10. add architecture guards
11. reduce root runtime to composition only

## Current Execution Plan

The remaining extraction work proceeds in this concrete order:

1. move ToolGateway implementation into `internal/runtime/mcp`
2. move MCP diagnostics into `internal/runtime/mcp`
3. move EventBus publish helpers and contracts into `internal/runtime/bus`
4. inject root-only hooks into `bus`
5. move `EventBus` implementation into `internal/runtime/bus`
6. move pipeline coordinator implementation into `internal/runtime/pipeline`
7. move pipeline-owned tests with that code
8. move tool executor implementation into `internal/runtime/tools`
9. inject narrow tool dependencies
10. remove root-owned owner tests
11. strengthen architecture guards
12. reduce root runtime to constructors and wrappers only
13. run full runtime verification
14. run full repo verification

## Definition Of Done

- `internal/runtime` contains only runtime-facing constructors, wrappers, and assembly wiring.
- `mcp`, `bus`, `pipeline`, and `tools` each own their implementation and tests.
- no runtime subpackage imports root `internal/runtime`.
- owner-specific tests live in owner packages.
- architecture guards fail fast on forbidden imports or new omnibus files.
