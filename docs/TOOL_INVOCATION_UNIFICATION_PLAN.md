# Tool Invocation Unification Plan

## Purpose

Unify tool invocation semantics behind one canonical runtime model.

This workstream exists because tool semantics are currently distributed across:

- gateway transport allow/deny logic
- gateway context fallback logic
- tool-name normalization
- validator input compatibility rules
- authorizer classification
- executor normalization and dispatch
- native-tool registration
- emit-event/tool equivalence handling
- observability/telemetry surfaces

That distribution creates the same class of problem previously seen in flow identity:

- one concept
- many owners
- local heuristics
- silent fallback
- expensive debugging

The goal is to remove that architecture class, not to patch individual symptoms.

## Non-Negotiable Standards

1. No exception matrix.
   - Do not introduce a growing list of special-case tool paths.

2. No fallback semantics for core tool identity or authorization.
   - A tool call should not mean different things depending on which layer handled it.

3. One semantic owner.
   - Tool identity, visibility, authorization, context requirements, compatibility normalization, denial reasons, and execution telemetry must all derive from one canonical model.

4. Fail closed on invalid or ambiguous tool semantics.
   - Unknown tool identity, ambiguous aliasing, or invalid context requirements must fail explicitly.

5. Observability must be canonical, not reconstructive.
   - The runtime must emit canonical tool execution and denial telemetry rather than forcing operators to infer it later.

## Clear Goals

This workstream is not done until all of the following are true.

### Goal 1: Canonical Tool Identity

There is one authoritative representation of tool identity used by:

- gateway
- validator
- authorizer
- executor
- dispatcher
- native-tool registration
- telemetry

Success criteria:

- no subsystem performs semantically meaningful private tool-name normalization
- aliases, if they exist, are defined in one place only
- raw request tool names are resolved once into a canonical identity

### Goal 2: Canonical Tool Capability Policy

There is one authoritative decision model for:

- whether a tool is visible
- whether it is callable
- what context it requires
- whether the actor is authorized
- what input compatibility rules apply
- what denial reason applies

Success criteria:

- gateway and executor do not implement separate authorization semantics
- validator compatibility checks are derived from the same canonical tool capability model
- mutating vs read-only tool semantics are not inferred independently in multiple layers

### Goal 3: Precomputed Per-Turn Tool Capability Set

Each turn computes one canonical tool capability set once, and downstream layers consume it.

Success criteria:

- visible / allowed / callable tool sets cannot diverge because of local re-evaluation
- the per-turn capability set includes canonical tool ids and denial metadata
- request allowlists and runtime tool offerings are materialized from the same canonical source

### Goal 4: Explicit Context Requirements

Tool context requirements are modeled explicitly, not via local fallback behavior.

Success criteria:

- tool invocation context is validated against an explicit requirement model
- no hardcoded context-token fallback lists remain for core semantics
- a tool either has the required context or fails explicitly

### Goal 5: Canonical Input Compatibility Handling

Tool input compatibility and payload normalization are handled in one canonical place.

Success criteria:

- validator and executor do not each maintain separate payload rewrite logic
- input normalization is explicit and attributable
- invalid payload shapes fail with one canonical reason

### Goal 6: Canonical Tool Execution Telemetry

Every tool invocation emits canonical telemetry.

Success criteria:

- each tool invocation records:
  - actor
  - canonical tool id
  - latency
  - success/failure
  - denial reason or execution error
  - normalized input/output summary
- operators can distinguish:
  - tool not offered
  - tool offered but denied
  - tool called and failed
  - tool called and succeeded

### Goal 7: No Residual Semantic Drift Between Layers

There must be no remaining semantically important alternate path where one layer can accept, deny, rename, or reinterpret a tool differently from the canonical model.

Success criteria:

- all semantically important call sites are audited
- remaining helpers are wrappers or transport plumbing only
- there is no meaningful “gateway semantics” versus “executor semantics” split left

## Architecture Direction

Introduce a canonical tool invocation model, likely centered on two explicit concepts:

1. `ToolIdentity`
   - canonical tool id
   - alias set, if allowed
   - origin/class
   - mutability / side-effect class

2. `ToolInvocationPolicy`
   - visibility rules
   - authorization rules
   - context requirements
   - compatibility/normalization rules
   - denial reasons

And one per-turn materialization:

3. `TurnToolCapabilitySet`
   - canonical offered tools
   - canonical callable tools
   - denial metadata
   - normalized request mapping

Exact Go type names may differ, but the semantic split should remain.

## Phases

### Phase 1: Audit and Guardrails

Map every semantically important tool-invocation call site:

- gateway
- validator
- authorizer
- executor
- dispatcher
- native-tool registration
- emit-tool special handling
- observability/telemetry

Add focused tests that pin current divergence cases:

- visible but denied later
- raw name accepted by one layer but not another
- context fallback changing outcome
- validator/executor normalization mismatch

#### Phase 1 Audit Status

The first audit pass identified these semantically important ownership seams:

1. Tool identity normalization
   - `internal/runtime/mcp/gateway.go`
   - `internal/runtime/tools/native_tools.go`
   - `internal/runtime/tools/executor.go`
   - current smell:
     - gateway and runtime maintain separate normalization functions
     - MCP-prefixed names and native aliases are still normalized locally

2. Tool authorization / capability classification
   - `internal/runtime/tools/authorizer.go`
   - `internal/runtime/mcp/gateway.go`
   - `internal/runtime/tools/executor_emit.go`
   - current smell:
     - allowlists, emit permissions, native fallback permissions, and actor-config tools are still decided in more than one place

3. Tool input compatibility / normalization
   - `internal/runtime/tools/executor.go`
   - `internal/runtime/tools/validator.go`
   - current smell:
     - executor and validator still maintain duplicated payload-rewrite logic

4. Tool resolution / dispatch
   - `internal/runtime/tools/registry.go`
   - `internal/runtime/tools/dispatcher.go`
   - `internal/runtime/tools/native_tools.go`
   - current smell:
     - builtin/native/contract/MCP resolution is still split across resolver and fallback registration paths

5. Context requirements
   - `internal/runtime/mcp/gateway.go`
   - current smell:
     - direct tool path and MCP path still share the same idea through duplicated fallback logic rather than one canonical context requirement model

6. Tool execution telemetry
   - `internal/runtime/tools/executor.go`
   - current smell:
     - execution hook exists but still discards canonical execution data

7. Tool capability and authorization policy
   - `internal/runtime/tools/authorizer.go`
   - `internal/runtime/tools/executor_emit.go`
   - `internal/runtime/tools/emit_runtime.go`
   - `internal/runtime/tools/native_tools.go`
   - `internal/runtime/mcp/gateway.go`
   - current smell:
     - capability decisions are still split across:
       - permission checks
       - emit permission checks
       - native capability checks
       - actor-config allowlists
       - request-level `allowed_tools`
       - read-only context fallback rules
     - gateway and executor still do not consume one canonical capability policy

#### Phase 1 Closure Checklist

Phase 1 is not complete until:

- every semantically important tool layer is listed above or intentionally ruled out as transport-only
- each identified divergence has either:
  - a focused guardrail test, or
  - a documented rationale for why it is not semantically meaningful
- the remaining design questions are explicit:
  - whether aliases survive beyond ingress
  - whether emit tools remain a special case or become ordinary capability-classified tools
  - whether context fallback survives at all once canonical context requirements exist
  - how far the precomputed capability set should extend beyond gateway-mediated execution into direct LLM turn execution

#### Phase 1 Progress

Current checkpoint:

- added a first guardrail proving validator and executor normalization stay aligned for current duplicated payload rewrite cases
- added alias/canonicalization guardrails for gateway-side and runtime-side normalization
- introduced a first shared canonical helper:
  - `internal/runtime/core/toolidentity`
  - currently owns canonical tool-name resolution and emit-tool classification
- collapsed executor-side and validator-side payload rewrite logic onto one shared helper:
  - `internal/runtime/tools/tool_input_normalization.go`
- implemented canonical executor-side tool telemetry on the existing runtime log surface:
  - success, denial, and failure now emit structured tool-execution runtime logs
  - focused tests cover all three outcomes
- canonicalized config/request allowlist identities so raw aliases collapse to canonical tool ids
- added actor-aware tool listing at the gateway boundary so `tools/list` can prefer actor-scoped definitions over global catalog output
- moved context-fallback classification into the shared tool-identity layer instead of leaving it as a gateway-private rule
- changed request-level `allowed_tools` from a parallel gateway filter into a consumer of the canonical capability set
- changed `tools/list` to consume the visible subset of the canonical capability set instead of inventing tool rows from request allowlists
- changed execution-context capability attachment from one-tool snapshots to full per-turn catalog capability sets
- changed executor authorization to fail closed when a canonical capability set is present but the requested tool is missing or not callable
- extended the same per-turn capability-set model into direct LLM conversation tool execution so gateway and non-gateway turns do not drift
- changed actor-side tool offering to prefer actor-scoped executor definitions over global fallback when available
- moved gateway context-fallback decisions onto the canonical capability model instead of a gateway-private tool list
- changed actor-scoped executor tool definitions to include enabled native tools so offering and execution do not drift
- changed actor-scoped offering and execution-time resolution to consume the same actor-scoped registered-tool registry
- changed LLMAgent construction to treat executor-provided actor-scoped offerings as canonical instead of re-filtering them locally
- changed executor actor-scoped offerings to include emit tools via the same actor-scoped owner used by native and registered tools
- aligned gateway actor-aware emit listing with the same actor-scoped emit generation path
- removed the old boolean `AllowsContextFallback` semantic helper so context handling now consumes explicit `ContextRequirement`
- changed gateway emit dedupe to consume capability kind from the capability set instead of raw emit-name prefix checks
- changed LLM terminal-tool behavior to consume capability kind from the capability set instead of raw emit-name prefix checks
- removed duplicate emit authorization inside `handleEmitTool`; canonical authorization now happens before dispatch
- tightened runtime-facing executor contracts so gateway, conversation, and LLM-agent construction require capability-aware executors by type instead of optional side-interfaces
- tightened runtime-facing offering contracts so gateway and LLM-agent construction require actor-scoped tool definitions by type instead of optional type assertions

This does **not** complete the workstream.
It is only the first reduction of duplicated semantics.

### Phase 2: Canonical Tool Identity Layer

Create the shared identity model:

- canonical tool ids
- alias handling, if any
- one-time request-name resolution

Migrate all semantically important call sites onto it.

### Phase 3: Canonical Invocation Policy

Create the shared policy model:

- visibility
- authorization
- context requirements
- compatibility normalization
- denial reasons

Make gateway, validator, authorizer, and executor consume it.

### Phase 4: Per-Turn Capability Materialization

Materialize one canonical capability set per turn.

Use that set for:

- tool offering
- request allowlist generation
- authorization decisions
- denial diagnostics

### Phase 5: Tool Telemetry

Implement canonical tool execution telemetry from the shared model.

This is part of the workstream, not an optional add-on.

### Phase 6: Final Semantic Audit

Audit all remaining tool-related logic and classify each site as:

- canonical semantic consumer
- wrapper only
- still semantically divergent

The workstream is not done until no semantically divergent paths remain.

## Testing Requirements

The workstream is not done until tests prove:

1. Tool identity is resolved once and consistently across layers.
2. Gateway and executor produce the same authorization outcome from the same canonical capability set.
3. Context requirements fail closed.
4. Validator and executor consume the same compatibility normalization behavior.
5. Telemetry exists for successful, denied, and failed tool calls.
6. No known previous divergence case remains reproducible.

## Spec / Design Checkpoint

Checkpoint after Phase 1 or early Phase 2 if needed.

Questions to confirm:

- should tool aliases exist at all, or should the canonical model ban them except at ingress boundaries?
- should mutating/read-only/tool-origin classification become an explicit runtime descriptor?
- should denial reasons become spec-visible or remain runtime-internal?

This checkpoint is for clarification, not for inventing fallback mechanisms.

## Definition of Done

This workstream is done only when:

- all clear goals above are met
- all semantically important tool-invocation layers use the same canonical model
- no fallback semantics remain for core tool identity / authorization / context handling
- canonical tool execution telemetry exists
- the final semantic audit shows no remaining divergent semantic paths
- the repo test suite is green from that baseline

## Final Semantic Audit

Status:

- complete as of 2026-04-04

Goals 1-7:

- Goal 1: met
  - canonical tool identity is centralized in `internal/runtime/core/toolidentity`
  - runtime-facing layers no longer maintain divergent private alias semantics
- Goal 2: met
  - authorization, visibility, callable state, kind, context requirement, and denial metadata are materialized through the canonical capability policy/model
- Goal 3: met
  - gateway, direct LLM execution, and executor authorization consume per-turn capability sets instead of recomputing semantics independently
- Goal 4: met
  - context handling now consumes explicit `ContextRequirement`
  - the old `AllowsContextFallback` semantic helper was removed
- Goal 5: met
  - validator and executor share one canonical runtime input-normalization path
- Goal 6: met
  - canonical tool-execution telemetry is emitted on the runtime log surface
- Goal 7: met
  - runtime boundaries no longer have a meaningful “gateway semantics” vs “executor semantics” vs “conversation semantics” split for tool invocation

Former divergent paths removed:

- gateway request allowlists acting as a parallel visibility/auth layer
- gateway-private context fallback tool classification
- executor authorization succeeding without canonical capability materialization
- direct LLM tool execution without canonical capability materialization
- actor-side tool offering drifting from execution-time actor registry
- emit dedupe and terminal-tool behavior depending on raw `emit_` prefix checks
- duplicate emit authorization inside `handleEmitTool`
- runtime-facing executor contracts treating capabilities and actor-scoped offerings as optional side-interfaces

Remaining helper sites classified as wrapper/plumbing only:

- `normalizeGatewayToolName(...)`
  - wrapper onto canonical tool identity
- `normalizeNativeToolName(...)`
  - wrapper onto canonical tool identity
- `toolKindPolicy(...)`
  - canonical policy projection, not an alternate decision path
- `toolContextRequirementPolicy(...)`
  - canonical policy projection, not a gateway/executor-specific fallback
- test/probe executors implementing `ToolCapabilitiesForActor(...)` and `ToolDefinitionsForActor(...)`
  - test scaffolding only

Verification baseline:

- `go test ./internal/runtime/... -count=1`
- `go test ./... -count=1`

Conclusion:

- this workstream has reached core-complete status
- remaining future work in this area is follow-up hardening or feature evolution, not unfinished architectural unification
