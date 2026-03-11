# MAS Platform v1.1.0 Phase 6 Subplan

Date: 2026-03-11
Author: Codex implementer

## Phase 6 Goal

Remove the remaining product-shaped policy, authority, tooling, prompt, session, and orchestration semantics from generic runtime layers.

By the end of Phase 6:

- generic runtime boot must no longer require Empire default policy or module wiring
- permissions and message authority must have a generic owner
- prompt identity and tool authorization must come from MAS/platform machinery rather than Empire alias maps
- remaining `productpolicy` reads in generic runtime must either disappear or become explicit product-owned injection points
- `internal/factory/` and `internal/commgraph/empire/` must have explicit disposition

This phase is the bridge between MAS conformance work and the final persistence/genericity cleanup phases.

## Starting Point

- the MAS-default runtime path is active and green in the core runtime packages
- `productpolicy` is no longer needed for active workflow semantics, but it still leaks into scan/discovery, prompt/runtime, authority, workspace, and tool layers
- the strongest current Phase 6 inputs came from the sidecar audit of active `productpolicy` callers and remaining product-surface packages
- Slice 6.1 has started:
  - `RuntimeOptions` now accepts explicit `WorkflowModule` and `ProductPolicy` injection
  - `NewRuntime` honors those injected boot dependencies and validates injected workflow contracts
  - generic runtime boot now requires those injected dependencies instead of falling back to registered Empire defaults
  - Empire composition now belongs at the product entrypoint, not inside generic runtime boot
- Slice 6.4 has started:
  - prompt lookup now prefers MAS bundle identity and flow/package-aware prompt directories before falling back to the legacy root prompt directory
  - hardcoded operating-role aliasing is reduced; prompt identity now resolves through agent-registry keys, runtime-id template matches, and role matches within the matching MAS flow mode
  - legacy fallback still exists, so the next step is to remove the remaining alias/path heuristics once the product composition boundary is cleaner
- Slice 6.3 has started in a narrow non-orchestrator cluster:
  - fallback scan mode in workflow projection, discovery aggregation, and scoring hydration now prefers MAS `default_scan_mode` from the resolved bundle
  - `productpolicy.DiscoveryFallbackMode()` remains only as a compatibility fallback inside the shared helper
  - `coordinator_projection.go` no longer carries hardcoded mode literals for expected-agent counts
  - dispatch-time `agents_expected` compatibility now routes through a helper instead of raw product taxonomy
  - persisted scan-state restore preserves explicit scanner keys while keeping the logical scanner-family shorthand behind a narrow compatibility helper
  - `scan_orchestrator_runtime.go` no longer imports or calls `productpolicy` directly; it now derives fallback mode and request dispatch behavior from the MAS bundle and node handlers, with a narrow compatibility fallback for the existing concrete scanner event family
  - `coordinator_scoring.go`, `coordinator_discovery.go`, and `discovery_aggregator_runtime.go` now use pipeline-owned mode/default helpers rather than the older wrapper path
  - the remaining wrapper path is narrower and explicitly concentrated in the shared compatibility helpers plus campaign/module scan-policy code

## Audit Outcome

The active `productpolicy` dependency surface breaks into three migration classes:

- contract-driven replacements
  - scan-mode normalization
  - scan dispatch kind / scanner counts
  - emit normalization/remediation
  - prompt/schema guards
- platform-builtin replacements
  - structured directive interception in manager dispatch
- explicit product-owned injection
  - control-plane identity
  - workspace class
  - global routing / management overrides
  - any remaining product-specific authority exceptions

That split will guide the slice order below.

Current package-disposition bias from the audit:

- `internal/factory/` should move product-owned first.
  - current production reach is already narrow and concentrated in Empire command wiring
- `internal/commgraph/empire/` should move product-owned behind a generic authority boundary.
  - do this after generic permissions/message authority exist
- `internal/runtime/productpolicy/empire/` should move product-owned only after the active generic callers are drained.
  - keep `internal/runtime/productpolicy/` itself only as a shrinking migration seam, not a place to add new behavior

## Audited Extraction Order For Remaining `productpolicy` Callers

The current recommended extraction order outside the already-active workflow/pipeline semantics is:

1. emit transition semantics
   - move event-authority and required-emission rules out of product policy and into contract/runtime contract enforcement
2. emit payload rewriting for scan and budget flows
   - move schema-level normalization into contract-owned transforms
   - leave only truly product-specific free-text interpretation as explicit product injection, if it survives
3. workspace class routing
   - move `workspace_class` ownership onto agent metadata plus a small generic resolver
4. scan mode and priority canonicalization outside pipeline
   - move alias/canonicalization into contracts or remove alias tolerance entirely
5. prompt-turn remediation and prompt schema guard extras
   - move these to explicit product-owned assets/validators rather than keeping them on the default runtime policy seam

This order is now the default sequencing rule for Slice 6.2 follow-up work.

## Slice 6.1: Remove Empire Defaults From Generic Boot

### Objective

Stop generic runtime construction from hardwiring Empire policy and module defaults.

### Target files

- `internal/runtime/runtime.go`
- `internal/runtime/productpolicy/policy.go`
- `internal/runtime/productpolicy/empire/policy.go`

### Work

- remove default `empirepipeline.NewModule()` and `empireproductpolicy.New()` wiring from generic runtime boot
- make product/module/policy registration explicit at composition boundaries instead of hidden defaults
- keep runtime construction viable for tests by using explicit injected fixtures instead of implicit global defaults

### Acceptance

- generic runtime boot does not import Empire policy/module packages as required defaults
- product composition is explicit and testable

## Slice 6.2: Split `productpolicy` By Ownership

### Objective

Replace the single product-shaped `Policy` abstraction with properly owned generic/platform/product surfaces.

### Target files

- `internal/runtime/productpolicy/policy.go`
- `internal/runtime/agents/agent_llm.go`
- `internal/runtime/tools/executor_emit_guardrails.go`
- `internal/runtime/tools/executor_emit_normalization.go`
- `internal/runtime/contracts/prompt_schema_guard.go`
- `internal/runtime/pipeline/*scan*`

### Work

- move contract-driven behavior onto workflow-module or contract loaders:
  - mode normalization
  - scanner selection/counts
  - emit normalization/remediation
  - prompt/schema contract checks
- keep only true platform-builtins in generic runtime
- convert product-only concerns into explicit injected interfaces instead of keeping them behind `productpolicy`

### Acceptance

- `productpolicy.Policy` is either deleted or drastically reduced to a clean, generic ownership boundary
- no active generic runtime path uses `productpolicy` as a backdoor for product behavior

Current slice status:

- active emit-authority and required-emission behavior for the tool/agent path is now owned locally in `internal/runtime/tools` instead of hidden behind `productpolicy`
- active emit normalization for scan and budget compatibility paths is now owned locally in `internal/runtime/tools`
- `agent_llm.go` no longer calls `productpolicy` for:
  - post-turn required-emission enforcement
  - strict contract text for emit requirements
  - remediation prompt generation
  - scan mode and priority normalization helpers
- this is still a generic-runtime hardcoded seam, not yet a MAS-contract-derived authority model

## Slice 6.3: Permissions And Message Authority Platformization

### Objective

Move permissions, producer authority, mailbox authority, and role aliasing out of Empire policy code.

### Target files

- `internal/commgraph/policy.go`
- `internal/commgraph/registry.go`
- `internal/commgraph/empire/policy.go`
- `internal/runtime/tools/authorizer.go`
- `internal/runtime/tools/emit_runtime.go`
- `internal/runtime/tools/executor_human_tasks.go`
- `internal/runtime/agents/agent_llm.go`

### Work

- implement the MAS permissions model declaratively
- create a generic owner for:
  - producer-role/event authority
  - mailbox send/decide permissions
  - role alias lookup
  - message-domain / peer / all routing permissions
- remove handwritten Empire role checks from live tool/agent/runtime flows

### Acceptance

- tool authorization and mailbox authority are no longer inherited from Empire policy code
- `internal/commgraph/empire/` is no longer the hidden source of truth for generic runtime authority

## Slice 6.4: Prompt Identity And Prompt Templating

### Objective

Make prompt identity and prompt variable substitution contract-driven.

### Target files

- `internal/runtime/contracts/prompts.go`
- `internal/promptcontracts/`
- `internal/runtime/agents/agent_llm.go`
- `internal/runtime/manager/agent_manager.go`

### Work

- remove `PromptAgentIDForConfig()` alias logic
- resolve prompt identity from the MAS agent registry and resolved bundle
- align prompt templating with MAS policy/contract assets instead of path heuristics and Empire alias tables

### Acceptance

- prompt targeting comes from MAS contracts, not hardcoded Empire role maps
- prompt assets load through the resolved bundle path

## Slice 6.5: Tool Model And MCP Gateway

### Objective

Align tool registration and authorization with the MAS tool model.

### Target files

- `internal/runtime/tools/`
- `internal/runtime/mcp/`
- `internal/runtime/contracts/workflow_contracts.go`

### Work

- separate platform-builtin tools from workflow-registered tools
- make tool schemas/registration contract-driven
- move tool authorization onto the generic permission model from Slice 6.3
- explicitly account for contract-derived entity tools if they remain part of the MAS surface

### Acceptance

- generic runtime tool exposure no longer depends on productpolicy/commgraph fallbacks
- MCP-facing tool behavior matches the MAS tool model

### Phase 6.5 Execution Prep

Current runtime behavior under `internal/runtime/tools/` is narrower than the target MAS tool model and should be treated as the migration baseline:

- universal tools are still hardcoded in runtime policy:
  - `agent_message`
  - `mailbox_send`
- emit-tool authorization is still handled through a separate role/event allowlist path
- non-universal, non-emit tools are only constrained if the actor config contains `tools` or `allowed_tools`
- if neither config key is present, the authorizer currently allows the tool by default
- denied tools emit `spec.contradiction_detected` from runtime, but that is still a runtime-side contradiction signal rather than a MAS permission-model decision
- the current authorizer does not yet distinguish tool ownership through the MAS `handler_type` split (`platform_builtin` vs `workflow_registered`)
- contract-derived entity tools are not yet first-class in the authorization model

That means Slice 6.5 should start from explicit parity tests for current behavior and then replace the current authorization lattice in one bounded tranche instead of incrementally layering more exceptions.

Slice 6.5 has now started at the test/conformance level:

- `authorizer_behavior_test.go` pins the current authorizer behavior for:
  - default-allow actors without tool constraints
  - universal tool bypass
  - emit-tool bypass through the separate emit path
  - merged `tools` + `allowed_tools` restrictions
  - contradiction emission on denied tools
- `authorizer.go` now routes every decision through an internal ownership/authorization classifier:
  - ownership: `platform_builtin` vs `workflow_registered`
  - authorization path: `universal`, `emit_allowed`, `actor_config`, `default_allow`, `denied`
- `authorizer_internal_test.go` pins that matrix without changing external behavior yet
- `executor_human_tasks.go` now asks `commgraph` for human-task decision authority instead of going through `productpolicy`
- `commgraph` now has the first explicit generic authority seam (`CanDecideHumanTasks`) even though the current concrete policy is still Empire-backed
- `global routing`, `global management`, and `mailbox_send` authorization now also route through explicit `commgraph` seams instead of inline role checks in `tools`
- `internal/runtime/tools/executor.go` is now a thin caller for those authority decisions; the concrete rules remain quarantined in `internal/commgraph/empire/`

## Local Workspace/Routing Progress

The workspace routing leak has also started moving out of `productpolicy`:

- `workspace_class` is now decoded from MAS agent contracts
- flow-instance activation now persists `workspace_class` and `manager_fallback` into agent config metadata
- `internal/runtime/workspace/manager.go` now resolves workspaces from agent config or the active MAS agent registry instead of `productpolicy.WorkspaceClass`
- `internal/runtime/mcp/diagnostics.go` now resolves diagnostic workspace routing from the same workspace metadata path
- current runtime topology still maps MAS `holding` to the existing infra container and leaves non-factory classes on the instance-local path as a compatibility routing layer
- the dead `WorkspaceClass` and `DiagnosticWorkspaceClass` methods have now been removed from the `productpolicy` interface entirely

### Highest-Risk Slice 6.5 Gaps

1. default-allow behavior for non-emit tools means authorization is still actor-config-driven rather than permission-model-driven
2. platform-builtin versus workflow-registered ownership is not an active runtime distinction yet
3. universal tool ownership is still a hardcoded runtime list instead of contract/materialized authority
4. emit-tool authority is still decoupled from the broader tool registry and permission model
5. MCP/tool registration and authorization are not yet driven by one shared contract-owned source of truth

### Concrete Next Implementation Candidate

The next implementation candidate for Phase 6.5 should be:

#### 6.5A: Introduce A Tool Ownership Matrix And Permission Resolver

Scope:

- build one runtime-owned view of tool metadata with, at minimum:
  - tool name
  - ownership class: `platform_builtin` or `workflow_registered`
  - authorization source: universal, permission-granted, emit-authorized, or product-injected compatibility
- make the executor/authorizer consume that shared view instead of separate hardcoded universal and emit paths
- preserve current behavior behind tests first, then replace the default-allow rule with an explicit compatibility mode that can be removed later

Primary implementation files for the next code tranche:

- `internal/runtime/tools/authorizer.go`
- `internal/runtime/tools/policy.go`
- `internal/runtime/tools/emit_runtime.go`
- `internal/runtime/tools/contracts.go`
- `internal/runtime/mcp/`
- `internal/runtime/contracts/workflow_contracts.go`

## Slice 6.6: Session And Scope Model Cleanup

### Objective

Replace Empire-shaped session/scope assumptions with generic MAS addressing and session ownership.

### Target files

- `internal/runtime/agents/agent_llm.go`
- `internal/runtime/sessions/postgres.go`
- `internal/events/types.go`
- `internal/runtime/workspace/manager.go`

### Work

- remove `session_per_vertical` and related vertical-only assumptions
- replace `VerticalID`-shaped generic scope with a generic scope/entity/instance vocabulary
- align session persistence with MAS addressing and instance ownership

### Acceptance

- generic event/session/runtime code no longer depends on `vertical` as a primitive scope model
- session identity aligns with generic flow/entity/instance semantics

## Slice 6.7: Product-Domain Package Disposition

### Objective

Give the remaining product-domain packages explicit end states instead of leaving them as silent leakage.

### Target files

- `internal/factory/`
- `internal/commgraph/empire/`

### Work

- inventory active production callers
- decide for each package whether it is:
  - product-owned and relocated/isolated
  - generalized into a platform package
  - deleted because MAS runtime now replaces it
- current bias from audit:
  - `internal/factory/` is likely product-owned unless MAS nodes fully replace it
  - `internal/commgraph/empire/` is likely product-owned data behind a generic authority interface

### Acceptance

- both packages have explicit file-level disposition
- generic runtime no longer depends on them implicitly

## Recommended Order

1. Slice 6.1
2. Slice 6.2
3. Slice 6.3
4. Slice 6.4
5. Slice 6.5
6. Slice 6.6
7. Slice 6.7

Reason:

- default Empire boot wiring has to disappear before the rest of the extraction can be honest
- permissions/authority need a generic owner before tool and prompt cleanup can finish cleanly
- session/scope cleanup depends on the earlier authority and prompt/tool decisions
- package disposition should happen after the shared surfaces they depend on are no longer product-shaped

## Recommended Package Order

1. `internal/factory/`
2. `internal/commgraph/empire/`
3. `internal/runtime/productpolicy/empire/`
4. collapse or remove the remaining generic `productpolicy` seam

Reason:

- `internal/factory/` already has a narrow Empire command surface and can be isolated earliest
- `internal/commgraph/empire/` must wait for generic permissions/authority to exist
- `productpolicy/empire` cannot move cleanly until the active generic runtime callers are reduced

## Exit Gate

Phase 6 is complete only if:

- generic runtime boot no longer requires Empire defaults
- `productpolicy` no longer acts as a catch-all product backdoor in generic runtime
- permissions, prompt identity, tool authorization, and session identity have generic owners
- `internal/factory/` and `internal/commgraph/empire/` have explicit end states and no hidden runtime dependence remains
