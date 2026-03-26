# EmpireAI v2.5.0 — Platform Execution Primitives + Compliance Guidelines

**Version:** 2.5.0
**Previous:** 2.4.0
**Date:** 2026-03-09

## Summary

Defines the platform execution primitives that enable Phase 11 (declarative node execution) and the compliance guidelines the implementer follows to maintain spec conformance. Also adds namespace_prefix and required_agents to all 4 flow schemas, and provides runtime/ bridge files for current loader compatibility.

## Platform Execution Primitives

### Generic patterns (34 of 47 handlers)
The engine interprets these handler fields natively, no product code needed:
- `advances_to` — state transition
- `emits` — event emission
- `sets_gate` — gate state update
- `guard` — condition evaluation
- `data_accumulation` — entity field writes
- `on_complete` — conditional branching
- `rules` — typed routing
- `action: record_evidence` — payload append

### Accumulate primitive (+7 handlers = 41 total)
Track multiple events for same entity, fire on completion.
- `accumulate.track` — expected items list
- `accumulate.completion` — all | threshold | timeout
- `accumulate.on_complete` — what to do when done
- Platform guarantees: idempotent, crash-safe, timeout-aware

### Dispatch primitive (+1 handler = 42 total)
Fan-out work to multiple agents from single trigger.
- `dispatch.targets` — target mapping
- `dispatch.event_per_target` — event to emit per target
- `dispatch.track_completion` — set up accumulation for responses

### Query and Format primitive (+1 handler = 43 total)
Read state across entities, compile summary, emit digest.
- `query.scope` — which entities
- `query.fields` — which fields
- `format.template` — summary structure

### Product hooks (4 handlers, 9%)
Require product-specific code in approved packages:
- process_dedup, process_synthesis, resolve_contest, reset_pipeline_state

### Coverage: 43/47 handlers (91%) declarative, 4/47 (9%) product hooks

## Compliance Guidelines

### Boot validation (10 checks)
YAML parse, flow directories exist, input pins wired, required agents fulfilled, no data pin conflicts, namespace collision-free, tools resolve, permissions sufficient, payload schemas exist, entity schema covers all writes.

### Runtime enforcement (11 checks)
Tool schema validation, permission checks, message scope enforcement, state transition validation, guard evaluation, payload validation, accumulation idempotency.

### Handler execution rules (10 rules)
Ordered execution: match event → guard → accumulate → advances_to → emits → sets_gate → data_accumulation → on_complete → rules → product hook.

### Testing requirements (9 tests)
Boot validation, handler coverage, state paths, payload schemas, subscription resolution, permission enforcement, accumulation idempotency, crash recovery, namespace substitution.

### Product boundary rules (4 rules)
Product code in approved packages only, zero product refs in generic code, hooks registered at bootstrap, CI guard enforced.

## Flow Schema Updates

All 4 flows now declare:
- `namespace_prefix` — event prefix for reuse substitution
- `required_agents` — agent role sockets the project must fill
  - discovery: 3 roles (market_researcher, trend_researcher, scanner)
  - scoring: 1 role (analyst)
  - validation: 5 roles (researcher, spec_writer, spec_reviewer, technical_reviewer, brand_designer)
  - operating: 2 roles (tech_lead, product_lead)

## Runtime Bridge

`empire/runtime/` contains flat merged files for current loader compatibility:
- `runtime/nodes.yaml` — all 6 nodes merged
- `runtime/events.yaml` — all 175 events with full metadata (owning_node, runtime_handling, consumer)

Loader reads from runtime/ until multi-file loading is implemented. package.yaml documents both runtime_contracts and target_contracts.

## Platform-spec Sections (18 total)

platform, vocabulary, contract_formats, workflow_state, builtin_hooks, compliance_rules, versioning, file_layout, permissions_model, tool_model, directive_pattern, persistence_model, event_schema, prompt_templating, flow_model, project_model, execution_primitives, compliance_guidelines

## Contract-Driven Policy (replacing Go policy functions)

### agents.yaml additions
- `workspace_class` on all 28 agents (holding | factory | opco). Replaces WorkspaceClass().
- `manager_fallback` on all 28 agents (agent ID to escalate to). Replaces ManagerFallbackAgentID().

### policy.yaml additions
- `scan_modes` map: 4 modes (local_services, saas_gap, saas_trend, corpus) with rubric, signal types, priority.
  Replaces: NormalizeScanMode(), DefaultScanMode(), RubricNameForScanMode(), EmitsCategorySignals(), EmitsTrendSignals().
- `default_scan_mode`: saas_gap

### What this replaces
Of the 20 Go policy functions identified:
- 14 now read from contracts (permissions, scan modes, workspace class, manager fallback)
- 4 are platform primitives (payload validation, emit enforcement, directive routing, transition validation)
- 2 remain as legitimate product hooks (AdditionalTurnRequirement, ContractRemediationPrompt)

### Platform-spec additions
- `agent_registry_fields`: documents workspace_class, manager_fallback, permissions as contract fields
- `policy_driven_behavior`: documents scan_modes, permission_bundles, thresholds as data-driven policy
- 4 additional compliance checks for contract-driven enforcement
