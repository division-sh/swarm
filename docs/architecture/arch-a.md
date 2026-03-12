# Architectural Plan: MAS Platform Extraction

**Date:** 2026-03-11
**Spec authority:** `docs/specs/mas-platform/platform/contracts/platform-spec.yaml` (v1.1.0)
**Litmus test:** Can a second product boot by supplying contracts, a product module, and a `main.go`, without editing generic code under `internal/runtime/`, `internal/commgraph/`, or `internal/models/`?

---

## Dependency Order

```text
P1 Contract Authority
  -> P2 Contract Normalization
    -> P3 Generic Runtime Kernel
      -> P4 Workflow State Authority
        -> P5 Tool + Permission Authority
          -> P6 Boot Compliance
            -> P7 Dynamic Flow Instances
```

Critical path: `P1 -> P2 -> P3 -> P4 -> P5`.

The first failure mode to eliminate is not cosmetic coupling. It is **authority ambiguity**: the code still has multiple competing sources of truth for contracts, routing, policy, runtime state, and product behavior.

---

## P1. Make the Contract Tree the Only Authority
**Priority:** Critical

### Current State

The loader can discover package trees and flow contracts, but generic code still defaults to Empire as the canonical runtime source and still treats legacy file forms as first-class inputs.

- `internal/runtime/contracts/workflow_contracts.go:1693-1775` defaults the workflow contracts directory to `docs/specs/mas-platform/empire/contracts`, then falls back to legacy files like `workflow.yaml`, `hooks.yaml`, and `runtime/*.yaml`.
- `internal/runtime/contracts/prompts.go:283-290` loads the prompt bundle by hardwiring the Empire contract root.
- `internal/runtime/tools/contracts.go:28-58` loads tool schemas from the default bundle, which today resolves to Empire unless explicitly overridden.
- `internal/runtime/contracts/schema_registry_generated.go:4` is generated from Empire runtime events, so the generic event schema registry is product-bound before runtime boot even starts.
- Empire’s own package manifest still declares `runtime_contracts` as the practical bridge and `target_contracts` as future intent, not present reality: `docs/specs/mas-platform/empire/contracts/package.yaml:19-25`.

### Spec Requires

The spec makes the flow/package tree authoritative. The runtime bridge exists only as a compatibility layer. Product identity, child flows, events, agents, tools, prompts, and policy must all come from the loaded product bundle, not from Empire defaults or legacy filenames.

### Required Changes

1. Remove Empire-specific defaults from generic loader, prompt, and schema-registry code.
2. Make product selection explicit at boot. The runtime must be given a product contract root; it must not infer Empire as the default product.
3. Demote legacy `workflow.yaml` / `hooks.yaml` / runtime-bridge loading to a migration path, not the kernel contract model.
4. Regenerate prompt, event, and tool registries from the selected product bundle rather than from Empire source paths baked into generic packages.

### Why This Priority

Everything else depends on one authoritative contract model. If the runtime can silently fall back to Empire, then any later “generic” refactor still rests on the wrong source of truth.

---

## P2. Normalize Contracts to the Spec’s Abstractions
**Priority:** Critical

### Current State

The in-memory contract model still carries Empire-era semantics that the spec explicitly says should be derived, not declared.

- `internal/runtime/contracts/workflow_contracts.go:1460-1472` models event entries with `emitter`, `consumer`, `runtime_handling`, `owning_node`, and `delivery_channel`.
- `internal/runtime/contracts/workflow_contracts.go:1474-1490` models agents without `permissions` or `permissions_bundle`, even though the spec makes those part of the platform permission system.
- `internal/runtime/contracts/workflow_contracts.go:749-776` still preserves `runtime_contracts` and `target_contracts` in the package model as concurrent structural concepts.

### Spec Requires

- `events.yaml` is payload-only. Routing and ownership are derived at boot from nodes and agents.
- Agent definitions can include `permissions` and `permissions_bundle`, and tool enforcement is based on those contract declarations.
- The package/flow model is centered on a single recursive flow tree, not dual “runtime vs target” authority.

### Required Changes

1. Strip routing/ownership concerns out of the event contract model and derive them exclusively from nodes and agents.
2. Add missing permission fields to the agent contract model.
3. Treat `runtime_contracts` as compatibility-only metadata and stop using it as a parallel structural source of truth in the kernel.
4. Tighten semantic bundle construction so the spec abstractions, not Empire compat fields, drive route derivation and boot validation.

### Why This Priority

Boot wiring, route derivation, tool authorization, and required-agent validation cannot be correct while the contract model itself encodes the wrong boundaries.

---

## P3. Shrink the Generic Runtime Kernel to Product-Neutral APIs
**Priority:** Critical

### Current State

The core runtime and pipeline APIs still expose Empire concepts directly.

- `internal/runtime/pipeline/module.go:19-120` defines public generic payload types like `ValidationStartedPayload`, `BrandRequestedPayload`, `VerticalKilledPayload`, `ScanAssignedPayload`, and many others.
- `internal/runtime/pipeline/module.go:388-397` requires generic modules to provide `ScanPolicy`, `DiscoveryPolicy`, `ScoringPolicy`, and `PayloadFactory`, which are Empire workflow concerns, not platform primitives.
- `internal/models/agent.go:5-15` makes `VerticalID` a first-class generic runtime field.
- `internal/runtime/productpolicy/policy_reader.go:87-140` hardcodes Empire scan-mode semantics like `saas_gap`, `saas_trend`, `local_services`, and `corpus`.
- The runtime boot path still uses globally installed default workflow/product policy factories, which keeps product policy in the generic call graph: `internal/runtime/runtime.go:104-143`.

### Spec Requires

The platform kernel is built around flows, agent roles, workflow instances, guards, actions, tools, timers, and events. Product semantics live in product modules and contracts.

### Required Changes

1. Move Empire payload types and payload factories out of generic `pipeline` APIs and into `internal/runtime/pipeline/empire` or another product-owned surface.
2. Remove product-shaped policy hooks from the generic `WorkflowModule` interface; the kernel should depend on contracts plus guard/action registries.
3. Replace generic `VerticalID` naming with entity-neutral semantics in generic models and runtime APIs.
4. Eliminate Empire scan-mode and lifecycle logic from generic product-policy helpers.

### Why This Priority

This is the load-bearing platform boundary. If product behavior still appears in generic public types, then every second product necessarily changes generic code.

---

## P4. Make `workflow_instances` the Sole Runtime State Authority
**Priority:** High

### Current State

The runtime already has a spec-shaped `workflow_instances` store, but generic runtime execution still restores and maintains Empire-specific side stores and compatibility buckets.

- `internal/runtime/pipeline/workflow_instance_store.go:17-49` matches the spec closely: workflow identity, current state, transition history, accumulator state, timer state, and metadata.
- `internal/runtime/pipeline/state_store.go:32-218` still restores runtime state from Empire-specific tables such as `scan_accumulators`, `pending_dedup_candidates`, `validation_pipelines`, and `pipeline_processed_events`.
- `internal/runtime/pipeline/workflow_instance_projection.go:14-42` explicitly labels state buckets as `platform`, `product`, and `compatibility`.

### Spec Requires

Platform-managed workflow state lives in `workflow_instances`. Product workflows can use `accumulator_state` and `metadata`, but the platform should not require parallel product tables to reconstruct core execution state.

### Required Changes

1. Migrate recovery and live execution to rely on `workflow_instances` as the canonical workflow state store.
2. Move any remaining Empire-specific projection stores behind product-owned adapters, or eliminate them if their state can live in accumulator buckets.
3. Stop classifying buckets as platform vs product compatibility in generic code; generic code should only manage opaque, contract-addressed node state.
4. Treat legacy tables as migration inputs only, not as runtime dependencies.

### Why This Priority

A platform cannot be generic if its recovery path still assumes Empire tables. This is the state-side equivalent of P1: the runtime needs one state authority.

---

## P5. Replace Hardcoded Tool and Authority Logic with Contract-Driven Permissions
**Priority:** High

### Current State

The tool layer is structurally generic, but its registration and authorization model is still not the spec’s model.

- `internal/runtime/tools/handler_registry.go:9-71` hardcodes the set of available platform and external tools in generic code.
- `internal/runtime/tools/authorizer.go:47-149` effectively defaults to allow unless the actor config contains explicit tool lists; it does not enforce contract permissions.
- `internal/commgraph/authority.go:59-157` enforces message, routing, management, and mailbox rules via a parallel authority model based on roles, not the spec permission model.
- There is no platform-generated persistence tool set from `entity_schema`; ripgrep finds only entity schema validation usage, not generated `get_entity`, `save_entity_field`, `search_entities`, or `query_metrics`.

### Spec Requires

- The platform owns tool schema serving.
- Tool execution and message scope are enforced from agent permissions declared in contracts.
- Persistence tools are auto-generated from `entity_schema`.

### Required Changes

1. Add `permissions` / `permissions_bundle` to the runtime agent model and enforce them in the tool executor.
2. Move workflow-specific tool registration behind product/workflow registration hooks rather than generic hardcoded handler maps.
3. Replace commgraph’s separate message/routing authority model with permission-based enforcement aligned to the spec.
4. Generate typed persistence tools from `entity_schema` and remove generic reliance on raw product tables and raw SQL paths for agent access.

### Why This Priority

This is the main runtime safety boundary. Without it, “generic runtime” only means “shared code with product-specific allowlists hidden in logic.”

---

## P6. Finish Boot-Time Compliance Enforcement
**Priority:** Medium

### Current State

There is meaningful validation logic, but the MAS boot path still explicitly marks several required checks as placeholders.

- `internal/runtime/pipeline/workflow_contract_validation.go` validates many useful invariants already.
- `cmd/mas/main.go:260-276` still logs `validate_pins`, `validate_required_agents`, `validate_tools`, `validate_permissions`, and `validate_platform_version` as deferred or placeholder steps.

### Spec Requires

Boot must enforce flow coherence, node coherence, agent coherence, project coherence, and platform-version compatibility.

### Required Changes

1. Promote placeholder boot checks into enforced runtime validation.
2. Validate required input pin satisfaction, write-pin exclusivity, required-agent fulfillment, tool existence, permission sufficiency, and handoff completeness.
3. Enforce `platform_version` semver compatibility at boot instead of logging it as future work.

### Why This Priority

This is downstream of P1-P5. Validation should be built on the final authority model, not on the current hybrid one.

---

## P7. Complete Dynamic Flow Instances as a Platform Primitive
**Priority:** Medium

### Current State

Template flow support exists and is a strong base, but the activation path still carries Empire assumptions.

- `internal/runtime/pipeline/workflow_transition_engine.go:436-535` implements `create_flow_instance`, initializes workflow state, and auto-emits instance-local events.
- `internal/runtime/manager/flow_activation.go:15-190` activates template flows, but still requires `vertical_id`, creates vertical workspaces/schemas, and builds agent configs in Empire terms.
- `internal/runtime/bus/routing_derivation.go:43-165` supports template routes, but still contains a compatibility fallback for an “odd handoff signature.”
- `internal/runtime/bus/eventbus.go:114-124` exposes `AddFlowInstance`, so the routing surface is already close to what the spec needs.

### Spec Requires

`create_flow_instance` is a platform-owned primitive: instantiate a template flow, register nodes/agents/events in runtime routing, create the instance record at the initial state, and start participants.

### Required Changes

1. Keep template flow registration in the platform, but move workspace/entity provisioning behind product hooks.
2. Replace `vertical_id` assumptions in instance activation with entity-neutral flow-instance semantics.
3. Remove handoff compatibility fallbacks and make dynamic routing purely contract-derived.
4. Ensure instance-local subscriptions and event namespaces are resolved entirely from the flow contracts.

### Why This Priority

Dynamic flow instances are a real platform capability, but they should be finished after the kernel, state, and permission foundations are correct. Otherwise the capability just reproduces Empire assumptions at runtime.

---

## Summary

The first architectural move is not to “extract Empire files.” It is to remove **competing authorities**:

1. Empire-default contract loading vs product-selected loading.
2. Declared routing metadata vs derived routing.
3. Product-shaped generic APIs vs platform-neutral kernel APIs.
4. `workflow_instances` vs Empire side stores.
5. Contract permissions vs hardcoded tool/role authorities.

Once those five authority conflicts are resolved, the remaining work becomes additive: compliance hardening, full template-flow support, and product-specific modules that no longer leak back into the platform.
