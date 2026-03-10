# Plan: Full v2.6.0 Compliance

## Goal

Adopt `v2.6.0` completely, not just through the runtime bridge.

By the end of this plan:

- root runtime/contracts behavior is compliant with `v2.6.0`
- the loader understands the package/flow layout
- the runtime can consume flow-local contracts rather than only merged compatibility files
- routing derives from `agents.yaml + nodes.yaml` rather than legacy event-catalog metadata
- node execution follows the `v2.6.0` handler model and normative execution order
- the system is green under full-suite verification

This plan assumes the current codebase starts from:

- `2.2.1`/`2.4.x`-style runtime architecture
- strong genericity boundary already enforced
- current runtime still fundamentally transition-first
- current runtime still expects a merged contract bundle

## Phases

### Phase 1: Package-Aware Loader

Implement a package/flow-aware contract loader without changing runtime semantics yet.

Work:
- parse `platform/package.yaml`
- parse `empire/package.yaml`
- resolve project-level files
- resolve flow directories listed in `package.yaml`
- resolve `runtime_contracts` and `target_contracts`
- build one internal bundle API that can represent both bridge and target layouts

Files likely touched:
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contracts/workflow_contracts_test.go`

Done when:
- loader can read `docs/specs/empireai-v2_6_0/contracts-v250/empire/package.yaml`
- loader can enumerate all 4 flows plus project-level contracts
- tests prove runtime bridge files and flow directories are both discoverable
- no runtime behavior has changed yet

### Phase 2: Runtime Bridge Adoption

Switch the current runtime to consume the `v2.6.0` compatibility bridge files first.

Work:
- prefer:
  - `empire/runtime/nodes.yaml`
  - `empire/runtime/events.yaml`
  - `empire/runtime/agents.yaml`
- preserve fallback only as temporary migration support
- update current contract-path tests

Files likely touched:
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contracts/workflow_contracts_test.go`
- any code still assuming flat root files

Done when:
- current runtime loads from `empire/runtime/*` under `v2.6.0`
- current compliance/tests are green against the bridge files
- no direct dependency on root flat `workflow-schema.yaml`/`system-nodes.yaml` remains in active loading paths

### Phase 3: Bundle Merge Layer

Introduce a merge layer that can assemble a runtime view from multiple project/flow files.

Work:
- merge project-level nodes/events/agents/tools/policy
- merge flow-level nodes/events/agents/tools/policy
- preserve source provenance in memory for debugging/validation
- expose one merged runtime view to the rest of the code

Files likely touched:
- `internal/runtime/contracts/workflow_contracts.go`
- new merge helpers under `internal/runtime/contracts/`

Done when:
- runtime can build a merged node/event/agent view from:
  - project-level files
  - per-flow files
- the merged result is structurally equivalent to current runtime bridge inputs
- current pipeline/runtime code only consumes the merged bundle API

### Phase 4: Compliance Rebase For v2.6.0

Rebase compliance/testing onto package + bridge semantics.

Work:
- update `TestContractCompliance`
- validate package structure
- validate flow directories
- validate runtime bridge presence and coherence
- validate merged bundle output

Files likely touched:
- `internal/runtime/contract_compliance_test.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`
- related tests

Done when:
- contract compliance is green under `v2.6.0` bridge contracts
- compliance no longer assumes old flat file layout
- package + flow structure is part of CI enforcement

### Phase 5: Replace Flat Workflow Assumptions

Remove the remaining hard dependency on `workflow-schema.yaml` and `guard-action-registry.yaml` as the runtime’s primary semantic source.

Work:
- identify all runtime sites that expect:
  - `Workflow.Workflow.Transitions`
  - flat hook registry
  - flat event catalog semantics
- move those assumptions behind bundle adapters
- stop treating old flat workflow files as the contract source of truth

Files likely touched:
- `internal/runtime/pipeline/workflow.go`
- `internal/runtime/pipeline/workflow_transition_engine.go`
- `internal/runtime/pipeline/workflow_nodes.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`

Done when:
- no active runtime path requires a physical flat workflow file to exist
- semantic access comes from adapter/bundle APIs, not file-specific assumptions

### Phase 6: Flow Schema Support

Make `schema.yaml` a first-class runtime input.

Work:
- parse:
  - `states`
  - `initial_state`
  - `terminal_states`
  - `pins`
  - `required_agents`
  - `namespace_prefix`
- map flow schemas into the internal workflow model
- validate no write-pin conflicts across flows

Files likely touched:
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`
- compliance tests

Done when:
- runtime sees flow schemas as authoritative state/pin definitions
- compliance validates flow pin collisions and required-agent fulfillment
- state definitions can be derived from flow schemas rather than only old workflow files

### Phase 7: Handler-Derived Internal Transition View

Build an adapter that derives the current runtime’s transition/state needs from node handlers.

Work:
- derive internal transition-like semantics from handler fields:
  - `advances_to`
  - `guard`
  - `sets_gate`
  - `data_accumulation`
  - `emits`
  - `rules`
  - `on_complete`
- keep current engine running on that derived internal view initially
- avoid big-bang engine rewrite

Files likely touched:
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/pipeline/workflow.go`
- `internal/runtime/pipeline/workflow_transition_engine.go`

Done when:
- current runtime can execute from handler-derived semantics rather than direct old transition documents
- old transition-first model is now an internal compatibility representation, not the external contract model

### Phase 8: Routing Derivation

Move routing authority from event-catalog metadata to `agents.yaml + nodes.yaml`.

Work:
- derive runtime node delivery from:
  - `nodes.yaml` subscriptions
  - owning system node
  - execution type
- derive agent delivery from:
  - `agents.yaml` subscriptions
  - project/flow agent scope
- keep bridge metadata only where still needed during transition

Files likely touched:
- `internal/runtime/pipeline/workflow_nodes.go`
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/factory/contracts_policy.go`
- runtime/compliance tests

Done when:
- runtime no longer depends on old event-catalog consumer/routing fields for normal delivery
- routing is derived from agent/node contracts
- bridge event metadata is no longer the authoritative routing model

### Phase 9: Target Flow File Adoption

Switch from `empire/runtime/*` compatibility files to the real target flow files.

Work:
- load from:
  - `flows/*/nodes.yaml`
  - `flows/*/events.yaml`
  - `flows/*/schema.yaml`
  - `flows/*/agents.yaml`
  - project-level files
- keep `empire/runtime/*` only as migration fallback until cutover is proven
- then remove runtime dependence on bridge files

Files likely touched:
- `internal/runtime/contracts/workflow_contracts.go`
- compliance tests

Done when:
- active runtime path reads target flow files, not bridge files
- `empire/runtime/*` is no longer required for normal operation
- bridge files are optional compatibility only, or removable

### Phase 10: Normative Handler Execution Order

Implement the `v2.6.0` 10-step handler execution order as the real runtime model.

Required order:
1. `guard`
2. `accumulate`
3. `compute`
4. `on_complete`
5. `advances_to`
6. `sets_gate`
7. `data_accumulation`
8. `emits`
9. `rules`
10. `action hook`

Work:
- map the current engine to the normative order
- implement transactional handler execution
- enforce rollback/idempotency semantics
- handle `rules` as alternative path vs `advances_to + emits`

Files likely touched:
- `internal/runtime/pipeline/workflow_transition_engine.go`
- `internal/runtime/pipeline/workflow_hooks.go`
- `internal/runtime/pipeline/system_node_runner.go`
- related runtime store / transaction files

Done when:
- runtime executes handlers in the `v2.6.0` normative order
- tests prove ordering, idempotency, and rollback behavior
- no major runtime behavior still depends on the pre-`2.6.0` execution model

### Phase 11: Full v2.6.0 Compliance Closeout

Eliminate remaining bridge-only assumptions and prove final compliance.

Work:
- remove obsolete bridge fallbacks if safe
- update closeout docs
- run final audits
- fix any residual contract/runtime mismatch

Done when:
- root runtime behavior is compliant with `v2.6.0`
- full suite is green
- compliance tests validate the `v2.6.0` package/flow/handler model directly
- no active runtime path requires old flat contracts or bridge-only semantics to function

## Acceptance Gates

Run throughout the migration:

```bash
go test ./internal/runtime/contracts -count=1
go test ./internal/runtime -run TestContractCompliance -count=1
go test ./internal/runtime/pipeline -count=1
go test ./internal/runtime -count=1
go test ./... -count=1
```

## Definition Of Done

`v2.6.0` is fully adopted only when all of the following are true:

- runtime loads the package/flow contract model
- target flow files are the active source, not just `empire/runtime/*`
- routing derives from nodes + agents rather than legacy event-catalog routing metadata
- handler execution follows the normative `v2.6.0` order
- compatibility bridges are removed or explicitly non-authoritative
- contract compliance and full suite remain green
