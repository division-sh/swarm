# Phase 5 Closeout: Replace Flat Workflow Assumptions

## Status

Phase 5 is complete.

The runtime no longer depends on raw flat `workflow-schema.yaml` / `guard-action-registry.yaml`
shapes as its primary semantic access path. Production consumers now use semantic bundle APIs,
and those semantics are no longer sourced only from the flat workflow documents.

## What Landed

### Semantic bundle accessors

`WorkflowContractBundle` now exposes semantic accessors instead of forcing runtime/compliance
code to read:

- `bundle.Workflow.Workflow.*`
- `bundle.Hooks.*`

The production runtime now consumes:

- `WorkflowName()`
- `WorkflowVersion()`
- `WorkflowEntitySchema()`
- `WorkflowStages()`
- `WorkflowTerminalStages()`
- `WorkflowTransitions()`
- `WorkflowInitialStage()`
- `WorkflowTimers()`
- `GuardEntries()`
- `ActionEntries()`
- `GuardEntryByID()`
- `ActionEntryByID()`

### Flow-derived semantics

The semantic layer now also derives and exposes:

- `FlowInitialStage(flowID)`
- `FlowStates(flowID)`
- `FlowTerminalStages(flowID)`
- `FlowNamespace(flowID)`

And uses flow schemas as fallback for:

- workflow stages
- workflow terminal stages

when those are absent from the flat workflow doc.

### Handler-derived semantics

The semantic layer now preserves handler-first node semantics from `nodes.yaml`:

- `advances_to`
- `sets_gate`
- `data_accumulation`
- `condition`
- `guard`
- wildcard event handler ownership

Bundle APIs added:

- `NodeEventHandlers(nodeID)`
- `NodeEventHandler(nodeID, eventType)`
- `RuntimeEventOwners(eventType)`

### Real consumer rebases

The new semantic layer is now used by real consumers:

- workflow-node policy assembly
- workflow contract validation

This proves the semantic layer is not just metadata; it already drives runtime/compliance
behavior safely.

## What Did Not Change Yet

- The transition engine is still transition-first.
- Flat workflow/hook documents still back the semantic layer in several places.
- Handler-derived semantics are not yet the authoritative execution model.

That is expected. It is Phase 6+ work.

## Exit Criteria Met

- No active production runtime/compliance path reads raw flat workflow/hook structs directly as
  its primary semantic API.
- Semantic access now comes through bundle adapters.
- Flow metadata contributes real semantics.
- Handler metadata contributes real semantics.
- Full suite is green.

## Verification

```bash
go test ./... -count=1
```
