# Flow-Scoped Entities Implementation Plan

## Goal

Move runtime state ownership from a shared cross-flow entity row to flow-local
entities linked by a platform-owned `subject_id`.

## Principles

- One entity row belongs to one flow.
- `current_state` is singular within the owning flow.
- Cross-flow lineage uses `subject_id`.
- Cross-flow data moves by event handoff or read-only lookup, not shared writes.

## Implementation Order

1. Add `subject_id` to runtime persistence and entity/tool views.
   - Persist `entity_state.subject_id`.
   - Load and expose `subject_id` from entity tools and workflow store.
   - Propagate `subject_id` on `create_entity`.

2. Enforce flow boundary creation semantics.
   - Boot validation: stateful input-pin handlers must declare `create_entity: true`.
   - Reject invalid contracts at boot for platform `>= 1.6.0`.

3. Prohibit cross-flow writes.
   - `save_entity_field` and other entity mutation paths must reject writes when the
     caller flow does not own the target entity row.

4. Add subject-level operator/query support.
   - Implement `get_subject_status`.
   - Add subject-aware entity lookup paths used by dashboards and status views.

5. Remove shared-row state workarounds.
   - Stop writing `flow_states`.
   - Remove effective-state fallback logic that derives state from `flow_states`.

6. Add conformance coverage.
   - Subject propagation across `create_entity` handoffs.
   - Boot rejection for stateful input-pin handlers without `create_entity: true`.
   - Cross-flow write rejection.
   - Subject-level status aggregation.

## Current Slice

This tranche covers:

- `subject_id` persistence support
- `subject_id` propagation on `create_entity`
- initial runtime/tool exposure
