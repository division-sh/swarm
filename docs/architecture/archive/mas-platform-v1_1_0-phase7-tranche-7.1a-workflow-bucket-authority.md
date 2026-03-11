# Phase 7 Tranche 7.1A: Workflow Bucket Authority Freeze

Date: 2026-03-11
Author: Codex implementer

## Purpose

Turn the Phase 7 migration matrix into the first executable storage-authority tranche without taking a schema migration dependency.

This tranche exists to answer one question before any broader datastore work starts:

- which `workflow_instances.accumulator_state` buckets are canonical platform state
- which are canonical product state
- which are compatibility-only and must stop acting like silent authority

## Why This Is The Earliest Safe Slice

This slice is safe because it freezes ownership before it changes persistence shape.

It does not require:

- new tables
- removed tables
- renamed columns
- changed persisted bucket keys

It does require:

- one explicit ownership map
- one shared read/write boundary for live bucket access
- one set of tests that fail when unknown or compatibility-only buckets are treated as canonical

## Tranche Scope

In scope:

- workflow bucket classification
- workflow bucket read/write authority
- workflow-state loader exposure
- projection-path and transition-path alignment to the same ownership map
- documentation of the row-level workflow-state boundary

Out of scope:

- `verticals` reconciliation
- table DDL churn outside comment-only clarification
- dedicated product-state table removal
- generated persistence surfaces
- compliance/materialization gating

## Canonical Ownership Freeze

### Platform-Owned Canonical

- `validation-orchestrator`
- `entity_projection`

### Product-Derived Canonical

- `scoring-node`
- `discovery-aggregator`
- `build-orchestrator`

### Compatibility-Only

- `scoring-restore`
- `scan-state`
- `pending-dedup`

## File-Level Implementation Order

### 1. Platform contract vocabulary

File:

- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

Required outcome:

- workflow-state vocabulary and platform-owned workflow fields are frozen in one place

Why first:

- later code changes need a stable ownership vocabulary before they can safely enforce anything

### 2. Runtime contract loader boundary

File:

- `internal/runtime/contracts/workflow_contracts.go`

Required outcome:

- one loaded view exposes workflow-state field ownership and bucket ownership

Why second:

- projection code and store code must not each rediscover ownership independently

### 3. Workflow projection boundary

File:

- `internal/runtime/pipeline/workflow_instance_projection.go`

Required outcome:

- raw bucket string literals are replaced with named constants
- bucket writes/reads go through a single ownership helper

Why third:

- this is the widest live write/read surface for workflow buckets

### 4. Coordinator workflow projection writes

File:

- `internal/runtime/pipeline/coordinator_workflow_projection.go`

Required outcome:

- stage-projection writes align to the frozen ownership classes
- compatibility buckets stop acting like open-ended side channels

### 5. Transition-engine accumulator mutations

File:

- `internal/runtime/pipeline/workflow_transition_engine.go`

Required outcome:

- direct accumulator mutations share the same ownership boundary as projection writes

### 6. Workflow instance store commentary and row contract

File:

- `internal/runtime/pipeline/workflow_instance_store.go`

Required outcome:

- row-level workflow ownership is explicit
- the store no longer reads as if `accumulator_state` were an unrestricted product dump field

### 7. Deferred schema follow-up

File:

- `contracts/ddl-canonical.sql`

Required outcome:

- only comment-level or clearly staged follow-up changes in this tranche

Why last:

- schema should not move until runtime semantics already enforce the ownership model

## Tranche Invariants

These must remain true for every patch in 7.1A:

1. Persisted bucket keys do not change.
2. Existing rows remain readable without backfill.
3. Unknown bucket writes fail fast in tests or explicit guards; they do not silently become canonical.
4. Compatibility buckets may be read for restore/reporting, but no new logic should promote them to source-of-truth.
5. No patch in this tranche should require SQL rollback to restore runtime correctness.
6. If a write site cannot be cleanly classified, the tranche stops and records the unresolved site.

## Stop Conditions

Stop the tranche and re-plan if any of the following is discovered:

- a live bucket must change persisted key name to be classifiable
- a dedicated table and a workflow bucket are both still required as concurrent authoritative writers for the same semantic field
- the platform spec cannot represent the workflow-owned field boundary cleanly
- the loader boundary would need product-specific branching to expose ownership

## Rollback Plan

Rollback target:

- pre-enforcement behavior with identical persisted rows and identical bucket keys

Rollback method:

1. Remove the ownership helper enforcement but keep any harmless bucket constants if already adopted broadly.
2. Revert projection-path and transition-path routing to the previous direct bucket access sites.
3. Keep any non-semantic test fixtures only if they still reflect live bucket names.

Why rollback is safe here:

- this tranche should not alter schema shape
- this tranche should not require data migration
- this tranche should not rename persisted buckets

## Acceptance Evidence

The tranche is complete only when all of the following are true:

- there is one documented ownership map for all live workflow buckets
- the active projection path and transition-engine path both route through it
- compatibility buckets are explicitly marked non-canonical
- `workflow_instance_store.go` documents the limited workflow-owned boundary rather than implying open-ended ownership
- later slices can point to this note instead of redefining bucket ownership

## Recommended Next Implementation Order After 7.1A

1. Finish the remaining `7.1` workflow-state schema authority follow-up while the ownership freeze is still fresh.
2. Move to `7.2` and reconcile `verticals` against MAS `entity_schema`.
3. Move to `7.3` and collapse duplicated product state between dedicated tables and workflow buckets.
4. Only then take on `7.4` unresolved platform/product table cleanup.
5. Leave generated persistence surfaces and compliance gating for `7.5` and `7.6`.

Reason:

- `7.1A` establishes the boundary that the later table and materialization slices need in order to avoid creating a third authority path by accident.
