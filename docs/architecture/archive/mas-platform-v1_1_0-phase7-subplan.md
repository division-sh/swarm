# MAS Platform v1.1.0 Phase 7 Subplan

Date: 2026-03-11
Author: Codex implementer

## Phase 7 Goal

Replace the current split datastore authority with a singular platform/contract-derived persistence model.

By the end of Phase 7:

- platform-owned tables must have clear ownership from MAS platform contracts
- product-derived tables and state must be generated or materialized from MAS product YAML, not hand-maintained as silent parallel truth
- legacy tables and compatibility buckets must be either removed or explicitly quarantined
- compliance must validate derived persistence, not bless handwritten SQL as canonical

This is the phase that closes the largest remaining structural gap to “no exception” platformization.

## Starting Point

- runtime semantics are now largely MAS-driven
- persistence authority is still split across:
  - `docs/specs/mas-platform`
  - `contracts/ddl-canonical.sql`
  - handwritten store/runtime code
- several product-state buckets still exist both in dedicated tables and in `workflow_instances.accumulator_state`

## Persistence Classification

The current datastore falls into four buckets:

- platform-owned
  - `events`
  - `event_deliveries`
  - `event_receipts`
  - `schedules`
  - `mailbox`
  - `human_tasks`
  - `conversations`
  - `agent_sessions`
  - `agent_turns`
  - runtime idempotency/recovery tables such as `pipeline_receipts`, `pipeline_processed_events`, `system_node_ledger`
  - `runtime_config`
  - `runtime_log`
- product-derived from MAS YAML
  - `verticals`
  - `scan_accumulators`
  - `pending_dedup_candidates`
  - `validation_pipelines`
  - active workflow-instance JSON buckets used as product state:
    - `scoring-node`
    - `build-orchestrator`
    - `validation-orchestrator`
- legacy/remove
  - `routing_rules`
  - `org_templates`
  - `template_migrations`
  - `bootstrap_versions`
  - `prompt_overrides`
  - `template_prompt_drafts`
  - compatibility JSON buckets such as `scoring-restore`, `scan-state`, and `pending-dedup`
- resolved from follow-up audit
  - `agents` -> platform-owned
  - `scan_campaigns` -> product-derived
  - `geographies` -> product-derived
  - `inbound_events` -> platform-owned
  - `scoring_digest_buffer` -> legacy/remove

That classification is the working input for the slices below.

## Phase 7 Implementation Stance

Phase 7 should not try to rewrite all persistence paths at once.

It should proceed in four controlled moves:

1. freeze ownership and source-of-truth per table/state bucket
2. remove multi-owner state, especially where dedicated tables and `workflow_instances.accumulator_state` both hold the same semantics
3. move product-derived schemas behind MAS YAML derivation boundaries
4. only then change compliance and generation so the derived model becomes enforceable

The practical rule for implementation is:

- platform-owned tables may stay handwritten, but their ownership must come from MAS platform semantics
- product-derived tables/state must stop being silent handwritten truth and become generated/materialized outputs of product YAML
- legacy/remove tables must not stay on the critical path
- any remaining unresolved persistence surfaces must be explicitly decided before Phase 7 is considered done

## Phase 7 Migration Matrix

| Table / state bucket | Class | Current authority | Target authority | Planned tranche | Notes |
| --- | --- | --- | --- | --- | --- |
| `workflow_instances` | platform-owned | `ddl-canonical.sql` + `workflow_instance_store.go` + platform spec | platform contract materialization | 7.1 | Keep as platform state table, but stop using it as product-state spillover. |
| `workflow_instances.accumulator_state["validation-orchestrator"]` | product-derived | workflow projection code + validation table | generated product state projection or single canonical node-state materialization | 7.1 -> 7.3 | Remove dual truth with `validation_pipelines`. |
| `workflow_instances.accumulator_state["scoring-node"]` | product-derived | workflow projection code only | generated product node-state materialization | 7.3 | Currently the only live scoring accumulator store. |
| `workflow_instances.accumulator_state["build-orchestrator"]` | product-derived | `record_evidence` path only | generated product node-state materialization | 7.3 | Build flow has YAML `state_schema` but no dedicated table. |
| `workflow_instances.accumulator_state["scoring-restore"]` | legacy/remove | compatibility logic | remove after authoritative scoring state lands | 7.3 | Compatibility bucket only. |
| `workflow_instances.accumulator_state["scan-state"]` | legacy/remove | compatibility logic | remove after discovery state ownership is singular | 7.3 | Mirror of `scan_accumulators`. |
| `workflow_instances.accumulator_state["pending-dedup"]` | legacy/remove | compatibility logic | remove after discovery state ownership is singular | 7.3 | Mirror of `pending_dedup_candidates`. |
| `verticals` | product-derived | handwritten DDL + direct store/runtime access | derived from `package.yaml -> entity_schema` | 7.2 | Largest schema drift point in the repo. |
| `scan_accumulators` | product-derived | handwritten DDL + `PipelineStateStore` | generated from discovery `state_schema` | 7.3 | Current table shape does not match YAML shape. |
| `pending_dedup_candidates` | product-derived | handwritten DDL + `PipelineStateStore` | generated from discovery `state_schema` | 7.3 | Same issue as `scan_accumulators`. |
| `validation_pipelines` | product-derived | handwritten DDL + `PipelineStateStore` | generated from validation `state_schema` | 7.3 | Also duplicates `validation-orchestrator` workflow bucket. |
| `scoring_digest_buffer` | legacy/remove | handwritten DDL + pipeline reset path | remove or replace with contract-driven scoring state/history | 7.3 | Bridge-era scoring support table with narrow callers. |
| `events` | platform-owned | handwritten DDL + `internal/store/events.go` | platform-owned runtime storage | 7.4 | Stable; keep separate from product-derived work. |
| `event_deliveries` | platform-owned | handwritten DDL + event bus persistence | platform-owned runtime storage | 7.4 | Stable. |
| `event_receipts` | platform-owned | handwritten DDL + `event_receipt_store.go` | platform-owned runtime storage | 7.4 | Stable. |
| `pipeline_receipts` | platform-owned | handwritten DDL + `events.go` | platform-owned recovery/idempotency | 7.4 | Stable but should stay out of product derivation. |
| `pipeline_processed_events` | platform-owned | handwritten DDL + `PipelineStateStore` | platform-owned idempotency | 7.4 | Needs separation from Empire-specific bulk state store. |
| `system_node_ledger` | platform-owned | handwritten DDL + runtime idempotency | platform-owned idempotency | 7.4 | Stable. |
| `schedules` | platform-owned | handwritten DDL + `schedule_store.go` | platform-owned timer storage | 7.4 | Must promote exact identity out of payload JSON. |
| `mailbox` | platform-owned | handwritten DDL + `mailbox.go` | platform-owned HITL storage | 7.4 | Stable. |
| `human_tasks` | platform-owned | handwritten DDL | platform-owned HITL storage | 7.4 | Stable. |
| `conversations` | platform-owned | handwritten DDL + `llm_store.go` | platform-owned runtime storage | 7.4 | Stable. |
| `agent_sessions` | platform-owned | handwritten DDL + `llm_store.go` | platform-owned runtime storage | 7.4 | Stable. |
| `agent_turns` | platform-owned | handwritten DDL + `llm_store.go` | platform-owned runtime storage | 7.4 | Stable. |
| `runtime_config` | platform-owned | handwritten DDL | platform-owned runtime storage | 7.4 | Stable. |
| `runtime_log` | platform-owned | handwritten DDL | platform-owned runtime storage | 7.4 | Stable. |
| `routing_rules` | legacy/remove | handwritten DDL + `template_routing_store.go` | remove from runtime critical path; derive routing from contracts | 7.4 | MAS routing model does not want this as authority. |
| `org_templates` | legacy/remove | handwritten DDL + `template_routing_store.go` | remove or quarantine | 7.4 | Legacy bootstrap/template system. |
| `template_migrations` | legacy/remove | handwritten DDL | remove or quarantine | 7.4 | Legacy bootstrap/template system. |
| `bootstrap_versions` | legacy/remove | handwritten DDL + `template_routing_store.go` | remove or quarantine | 7.4 | Legacy bootstrap baseline. |
| `prompt_overrides` | legacy/remove | handwritten DDL + `prompt_overrides.go` | remove or quarantine | 7.4 | Not part of MAS persistence ownership. |
| `template_prompt_drafts` | legacy/remove | handwritten DDL | remove or quarantine | 7.4 | Same issue as prompt overrides. |
| `agents` | platform-owned | handwritten DDL + `agent_store.go` | platform-owned runtime registry aligned to generic flow/entity/path semantics | 7.4 | Ownership is platform, but schema still leaks `vertical_id` and Empire-shaped assumptions. |
| `scan_campaigns` | product-derived | handwritten DDL + `scan_campaigns.go` | product-derived storage behind Empire contract authority or explicit product-owned persistence | 7.4 | Active business-state table with no MAS schema authority yet. |
| `geographies` | product-derived | handwritten DDL + `scan_campaigns.go` | product-derived reference/config storage behind Empire contract authority | 7.4 | Product scope/reference data, not generic platform state. |
| `inbound_events` | platform-owned | handwritten DDL + `inbound.go` | platform-owned ingress dedup/retention store with generic target resolution boundaries | 7.4 | Table ownership is platform, but callers still resolve Empire-specific targets. |

## Table-By-Table Sequencing

### Tranche 7.1: Workflow-State Authority First

This tranche should touch only the platform workflow-state boundary and its obvious product-state leakage.

- normalize `workflow_instances` ownership against the platform spec
- define which `accumulator_state` buckets are canonical versus temporary projections
- stop adding new product state into compatibility buckets before Phase 7 finishes

Tables/state buckets in scope:

- `workflow_instances`
- `workflow_instances.accumulator_state["validation-orchestrator"]`
- `workflow_instances.accumulator_state["scoring-node"]`
- `workflow_instances.accumulator_state["build-orchestrator"]`
- compatibility buckets:
  - `scoring-restore`
  - `scan-state`
  - `pending-dedup`

Exit condition:

- every bucket in `workflow_instances` is tagged as either platform-owned canonical state, product-derived canonical state, or compatibility-only

Current tranche status:

- shared bucket ownership constants are now live in the workflow projection path
- safe raw bucket reads/writes in the active projection and transition-engine paths now use those constants instead of scattered string literals
- `payload_factory.go` now consumes the same canonical bucket access path for validation/entity projection hydration instead of reading raw accumulator keys directly
- workflow-state mutation in the active projection/transition path now clones and writes through shared bucket accessors rather than mutating raw accumulator maps in place
- the next remaining raw bucket sites are now smaller follow-up cleanup work rather than core active-path scatter

### Tranche 7.2: Entity Schema Reconciliation

This tranche should isolate the `verticals` drift problem before touching broader state generation.

- produce a column-by-column diff between `verticals` and `package.yaml -> entity_schema`
- classify every extra column as:
  - promote into product entity schema
  - move into node `state_schema`
  - remove as compatibility/legacy data
- define the generated entity DDL target shape

Tables in scope:

- `verticals`

Exit condition:

- `verticals` no longer behaves as an unbounded handwritten product schema

### Tranche 7.3: Product State Materialization

This tranche should replace the bespoke Empire state layer with explicit product-derived state ownership.

- reconcile each handwritten node-state table to its owning `state_schema`
- choose one canonical storage path per node state
- remove semantic duplication between dedicated tables and workflow-instance JSON buckets
- decide the fate of `scoring_digest_buffer`

Tables/state buckets in scope:

- `scan_accumulators`
- `pending_dedup_candidates`
- `validation_pipelines`
- `workflow_instances.accumulator_state["scoring-node"]`
- `workflow_instances.accumulator_state["build-orchestrator"]`
- `workflow_instances.accumulator_state["validation-orchestrator"]`
- `scoring_digest_buffer`
- compatibility buckets scheduled for removal

Exit condition:

- each product node state has one canonical owner

### Tranche 7.4: Platform Persistence Cleanup And Legacy Quarantine

This tranche should clean up surrounding platform tables and remove legacy runtime authority that MAS no longer wants.

- keep stable platform tables intact but explicitly out of product derivation
- remove routing/template/bootstrap/prompt stores from critical-path datastore authority
- finish redesigning the surrounding platform tables whose ownership is now known but whose schema/callers still leak product assumptions
- make timer identity schema-level

Tables in scope:

- `events`
- `event_deliveries`
- `event_receipts`
- `pipeline_receipts`
- `pipeline_processed_events`
- `system_node_ledger`
- `schedules`
- `mailbox`
- `human_tasks`
- `conversations`
- `agent_sessions`
- `agent_turns`
- `runtime_config`
- `runtime_log`
- `routing_rules`
- `org_templates`
- `template_migrations`
- `bootstrap_versions`
- `prompt_overrides`
- `template_prompt_drafts`
- `agents`
- `scan_campaigns`
- `geographies`
- `inbound_events`
- `scoring_digest_buffer`

Exit condition:

- no legacy table remains implicit runtime authority
- every previously unresolved table has an explicit class, owner, and migration order

### Tranche 7.5: Generated Persistence Surfaces

This tranche should wire runtime-facing persistence to derived contracts instead of ad hoc SQL assumptions.

- implement or solidify contract-derived entity/state access surfaces
- stop treating handwritten SQL helpers as the future generic API

### Tranche 7.6: Compliance And Materialization Gates

This tranche should make the new ownership model executable and enforceable.

- verify platform-owned tables against platform expectations
- verify product-derived tables/state against MAS product YAML
- demote `ddl-canonical.sql` from normative authority to generated/materialized artifact or checked output

## Highest-Risk Migration Blockers

### 1. `verticals` and `entity_schema` are materially out of sync

The product entity table is still a large handwritten schema rather than a derived output of `package.yaml -> entity_schema`.

Primary files:

- `contracts/ddl-canonical.sql`
- `docs/specs/mas-platform/empire/contracts/package.yaml`
- `internal/store/verticals.go`

Why this blocks Phase 7:

- generated entity persistence cannot start while the canonical table shape is open-ended and handwritten

### 2. Product state has multiple active sources of truth

Discovery and validation state live both in dedicated tables and in `workflow_instances.accumulator_state`. Scoring and build state live only in workflow JSON buckets.

Primary files:

- `internal/runtime/pipeline/state_store.go`
- `internal/runtime/pipeline/workflow_instance_projection.go`
- `internal/runtime/pipeline/coordinator_workflow_projection.go`
- `internal/runtime/pipeline/workflow_transition_engine.go`

Why this blocks Phase 7:

- no generator or materializer can be authoritative while runtime writes the same semantics into more than one place

### 3. `PipelineStateStore` is Empire-specific and destructive

The current persistence path hardcodes product tables and uses table-wide delete/reinsert behavior.

Primary files:

- `internal/runtime/pipeline/state_store.go`
- `internal/runtime/pipeline/coordinator_state.go`

Why this blocks Phase 7:

- this is not a generic platform persistence model and cannot be cleanly lifted behind MAS derivation

### 4. Timer identity is still hidden in payload JSON

Exact schedule identity still depends on `payload.__schedule_task_id`.

Primary files:

- `contracts/ddl-canonical.sql`
- `internal/store/schedule_store.go`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

Why this blocks Phase 7:

- platform-owned timer persistence is not fully schema-level while identity survives as a payload convention

### 5. Legacy routing/template/bootstrap tables still act like live authority

The MAS model wants routing derived from contracts, but legacy stores remain active.

Primary files:

- `contracts/ddl-canonical.sql`
- `internal/store/template_routing_store.go`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

Why this blocks Phase 7:

- as long as these remain in the critical path, persistence authority is still split

### 6. Runtime contracts still point to merged runtime artifacts instead of the target per-flow contract set

The product contract entry point still describes `runtime_contracts` as active and `target_contracts` as future.

Primary files:

- `docs/specs/mas-platform/empire/contracts/package.yaml`

Why this blocks Phase 7:

- product-derived persistence generation cannot become authoritative while the contract loading model is still transitional

## First Concrete Implementation Tranche

The first concrete implementation tranche should be:

### 7.1A: Workflow Bucket Authority Freeze

This is the earliest safe storage-authority slice because it changes ownership semantics before it changes schema shape.

Scope:

- freeze the authoritative `workflow_instances` contract boundary
- classify every live `workflow_instances.accumulator_state` bucket as `platform`, `product`, or `compatibility`
- stop allowing new mixed-ownership bucket writes to appear during later Phase 7 work

Why this tranche goes first:

- it is the smallest slice that materially reduces split storage authority without forcing immediate DDL churn
- it gives Tranche 7.2 and Tranche 7.3 a stable answer to “which state is canonical versus transitional”
- it creates a reversible checkpoint before touching `verticals`, product state tables, or generated persistence surfaces

Authoritative design note:

- [mas-platform-v1_1_0-phase7-tranche-7.1a-workflow-bucket-authority.md](/Users/youmew/dev/empireai/docs/architecture/mas-platform-v1_1_0-phase7-tranche-7.1a-workflow-bucket-authority.md)

Implementation invariants for 7.1A:

- no persisted bucket key changes in this tranche
- no table or column additions/removals in this tranche
- no new write path may bypass the bucket ownership map once introduced
- compatibility buckets remain readable for restore/reporting, but must not gain new semantic ownership
- if a bucket classification is ambiguous, the tranche stops and records the ambiguity rather than silently choosing an owner

File-level implementation order for 7.1A:

1. `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`
   Freeze the platform-owned workflow-state vocabulary first.
2. `internal/runtime/contracts/workflow_contracts.go`
   Expose a single loaded view of workflow bucket ownership and workflow-state field ownership.
3. `internal/runtime/pipeline/workflow_instance_projection.go`
   Replace raw bucket literals with named constants and ownership-aware helpers.
4. `internal/runtime/pipeline/coordinator_workflow_projection.go`
   Route projection writes through the same ownership helper and block new mixed-ownership writes.
5. `internal/runtime/pipeline/workflow_transition_engine.go`
   Route direct accumulator mutations through the ownership helper.
6. `internal/runtime/pipeline/workflow_instance_store.go`
   Narrow the row contract commentary and write surface to the frozen workflow-owned fields only.
7. `contracts/ddl-canonical.sql`
   Only after runtime write paths are constrained, document or apply any row-shape follow-up needed for later tranches.

Live bucket ownership freeze for 7.1A:

- platform-owned canonical:
  - `validation-orchestrator`
  - `entity_projection`
- product-derived canonical:
  - `scoring-node`
  - `discovery-aggregator`
  - `build-orchestrator`
- compatibility-only:
  - `scoring-restore`
  - `scan-state`
  - `pending-dedup`

Earliest safe patch shape:

- add one bucket-ownership map and one small helper layer at the workflow projection boundary
- switch existing read/write sites to those helpers without changing stored data shape
- add assertions/tests that fail when unknown buckets are written or when compatibility buckets are treated as canonical

Why this is the first safe patch:

- it has no required migration step
- it does not invalidate existing rows
- rollback is file-local because persisted keys and table shapes stay unchanged
- it turns later schema and materialization work into follow-on changes instead of concurrent guesswork

## Resolved Table Migration Order

Once Phase 7 moves beyond `7.1A`, the recommended order for the formerly unresolved tables is:

1. `scoring_digest_buffer`
2. `inbound_events`
3. `agents`
4. `geographies`
5. `scan_campaigns`

Reason:

- `scoring_digest_buffer` is the clearest removal target and has the narrowest active surface
- `inbound_events` is platform-owned with a relatively small caller boundary
- `agents` is platform-owned but central, so it should follow the smaller platform table cleanup
- `geographies` should be classified before `scan_campaigns` because campaign state depends on it
- `scan_campaigns` should move last among this set because it is the largest remaining handwritten Empire business-state table

## Slice 7.1: Workflow-State Schema Authority

### Objective

Make workflow-state persistence a platform-owned schema derived from MAS platform contracts.

### Target files

- `contracts/ddl-canonical.sql`
- `internal/runtime/pipeline/workflow_instance_store.go`
- `internal/runtime/contracts/workflow_contracts.go`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

### Work

- align workflow-state naming with MAS platform contracts
- stop using `workflow_instances` as a mixed platform-plus-product compatibility bucket
- separate platform workflow state from product projections and compatibility state

### Acceptance

- workflow-state schema is platform-owned and contract-derived
- `workflow_instances.accumulator_state` is no longer a generic dumping ground

## Slice 7.2: Entity Table Platformization

### Objective

Reconcile the handwritten product entity table with MAS `entity_schema`.

### Target files

- `contracts/ddl-canonical.sql`
- `docs/specs/mas-platform/empire/contracts/package.yaml`
- `internal/store/verticals.go`
- `internal/runtime/inbound.go`

### Work

- inventory `verticals` columns that do not come from `package.yaml -> entity_schema`
- decide whether each extra field becomes:
  - generated product entity field
  - flow state
  - removable compatibility data
- remove direct generic-runtime dependence on handwritten `verticals` columns

### Acceptance

- entity persistence is product-derived from MAS YAML, not manual SQL drift
- generic runtime does not rely on Empire entity columns as primitive truth

## Slice 7.3: Runtime Business-State Tables

### Objective

Platformize or remove the handwritten product workflow-state tables.

### Target files

- `internal/runtime/pipeline/state_store.go`
- `contracts/ddl-canonical.sql`
- `docs/specs/mas-platform/empire/contracts/flows/*/nodes.yaml`

### Work

- inventory and normalize:
  - `scan_accumulators`
  - `pending_dedup_candidates`
  - `validation_pipelines`
  - `scoring_digest_buffer`
- compare each table to the corresponding MAS `state_schema`
- eliminate duplicated state across dedicated tables and workflow-instance JSON buckets

### Acceptance

- product workflow state has one owner per state bucket
- dedicated tables and workflow-instance JSON no longer duplicate the same state semantically

## Slice 7.4: Schedule, Agent, Inbound, And Workspace Persistence

### Objective

Remove product vocabulary and payload-level identity hacks from the surrounding persistence model.

### Target files

- `internal/store/schedule_store.go`
- `internal/store/agent_store.go`
- `internal/store/inbound.go`
- `internal/runtime/workspace/manager.go`
- `internal/runtime/inbound.go`

### Work

- make exact schedule identity schema-level instead of relying on `payload.__schedule_task_id`
- redesign inbound/agent/workspace persistence around generic scope/entity/instance vocabulary
- classify unresolved tables:
  - `agents`
  - `scan_campaigns`
  - `geographies`
  - `inbound_events`

### Acceptance

- schedule identity is first-class in storage
- generic persistence layers no longer depend on `vertical` as a primitive storage scope

## Slice 7.5: Generated Persistence Tools

### Objective

Replace ad hoc SQL access with contract-derived persistence tools where MAS expects them.

### Target files

- `internal/runtime/tools/executor_sql.go`
- `internal/runtime/tools/executor_external.go`
- `internal/runtime/contracts/workflow_contracts.go`

### Work

- implement contract-derived entity read/write/search/query surfaces if they remain part of the MAS platform
- stop treating raw SQL helpers as the long-term generic platform API

### Acceptance

- persistence-facing runtime tools align with MAS contract surfaces
- handwritten SQL execution is no longer the default generic API

## Slice 7.6: Compliance Refactor For Derived Schema

### Objective

Make derived persistence executable and verifiable.

### Target files

- `internal/runtime/contract_compliance_test.go`
- `contracts/verification-gates.yaml`
- `contracts/ddl-canonical.sql`

### Work

- replace compliance that blesses handwritten SQL with compliance that verifies derived schema/materialization
- make contract-to-DDL or contract-to-store generation executable
- ensure platform-owned and product-derived schemas are both validated against runtime expectations

### Acceptance

- compliance checks derived schema, not manual SQL drift
- `ddl-canonical.sql` is no longer the primary authority

## Recommended Order

1. Tranche `7.1A` workflow bucket authority freeze
2. Remaining Tranche `7.1` workflow-state schema authority follow-up
3. Tranche `7.2` entity schema reconciliation
4. Tranche `7.3` product state materialization and dedup
5. Tranche `7.4` platform persistence cleanup and unresolved table decisions
6. Tranche `7.5` generated persistence surfaces
7. Tranche `7.6` compliance and materialization gates

Reason:

- the bucket authority freeze is the first reversible checkpoint and should land before any schema mutation or table authority move
- workflow-state ownership has to be cleaned up before product-derived state generation can converge
- entity and business-state tables must be classified before generated tools and compliance can be correct
- compliance refactor belongs at the end of the phase because it should validate the new derived model, not the old split model

## Exit Gate

Phase 7 is complete only if:

- persistence authority is singular and derivable
- platform-owned versus product-derived tables are explicit and enforced
- legacy tables and compatibility buckets are either removed or isolated
- compliance validates the derived persistence model instead of the handwritten SQL baseline
