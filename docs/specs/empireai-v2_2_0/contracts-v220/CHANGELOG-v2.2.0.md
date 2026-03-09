# EmpireAI v2.2.0 — System Node Decomposition + Full Interceptor Elimination

**Version:** 2.2.0
**Previous:** 2.1.0
**Date:** 2026-03-07

## Summary

Replaces the monolithic pipeline-coordinator interceptor with 5 focused system nodes. Every runtime-intercepted event now has an explicit owning system node with fully encoded decision logic. The interceptor override table is eliminated — all event routing and processing logic is contract-driven.

## Breaking Change

`pipeline-coordinator` replaced by:

| Node | Responsibility | Subs | Produces | Transitions | Handlers |
|------|---------------|------|----------|-------------|----------|
| `scoring-node` | Scoring, rubric, derivation | 4 | 8 | 4 | 4 |
| `scan-orchestrator` | Scan campaigns, dispatch, timeout | 10 | 2 | 0 | 4 |
| `discovery-aggregator` | Signal accumulation, dedup | 5 | 3 | 0 | 5 |
| `validation-orchestrator` | 4-gate validation pipeline | 14 | 10 | 9 | 9 |
| `lifecycle-orchestrator` | Lifecycle, OpCo handoff, timers | 15 | 3 | 1 | 8 |

## Event Catalog Enhancements

- `runtime_handling on 56 events: consuming, dual_delivery, passthrough (4), other (5)
- `owning_node` on 53 events: maps each to its processing system node
- Zero intercepted events without an owning node
- Zero events in node subscribes_to without runtime_handling declared

## System Node Event Handlers (30 total)

Each handler encodes the complete decision rule in YAML:
- **validation-orchestrator (9):** gate→advance→emit rules. e.g. `research.completed → set g1, advance to mvp_speccing`
- **lifecycle-orchestrator (8):** event→action mappings with guards. e.g. `vertical.approved → emit opco.spinup_requested`
- **discovery-aggregator (5):** threshold check → dedup check → emit. With on_pass/on_dedup/on_below branches.
- **scoring-node (4):** accumulation with composite formula (tier weights 60/30/10), 3-way classification
- **scan-orchestrator (4):** mode_to_scanners dispatch, completion accumulation, timeout handling

## Spec Changes

- §5.8 rewritten: 5-node table with handler counts, routing policy description
- Workflow-schema: all transition nodes remapped from pipeline-coordinator to new nodes
- Timer owners updated to new node names

## Touches

- system-nodes.yaml: REWRITTEN — 5 nodes with event_handlers, replaced pipeline-coordinator
- event-catalog.yaml: runtime_handling on 56 events
- workflow-schema.yaml: transition nodes remapped, timer owners updated
- upgrade-actions.yaml: 3 v2.2.0 actions added, previous_version → 2.1.0
- verification-gates.yaml: version-field-consistency updated
- tooling.lock: → 2.2.0
- empireai-v2_2_0.md: §5.8 rewritten

## What This Enables

The runtime can derive ALL event routing from contracts:
1. Read `owning_node` + `runtime_handling` from event-catalog.yaml
2. `consuming` → route to owning node only
3. `dual_delivery` → route to owning node AND agent subscribers
4. `passthrough` → route to agent subscribers only
5. No owning_node → pure agent delivery

No hardcoded override table. The interceptor switch/case in Go can be replaced by contract-driven dispatch.

## Migration Path

1. Create 4 Go files: scan_orchestrator.go, discovery_aggregator.go, validation_orchestrator.go, lifecycle_orchestrator.go
2. Move logic from coordinator.go using event_handlers as the specification
3. Remove interceptor middleware — replace with node-based dispatch from catalog
4. Run existing tests + add per-node contract compliance tests

## Entity Schema + Node State Schemas (v2.2.0 amendment)

### workflow-schema.yaml: entity_schema

The workflow now declares the full entity table schema in 8 field groups:
- identity (id, slug, name, geography)
- workflow_state (stage, mode, parked_at, kill_reason)
- discovery_phase (raw_signals, signal_strength, opportunity_pattern, discovery_mode)
- scoring_phase (composite_score, scoring_rubric, scores JSONB)
- validation_phase (business_brief, mvp_spec, cto_feasibility, brand, validation_kit — all JSONB)
- operating_phase (full_spec, deploy_config, live_url, launch_targets, credentials)
- metadata (human_notes, template_version, timestamps)
- derivation (parent_id, generation_depth, generator_agent_id)

Each field specifies its type and which event populates it.

### workflow-schema.yaml: data_accumulation on transitions

7 transitions now declare what entity fields they write and which event provides the data:
- discovered_to_scoring → name, geography, raw_signals, signal_strength, discovery_mode
- scoring_to_shortlisted → composite_score, scoring_rubric, scores
- scoring_to_marginal → composite_score, scoring_rubric, scores, parked_at
- researching_to_mvp_speccing → business_brief
- mvp_speccing_to_cto_review → mvp_spec
- cto_review_to_branding → cto_feasibility
- branding_to_ready → brand, validation_kit

### system-nodes.yaml: state_schema on 4 nodes

Each system node now declares its state table schema:
- scan-orchestrator (9 fields): scan tracking, expected/completed scanners, status
- discovery-aggregator (7 fields): pending dedup candidates with match status
- scoring-node (6 fields): dimension accumulation, analyst assignment
- validation-orchestrator (5 fields): gate state JSONB, revision count

### ddl-canonical.sql: workflow_instances table

Platform-level workflow state table added (38 tables total):
- instance_id, workflow_name, workflow_version, current_stage
- transition_history JSONB, accumulator_state JSONB, timer_state JSONB, metadata JSONB

### What this enables

The implementer can derive the complete workflow DB structure from the YAML package:
1. Entity table → workflow-schema.yaml entity_schema (8 groups, ~30 fields)
2. Node state tables → system-nodes.yaml state_schema (4 tables, 27 fields)
3. Data flow → workflow-schema.yaml data_accumulation (which transition writes which field)
4. Mailbox/tasks → tool-schemas.yaml (mailbox_send + human_task_request schemas)
5. Platform tables → platform-spec.yaml (workflow_instances DDL)

No separate DB YAML needed. The schema is implicit in the workflow + event contracts and is now explicit.

### Audit fix touches (from v2.2.0 audit)

- event-catalog.yaml: replaced 6 stale pipeline-coordinator/scan-campaign-manager references, pipeline.dead_letter → 5 alternate_emitters, consumer_type on 5 events, freeform strings → lists
- ddl-canonical.sql: workflow_instances table added (38 tables)
- empireai-v2_2_0.md: §5.8 rewritten with 5-node table, dimension count 9+2=11, subscription counts corrected, action count 13, table count 38
- tooling.lock: → 2.2.0
- verification-gates.yaml: all gates spec_version → 2.2.0
- CHANGELOG-v2.2.0.md: runtime_handling count 56, sub-distribution corrected
