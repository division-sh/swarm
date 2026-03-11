# MAS Platform v1.1.0 Implementation Plan

Date: 2026-03-10
Author: Codex implementer

## Goal

Bring the codebase to absolute MAS platformization:

- MAS contracts under `docs/specs/mas-platform` are the runtime source of truth.
- Generic platform/runtime code does not depend on Empire-specific boot wiring, product hooks, product policy, entity vocabulary, or handwritten product schema as permanent authorities.
- Empire behavior remains only as declarative product assets or explicitly product-owned code, not as hidden semantics inside generic packages.

This plan is based on codebase analysis plus the MAS-default migration work already completed through the runtime core.

## Scope Boundary

This plan intentionally excludes `internal/dashboard` from the active migration scope.

- Dashboard remains known residual debt.
- Dashboard references must not be used to justify leaving product leakage in generic runtime/platform code.
- A later dashboard cleanup can happen after the non-dashboard platform layers are clean.

## Source-of-Truth Rule

The authoritative contract source is `docs/specs/mas-platform`.

- MAS contracts and platform spec define the runtime model.
- Existing tests are migration inputs, not automatic truth, if they assert pre-MAS behavior.
- Handwritten SQL, generated registries, prompt alias maps, and compatibility shims are not authoritative if they disagree with MAS contracts.

## End-State Requirement

The end state is stricter than “runtime mostly works under MAS.”

It requires all of the following:

- generic runtime behavior resolved from platform machinery plus declarative MAS assets
- no required Empire workflow module or Empire product policy at generic runtime boot
- no required Empire hook module, action registry, spawn shim, or routing shim for workflow correctness
- no Empire taxonomy such as `vertical`, `opco`, `coordinator`, `holding`, `factory` in generic platform/runtime code
- no permanent handwritten parallel datastore authority beside MAS/platform contracts
- compliance and startup verification aligned to MAS contract semantics, not legacy bridge semantics
- remaining Empire references confined to declarative product assets or explicitly product-owned packages
- the semantic model is a real MAS semantic model:
  - nested handler-bearing structures are typed
  - handler-first execution can run `rules`, `on_complete`, and `accumulate` without flattening back to bridge logic
  - a second product can be introduced through contracts/composition rather than edits across generic packages

## Current State Snapshot

### Material progress already made

- MAS package tree is now the active runtime source for workflow execution.
- Handler-first execution, CEL-backed guard/rule evaluation, dynamic flow activation, and timer lifecycle are materially implemented in the runtime core.
- `internal/runtime/pipeline` is green under MAS-default semantics.
- full `internal/runtime` is back to the expected non-dashboard envelope; the remaining known failure is the out-of-scope dashboard genericity guard

### Retro-validation reopen items from Phases 0-4

These gaps were found after re-auditing the already-completed runtime-core phases against the stricter end-state bar. They are now part of the master plan and must not be forgotten:

- Phase 0/1: wildcard handler execution is still incomplete because derived handler-transition lookup remains exact-match even though ownership and direct handler lookup are wildcard-aware.
- Phase 0: package merge behavior is still duplicate-key/equality based, not fully proven against MAS path-keyed merger semantics.
- Phase 2: handwritten Empire guard/action execution by ID still exists in generic runtime.
- Phase 2: validation lifecycle behavior is still partly implemented directly in generic Go.
- Phase 3: dynamic flow instances still do not have first-class path identity in persistence.
- Phase 4: persisted lifecycle timer identity is still weaker than runtime timer identity because schedule persistence does not key by timer/task identity.

### Remaining genericity gaps

The main unresolved gaps are no longer in the narrow transition engine. They are in surrounding platform layers:

- runtime boot still hardwires Empire defaults
- product policy still leaks into generic runtime behavior
- store/inbound/bus layers still encode Empire entity vocabulary and routing assumptions
- persistence/schema ownership is still split across MAS spec, handwritten SQL, and store code
- generated payload/schema registries still compile Empire contracts into generic packages
- product-shaped packages such as `internal/factory` and `internal/commgraph/empire` are not yet accounted for in the migration plan

## Adversarial Audit Reset

The 2026-03-11 adversarial audit changes the plan in three important ways:

1. The semantic model is now treated as the first structural blocker.

- `HandlerTransitionSemantic` and `SystemNodeEventHandler` are still flattened around untyped `map[string]any` / `any` fields for:
  - `OnComplete`
  - `Rules`
  - `Accumulate`
  - `Guard`
  - `Compute`
  - `FanOut`
  - and several smaller nested handler fragments
- this is not acceptable for the end-state bar because:
  - nested MAS handler fields cannot be recursively validated
  - the handler-first engine still bails out on the most complex handlers
  - runtime walkers still manually inspect nested maps in multiple places

2. The source-of-truth pipeline is still split.

- the generated schema registry still targets the legacy flat event catalog rather than the resolved MAS bundle
- `payload_fields.go`, prompt-schema guards, and parts of compliance still inherit that stale authority
- `ddl-canonical.sql` still asserts stronger authority than the plan allows

3. Phase closure must now be honest.

- Phase 0: materially complete
- Phase 1: complete
- Phase 2: incomplete
- Phase 3: incomplete
- Phase 4: materially complete
- Phase 5+: still valid, but they now depend on Phase 2/3 corrective work being explicitly reopened

## Audit-Validated Gaps

The following gaps are treated as real and in-scope unless proven otherwise during implementation:

1. `internal/runtime/runtime.go` still hardwires `pipeline/empire` and `productpolicy/empire`.
2. `internal/runtime/productpolicy/` still drives generic runtime behavior for scan/discovery/control-plane semantics.
3. `internal/events/types.go` still makes `VerticalID` a first-class generic event-envelope field.
4. `internal/runtime/pipeline/workflow_nodes_runtime.go` still hardcodes Empire node IDs and concrete executors in the generic registry.
5. `internal/runtime/pipeline/workflow_node_validation.go`, `internal/runtime/pipeline/coordinator_validation.go`, and `internal/runtime/pipeline/workflow_hooks.go` still encode Empire validation/gate/action semantics directly in generic runtime code.
6. `internal/runtime/contracts/prompts.go` still hardcodes operating-role alias behavior.
7. `internal/runtime/contracts/payload_fields.go` and `internal/runtime/contracts/schema_registry_generated.go` still embed Empire event/schema authority into generic runtime contracts code.
8. `internal/runtime/pipeline/lifecycle_orchestrator.go` and `internal/runtime/pipeline/scan_campaign_manager.go` still need explicit end-state treatment instead of implicit “later cleanup.”
9. `internal/runtime/bus/routing.go` and related bus code still hardcode Empire namespace classification.
10. `internal/store/*`, `internal/runtime/inbound.go`, `internal/runtime/workspace/manager.go`, and workflow persistence still assume `vertical`-shaped data/storage/workspace semantics.
11. `contracts/ddl-canonical.sql`, compliance gates, and store boot logic still act as primary datastore authority.
12. `internal/factory/` is currently unplanned product-domain code inside the repo and needs explicit disposition.
13. `internal/commgraph/empire/` is currently unplanned product-policy topology code and needs explicit disposition.
14. late phases were too vague; they now need the same slice-level rigor as Phases 0-4.
15. `internal/runtime/contracts/payload_fields.go` is already stale relative to the live event catalog, and the schema generator still targets the flat legacy catalog instead of the resolved MAS bundle.
16. `internal/runtime/contracts/prompts.go` and `internal/promptcontracts/` still bypass the MAS contract loader and hardcode Empire prompt alias/path behavior.
17. the MAS `expected.yaml` test framework is still largely aspirational; current Go coverage is broad but not yet catalog-driven.

## Plan Structure

The migration is now divided into eight phases.

Phases 0-4 are runtime-core convergence.
Phases 5-8 are platform-conformance and genericity burn-down.

## Phase Status

- Phase 0: materially complete
- Phase 1: complete
- Phase 2: reopened and incomplete
- Phase 3: reopened and incomplete
- Phase 4: materially complete
- Phase 5: next active phase
- Phase 6: planned
- Phase 7: planned
- Phase 8: planned

## Immediate Priority Reset

Before claiming more Phase 5-8 completion, the following priorities must be addressed in order:

1. Typed recursive semantic model

- introduce typed nested structs for MAS handler-bearing fields
- remove the `map[string]any` / `any` semantic gaps in the contract model
- make handler-first execution capable of running nested `rules`, `on_complete`, and `accumulate`

2. Source-of-truth pipeline repair

- repoint generated schema/payload authority at the resolved MAS bundle
- stop treating legacy flat catalogs as active authority
- remove contradictory “authoritative” claims from handwritten DDL

3. Honest phase reclosure

- Phase 2 cannot close while handwritten guard/action IDs and validation lifecycle logic remain in generic runtime
- Phase 3 cannot close while `SpawnOpCo` remains active in runtime control flow and path identity is not first-class in persistence

4. Event-envelope and node-registry structural cleanup

- `events.Event.VerticalID` must be replaced by a generic scope/entity model
- `workflow_nodes_runtime.go` must stop hardwiring Empire node registries

## Phase Alignment To End State

- Phase 0 removes lossy contract translation.
- Phase 1 removes bridge-era flat-transition dependence.
- Phase 2 turns the flattened bridge semantic model into a typed MAS semantic model and removes handwritten Empire guard/branch semantics from active runtime execution.
- Phase 3 removes required Empire-specific instance/spinup paths and makes path identity first-class.
- Phase 4 removes handwritten timer semantics from active runtime behavior.
- Phase 5 repairs source-of-truth integrity and makes MAS platform conformance explicit instead of implicit.
- Phase 6 removes product-policy, permissions, tooling, session, and orchestration semantics from generic runtime layers.
- Phase 7 removes handwritten datastore authority and replaces it with contract/platform-derived persistence ownership.
- Phase 8 removes residual Empire taxonomy, generated contract leakage, product-domain packages in generic layers, and remaining naming/constant/classification debt.

If a phase does not reduce one of those dependency classes, it is incomplete even if tests are green.

## Recommended Sequence

1. contract surface alignment
2. MAS-default migration audit and handler-first completion
3. declarative semantics via CEL
4. addressing model and dynamic flow instances
5. timer lifecycle
6. MAS conformance and verification
7. policy/tool/session/platform extraction
8. datastore platformization
9. repo-wide genericity cleanup

This is the right order because the hardest remaining work is no longer executor semantics; it is the removal of parallel authorities and product leakage from surrounding layers.

## Phase 0: Contract Surface Alignment

### Objective

Make the loader and semantic bundle capable of representing the MAS platform contracts without lossy translation.

### Why this phase exists

If MAS fields are dropped at load time, later phases are forced to keep handwritten runtime behavior.

### Primary entry points

- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contracts/workflow_contracts_test.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`

### Work

- verify loader behavior against the authoritative MAS package format
- expand Go structs for current MAS fields:
  - mapped accumulation writes
  - richer handler fields
  - flow modes and instance metadata
  - timer lifecycle fields
  - typed verification fields needed later
- preserve package-aware semantic bundle shape
- add tests for wildcard handlers, mapped writes, template flow metadata, and MAS package discovery/merge behavior
- close the retro-validation gap where package merging is still stricter than MAS path-keyed merge semantics

### Remaining follow-up from this phase

- explicit `contract_merger` conformance still needs to be proven in Phase 5

### Exit criteria

- no Phase 1-8 MAS field is silently dropped by the loader
- semantic bundle preserves the MAS shapes later phases rely on

## Phase 1: MAS-Default Migration Audit And Handler-First Completion

### Objective

Make MAS the default runtime source and complete handler-first execution for the deferred workflow events.

### Why this phase exists

Bridge-era transitions and allowlists hide non-generic runtime semantics.

### Primary entry points

- `internal/runtime/pipeline/workflow_transition_engine.go`
- `internal/runtime/pipeline/workflow_nodes.go`
- `internal/runtime/pipeline/workflow_transition_engine_test.go`
- `internal/runtime/canned_llm_*`

### Work

- switch runtime default to MAS package tree
- classify failing tests as `keep`, `rewrite`, `delete`
- migrate deferred events to handler-first execution
- remove flat-transition promotion logic where it preserved obsolete behavior
- rebaseline tests to MAS semantics
- complete wildcard handler execution, not just wildcard ownership/lookup

### Exit criteria

- runtime executes against MAS contracts by default
- deferred events run handler-first without event-specific allowlists
- wildcard handler execution is supported end-to-end where MAS handlers use wildcard event patterns

## Phase 2: Declarative Semantics

### Objective

Replace the flattened bridge semantic model with a typed MAS semantic model, then complete declarative execution on that typed surface.

### Why this phase exists

Product-specific decision code inside generic runtime is direct genericity leakage.

### Primary entry points

- `internal/runtime/pipeline/workflow_transition_engine.go`
- `internal/runtime/pipeline/*orchestrator*`
- `internal/runtime/pipeline/*projection*`

### Work

- introduce typed semantic structs for nested MAS handler fields:
  - `GuardSpec`
  - `HandlerRuleEntry`
  - `AccumulateSpec`
  - `ComputeSpec`
  - `FanOutSpec`
  - and smaller typed fragments for gates/filter/reduce/count/clear/payload transforms where needed
- replace untyped `map[string]any` / `any` semantic fields in both:
  - `HandlerTransitionSemantic`
  - `SystemNodeEventHandler`
- support string shorthand plus structured forms through custom YAML unmarshalling where needed
- remove handler-first bailouts that currently reject handlers with nested `rules` / `on_complete`
- execute `guard.check`, `rules.*.condition`, `on_complete.condition`, and `accumulate` continuations declaratively on the typed model
- migrate active paths such as validation, operating approval, structured directives, and scoring fan-out
- remove live dependence on Empire workflow hooks for active runtime paths
- remove handwritten Empire guard/action execution IDs from generic runtime execution
- remove remaining handwritten validation lifecycle behavior from generic runtime execution paths

### Exit criteria

- nested MAS handler-bearing fields are typed in the semantic model
- active MAS handler semantics run declaratively on the typed model
- handler-first execution no longer rejects `rules` / `on_complete` because of flattened semantic gaps
- generic runtime no longer needs Empire workflow hooks for active paths
- generic runtime no longer relies on handwritten Empire guard/action switch logic for active workflow semantics
- validation lifecycle semantics are contract-driven rather than hardcoded by event name in generic runtime

## Phase 3: Addressing Model And Dynamic Flow Instances

### Objective

Implement generic flow-instance creation and addressing, then fully remove active Empire-specific spinup/control paths.

### Why this phase exists

If dynamic lifecycle still depends on Empire-specific spawn code, the runtime is not generic.

### Primary entry points

- `internal/runtime/pipeline/workflow_transition_engine.go`
- `internal/runtime/manager/flow_activation.go`
- `internal/runtime/bus/*`
- `internal/runtime/manager/opco.go`

### Work

- support slash-path addressing and instance-scoped event routing
- implement generic `create_flow_instance`
- support `template`, `instance_id_from`, `config_from`, `auto_emit_on_create`
- move operating activation to generic flow activation
- remove active `SpawnOpCo` runtime control flow from generic runtime correctness paths
- promote path identity to a first-class persistence concern instead of keeping it only in workflow-instance metadata

### Exit criteria

- operating instance creation works via generic platform machinery
- no Empire-specific spawn path is required for runtime correctness
- `SpawnOpCo` is compatibility-only and not part of the active runtime path
- dynamic flow instances have first-class persisted identity aligned to the MAS addressing model

## Phase 4: Timer Lifecycle

### Objective

Implement durable MAS timer lifecycle semantics.

### Why this phase exists

Timers are part of contract semantics and cannot remain partly handwritten conventions.

### Primary entry points

- `internal/runtime/pipeline/workflow_timer_lifecycle.go`
- `internal/runtime/pipeline/scheduler.go`
- `internal/runtime/runtime.go`
- `internal/runtime/pipeline/workflow_instance_store.go`

### Work

- represent MAS timer metadata in semantic contracts
- schedule and cancel lifecycle timers from state/event progression
- restore timers from persisted state at boot
- keep recurring timer behavior correct
- make persisted timer identity exact enough to distinguish multiple lifecycle timers that share owner/event/entity scope

### Exit criteria

- lifecycle timers are fully contract-driven
- no product-specific timer behavior remains in generic runtime paths
- persisted timer scheduling/cancellation identity matches runtime timer identity closely enough to avoid timer collisions

## Phase 5: MAS Conformance, Verification, And Test Framework

### Objective

Repair source-of-truth integrity, then make MAS platform conformance explicit in verification, startup, and automated testing.

### Why this phase exists

Without this phase, the runtime may appear MAS-driven while still accepting or rejecting contracts according to legacy assumptions.

### Primary entry points

- `internal/runtime/contract_compliance_test.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`
- `internal/runtime/runtime.go`
- `scripts/generate_event_schema_registry/main.go`
- `contracts/event-catalog.yaml`
- `contracts/ddl-canonical.sql`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`
- `docs/specs/mas-platform/tests/`

### Slice 5.0: Source-Of-Truth Repair

- repoint generated schema authority at the resolved MAS bundle instead of the flat legacy catalog
- ensure `payload_fields.go`, prompt-schema guards, and schema validation all consume that same MAS-derived authority
- remove or downgrade contradictory “this file is authoritative” claims from handwritten DDL and legacy docs
- make compliance explicitly fail when generated artifacts drift from the MAS bundle

### Slice 5.1: Boot Sequence Conformance

- port MAS `boot_sequence` into explicit Go verification steps
- align runtime startup ordering to the spec’s ordered boot model
- classify boot failures as fatal vs warning based on spec, not bridge-era habits

### Slice 5.2: Event Loop Conformance

- validate the runtime against the MAS event-loop lifecycle
- make event interception, routing, handler execution, timer re-entry, and projection updates match the modeled sequence
- explicitly cover deferred-event reinjection, wildcard routing, dead-letter behavior, and transaction rollback semantics with MAS-conformant tests

### Slice 5.3: Contract Merger And State Model Conformance

- explicitly verify path-keyed merge behavior
- verify entity-scope vs flow-scope state handling against MAS `state_management`
- close any remaining loader/runtime mismatch hidden by permissive merging
- explicitly consume the retro-validation reopen items from Phase 0 and Phase 3 where merge semantics and workflow-instance persistence still diverge from the MAS model
- prove bundle loading and generated schema resolution against the resolved MAS contract set, not just `contracts/event-catalog.yaml`

### Slice 5.4: Accumulation Engine Conformance

- inventory current accumulation behavior vs MAS `all`, `threshold`, and `timeout` modes
- either implement missing accumulation semantics here or explicitly push them into Phase 6 with contract-backed runtime support

### Slice 5.4b: Reopened Runtime-Core Conformance Debt

- resolve the remaining Phase 0-4 reopen items that affect MAS conformance directly:
  - wildcard derived-handler execution
  - handwritten Empire guard/action execution IDs
  - handwritten validation lifecycle logic
  - first-class workflow-instance path identity
  - exact persisted timer identity
  - typed recursive semantics still missing from reopened Phase 2
  - active `SpawnOpCo` / path-identity gaps still missing from reopened Phase 3

### Slice 5.5: MAS Test Framework Adoption

- map current runtime tests to MAS test tiers where possible
- implement the spec’s `expected.yaml`-driven verification harness incrementally
- define how `internal/runtime/masflowtest/` relates to the MAS test framework
- make MAS-defined tests part of compliance, not just local helper coverage
- extract a reusable MAS-default fixture/module loader from the current runtime tests
- keep the existing Go suites as coverage inputs, but stop treating them as a substitute for the catalog runner

### Exit criteria

- MAS-derived generated schema/payload authority is the live source for runtime validation
- runtime startup/verification is MAS-conformant by explicit checks
- contract validation reflects MAS boot semantics
- MAS test framework is partially implemented and is the path forward for platform compliance
- remaining conformance gaps are enumerated, not implicit
- all Phase 0-4 retro-validation reopen items are either closed here or explicitly rescheduled with file-level ownership in later phases

## Phase 6: Policy, Permissions, Tooling, Sessions, And Orchestration Extraction

### Objective

Remove product-shaped policy/orchestration logic from generic runtime packages and replace it with generic platform capabilities plus declarative contracts.

### Why this phase exists

This is where most remaining non-executor genericity leakage lives.

### Primary entry points

- `internal/runtime/productpolicy/`
- `internal/runtime/pipeline/scan_campaign_manager.go`
- `internal/runtime/pipeline/lifecycle_orchestrator.go`
- `internal/runtime/contracts/prompts.go`
- `internal/runtime/tools/`
- `internal/runtime/mcp/`
- `internal/runtime/agents/`
- `internal/commgraph/empire/`
- `internal/factory/`

### Slice 6.1: Product Policy Disposition

- decide whether `productpolicy.Policy` survives as a generic abstraction
- if it survives, shrink it to generic platform concerns only
- if it does not survive, replace all active callers with contract/platform-derived semantics
- remove `SetDefaultFactory` as a required Empire bootstrap path
- isolate `internal/runtime/productpolicy/empire` as explicit product-owned code only; generic runtime must not import it by default
- split current callers into three buckets and migrate in that order:
  - contract-driven replacements such as scan-mode normalization, handler remediation, emit normalization, and prompt/schema checks
  - platform-builtin replacements such as structured directive interception in manager dispatch
  - explicit product-owned injection points such as control-plane identity, workspace class, and global authority overrides

### Slice 6.2: Permissions Model Platformization

- implement the MAS permissions model declaratively
- remove handwritten role checks from Empire policy code
- evaluate permissions from contract/policy assets rather than Empire role conditionals
- cover message authority and mailbox decision rules currently living in `internal/commgraph/empire/`
- make this the prerequisite for tool authorization and mailbox routing cleanup; `commgraph` cannot be retired before producer authorities, mailbox authorities, and role aliases have a generic owner

### Slice 6.3: Lifecycle And Scan Orchestration Extraction

- make the remaining logic in `lifecycle_orchestrator.go` contract-driven or move it out of generic runtime
- make `scan_campaign_manager.go` either a generic orchestrator driven by MAS nodes/contracts or an explicitly product-owned module
- eliminate remaining control-plane and scan-mode product-policy dependence in generic pipeline code
- explicitly close the remaining `opco.*`, `vertical.*`, and scan-campaign compatibility control paths in manager/runtime code
- finish extracting remaining active `productpolicy` reads from:
  - `internal/runtime/pipeline/scan_orchestrator_runtime.go`
  - `internal/runtime/pipeline/coordinator_scan.go`
  - `internal/runtime/pipeline/workflow_instance_projection.go`
  - `internal/runtime/pipeline/discovery_aggregator_runtime.go`
  - `internal/runtime/pipeline/coordinator_discovery.go`
  - `internal/runtime/pipeline/coordinator_scoring.go`
  - `internal/runtime/pipeline/scan_campaign_manager.go`

### Slice 6.3b: Node Registry Platformization

- remove hardcoded Empire node IDs and executor bindings from `workflow_nodes_runtime.go`
- drive node-executor registration from generic platform capabilities plus contract metadata
- make remaining handwritten executors either:
  - generic platform nodes
  - explicit product-owned nodes outside generic runtime

### Slice 6.4: Prompt Templating And Agent Identity

- remove hardcoded role alias mapping from `internal/runtime/contracts/prompts.go`
- derive prompt targeting and variable substitution from MAS agent registry and policy assets
- implement spec-driven prompt templating rather than Empire alias tables
- stop using `internal/promptcontracts` path heuristics as the prompt source of truth; prompt resolution must come from the resolved MAS bundle
- remove `PromptAgentIDForConfig()` alias logic and replace it with MAS agent-registry lookup plus product-owned prompt assets loaded through the resolved bundle

### Slice 6.5: Tool Model And MCP Gateway

- align tools with MAS distinction between platform-builtin and workflow-registered tools
- make tool schemas/registration contract-driven
- align MCP gateway behavior to the platform tool model
- explicitly account for auto-generated entity tools such as typed read/write/search/query if they remain part of the MAS contract surface
- move tool authorization off `productpolicy` and onto the same generic permission/authority model introduced in Slice 6.2

### Slice 6.6: Agent Session Management

- align runtime agent/session modes with the MAS session model
- remove Empire-only assumptions such as per-vertical session semantics from generic agent/runtime code
- explicitly cover the persistence and lock model in `agents`, `agent_sessions`, and `agent_turns`
- eliminate `session_per_vertical`-style assumptions from agent runtime and replace them with generic scope/entity/instance session addressing

### Slice 6.6b: Event Envelope And Scope Model

- remove `VerticalID` as a product-shaped generic event-envelope primitive
- replace it with a generic scope/entity/instance model aligned to MAS addressing and persistence
- migrate bus/store/tools/runtime callers off Empire scope naming

### Slice 6.7: Product-Domain Package Disposition

- explicitly decide the fate of `internal/factory/`
- explicitly decide the fate of `internal/commgraph/empire/`
- move them to product-owned space, generalize them, or delete them
- current bias from audit:
  - `internal/factory/` should be treated as product-owned discovery/scanning code unless replaced entirely by MAS nodes
  - `internal/commgraph/empire/` should be treated as product-owned message policy data behind a generic registry boundary
- record active callers before moving code:
  - `internal/factory/contracts_policy.go` still consumes `runtimeproductpolicy`
  - `internal/commgraph/empire` still feeds message authorities, mailbox round-trips, and role/producer catalogs

### Exit criteria

- generic runtime no longer depends on Empire product policy for active behavior
- permissions are contract-driven
- prompt/tool/session behavior is platformized
- `internal/factory/` and `internal/commgraph/empire/` have explicit end-state disposition

## Phase 7: Datastore And Persistence Platformization

### Objective

Replace handwritten SQL/schema authority with platform/contract-derived persistence ownership.

### Why this phase exists

The runtime is not fully platformized while MAS spec, `ddl-canonical.sql`, and store code are all competing authorities.

### Primary entry points

- `contracts/ddl-canonical.sql`
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contract_compliance_test.go`
- `internal/store/`
- `internal/runtime/pipeline/state_store.go`
- `internal/runtime/pipeline/workflow_instance_store.go`
- `internal/runtime/tools/executor_sql.go`
- `internal/runtime/tools/executor_external.go`

### Slice 7.1: Workflow-State Schema Authority

- align workflow persistence shape to MAS platform spec
- remove legacy column-name mismatches such as `current_stage` vs `current_state`
- make workflow-state schema derivable from platform contracts
- eliminate `workflow_instances` as a generic escape hatch for handwritten product buckets that duplicate dedicated tables
- separate platform-owned workflow state from product projections; `workflow_instances.accumulator_state` must stop acting as a mixed platform-plus-product compatibility bucket

### Slice 7.2: Entity Table Platformization

- reconcile MAS `entity_schema` with the current `verticals` table
- eliminate direct generic-runtime dependence on handwritten Empire entity columns
- decide whether the entity table remains product-owned generated output or becomes a platform-generated artifact
- explicitly cover `events`, `agents`, `event_deliveries`, `event_receipts`, `mailbox`, `human_tasks`, and other platform-owned tables separately from product-derived entity tables
- inventory the extra handwritten `verticals` columns that do not come from `package.yaml -> entity_schema` and decide whether they become generated product fields, flow state, or removable compatibility data

### Slice 7.3: Runtime Business-State Tables

- inventory and platformize tables such as:
  - `pending_dedup_candidates`
  - `scan_accumulators`
  - `validation_pipelines`
  - `scoring_digest_buffer`
- decide which are:
  - platform-owned generic state
  - product-owned generated state
  - obsolete and removable
- remove duplicated state across dedicated tables and `workflow_instances.accumulator_state`
- explicitly inventory legacy/runtime-specific stores such as `template_routing_store`, `org_templates`, `template_migrations`, and `bootstrap_versions`
- include the active JSON-only runtime state buckets in that inventory:
  - `workflow_instances.accumulator_state["scoring-node"]`
  - `workflow_instances.accumulator_state["build-orchestrator"]`
  - `workflow_instances.accumulator_state["validation-orchestrator"]`
  - legacy compatibility buckets such as `scoring-restore`, `scan-state`, and `pending-dedup`

### Slice 7.4: Schedule/Agent/Inbound Persistence

- remove product vocabulary from generic schedule, inbound, and agent persistence where possible
- align table ownership and schemas to platform/contracts
- explicitly redesign:
  - `internal/runtime/inbound.go`
  - `internal/store/inbound.go`
  - `internal/store/agent_store.go`
  - `internal/runtime/workspace/manager.go`
  so they no longer depend on `vertical` as a generic storage/workspace primitive
- make schedule identity first-class in storage instead of leaking task identity through payload JSON
- explicitly classify unresolved tables during this slice:
  - `agents`
  - `scan_campaigns`
  - `geographies`
  - `inbound_events`
  and decide whether each is platform-owned, product-derived, or removable

### Slice 7.5: Generated Persistence Tools

- implement contract-derived entity tools such as typed read/write/search/query surfaces if still required by the MAS spec
- stop treating ad hoc SQL access as the long-term generic platform API

### Slice 7.6: Compliance Refactor For Derived Schema

- replace compliance that blesses handwritten SQL with compliance that verifies derived schema against runtime expectations
- stop treating `ddl-canonical.sql` as the normative authority
- require a contract-to-DDL or contract-to-store-materialization pipeline so schema authority is executable, not just documented
- close the current smell where timer identity survives storage only through `payload.__schedule_task_id`; exact schedule identity must become schema-level, not payload-level

### Exit criteria

- schema authority is singular and contract/platform-driven
- generic runtime/store code no longer depends on handwritten Empire table shape as truth
- compliance validates derived persistence, not manual SQL drift

## Phase 8: Repo-Wide Genericity Burn-Down

### Objective

Remove the residual Empire taxonomy, generated product artifacts, legacy compatibility shims, and generic-package naming/constant debt left after platform semantics and persistence are complete.

### Why this phase exists

This is the final “no exception” phase. It is where the codebase stops merely behaving generically and actually becomes structurally generic.

### Primary entry points

- `internal/runtime/runtime.go`
- `internal/runtime/contracts/payload_fields.go`
- `internal/runtime/contracts/schema_registry_generated.go`
- `internal/runtime/contracts/prompts.go`
- `internal/runtime/bus/routing.go`
- `internal/runtime/bus/*cycle*`
- `internal/runtime/pipeline/FactoryPipelineCoordinator` and related files
- `internal/config/config.go`
- other non-dashboard generic packages identified by the final audit

### Slice 8.1: Generic Boot Wiring

- remove Empire-default runtime construction from `internal/runtime/runtime.go`
- require explicit module/policy/config injection at product composition boundaries instead of generic boot
- stop importing `pipeline/empire` and `productpolicy/empire` from generic runtime boot

### Slice 8.2: Generated Contract Artifact Ownership

- move Empire-generated payload/schema artifacts out of generic contracts packages or regenerate them from MAS contracts in a clearly product-owned path
- ensure generic runtime only consumes generic interfaces/loaders
- fix the current split where `schema_registry_generated.go` is generated from the flat catalog and `payload_fields.go` is already stale

### Slice 8.3: Naming And Taxonomy Cleanup

- rename `FactoryPipelineCoordinator` and related generic types to platform-neutral names
- remove residual Empire naming from generic files, constants, comments, and runtime types
- include `VerticalID`-shaped event-envelope fields, `factory` mode defaults, and `opco`/`coordinator` terminology in the final naming audit

### Slice 8.4: Constants And Classification Cleanup

- remove magic constants such as scoring timeout, OpCo cycle defaults, and default escalation roles from generic code where they should be contract/policy driven
- remove hardcoded bus event classification tables where routing should be contract- or platform-model-driven
- remove residual `OpCoCycleTracker` / cycle-role defaults from generic bus code

### Slice 8.5: Final Compatibility Shim Removal

- retire compatibility-only manager/bootstrap surfaces that are no longer needed
- remove remaining bridge-era aliases, helper shims, and temporary runtime fallbacks

### Slice 8.6: Repo-Wide Genericity Audit Gate

- run a final non-dashboard genericity audit
- block completion on Empire taxonomy inside generic platform/runtime/store/bus/config/contracts packages
- allow Empire vocabulary only in declarative product assets and explicitly product-owned packages

### Exit criteria

- generic runtime/platform/store/bus/config/contracts packages are free of Empire-specific taxonomy and hidden product behavior
- remaining Empire artifacts live only in declarative assets or explicit product-owned code
- final genericity audit passes

## Final Cleanup Gate

The plan is not complete unless all of the following are true:

- generic runtime does not hardwire Empire module or policy defaults
- generic runtime does not require Empire workflow hooks or action registries
- generic runtime does not require Empire spawn/bootstrap code for correctness
- generic runtime/store/bus/config/contracts packages are free of Empire taxonomy
- persistence/schema authority is contract/platform-driven rather than handwritten in parallel
- permissions, tools, prompts, sessions, and orchestration semantics are platformized
- `internal/factory/` and `internal/commgraph/empire/` have explicit product-owned or removed status
- generated contract artifacts are owned in the right layer
- MAS conformance checks and test framework coverage exist for the implemented platform model

## Definition Of Done

Full platformization is done only when:

- the generic runtime can execute Empire from declarative contracts/spec assets alone
- the generic runtime no longer requires an Empire runtime package for semantics
- no Empire-specific execution behavior remains embedded in generic code paths
- no parallel handwritten schema authority remains as a permanent exception
- the non-dashboard repo-wide genericity audit passes

## Risks

- The largest remaining risk is underestimating Phases 6-8. They now contain more genericity work than the earlier executor phases.
- Schema authority migration can destabilize multiple store/runtime layers at once if done without explicit table-by-table ownership decisions.
- Product-policy removal can become a distributed refactor unless permissions, tools, prompts, and sessions are handled as one coordinated phase.
- Generated artifact relocation can create bootstrap churn unless runtime consumers are first pushed behind generic loaders/interfaces.
- The main failure mode remains stopping at “runtime is generic enough” while generic packages still embed Empire assumptions structurally.

## Immediate Next Milestone

Phase 5 should start next, with its own subplan, because the remaining work is now about explicit MAS conformance and verification rather than more ad hoc runtime convergence.

## Baseline Checks Previously Run

These targeted checks were run successfully during plan preparation and prior runtime-core phases:

- `go test ./internal/runtime/contracts -run 'TestResolveWorkflowContractPaths_DiscoversPackageLayout|TestLoadWorkflowContractBundle_LoadsCurrentRootFields' -count=1`
- `go test ./internal/runtime/pipeline -run 'TestValidateWorkflowContracts_CurrentBundle|TestValidateWorkflowContracts_CurrentRootBundleFixture' -count=1`
- `go test ./internal/runtime -run 'TestContractCompliance|TestEnsureRecurringWorkflowSchedules_UsesRecurringTimersFromContracts|TestEnsureRecurringWorkflowSchedules_DoesNotProvisionStageTimersAtStartup' -count=1`
