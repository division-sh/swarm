# Platformization Final Backlog

Status: active working brief
Spec baseline: `v2.1.0`
Last updated: 2026-03-08

## Goal
Reach full platformization and `v2.1.0` compliance:

- no Empire-specific behavior in generic runtime/platform layers
- workflow/routing/state/timers driven from authoritative YAML contracts
- `workflow_instances` as the canonical orchestration source of truth
- product-specific policy isolated behind injected module implementations

## Current State
The codebase is contract-aligned and materially more platformized than before, but it is not yet fully platformized.

What is already true:

- root `contracts/*` are aligned to the authoritative `v2.1.0` YAMLs
- runtime contract compliance is green
- generic `pipeline/` no longer directly bootstraps Empire the way it used to
- workflow projections and timer state now exist in `workflow_instances`

What is not yet true:

- generic runtime layers still contain Empire-specific logic and literals
- workflow routing is still partly code-owned
- hook execution is still partly switch-based
- legacy orchestration state still coexists with workflow-backed state

## Migration Principles

1. Do not edit `contracts/*` during this migration unless explicitly requested.
2. Remove Empire logic by moving it behind module injection, not by deleting behavior.
3. Prefer contract-driven routing and execution over code overlays.
4. Replace legacy orchestration reads only after equivalent workflow-backed reads are proven.
5. Tighten architecture guards after each cutover so removed debt does not come back.

## Final Backlog

### Phase 1: Product-Agnostic Contract Assembly
Status: completed on 2026-03-08

Objective: remove Empire naming and behavior from the contract loading layer.

Tasks:

1. Replace Empire-specific APIs in `internal/runtime/contracts/workflow_contracts.go`.
   - Remove or rename:
     - `EmpireContractPaths`
     - `ResolveEmpireContractPaths`
     - `LoadEmpireWorkflowContractBundle`
     - `EmpireContractFilesExist`
   - Replace them with generic workflow-contract repository APIs.

2. Make runtime/bootstrap construct product wiring explicitly.
   - Product selection should happen in bootstrap.
   - Generic runtime should consume an injected module/bundle only.

3. Remove any remaining Empire default-factory assumptions outside bootstrap/test harnesses.

Exit criteria:

- generic contract loading APIs contain no Empire naming
- runtime bootstrap is the only place that selects the Empire module
- tests pass without generic runtime calling Empire loaders directly

Completed:

- renamed the runtime contract assembly APIs to generic names in `internal/runtime/contracts`
- moved workflow bundle/definition/node/registry ownership into injected `WorkflowModule`
- removed Empire-named pipeline singleton entrypoints and contract-loader calls from generic `pipeline/`
- updated runtime bootstrap to assemble and validate the Empire module explicitly
- added test-module bridges so package test binaries install a module intentionally rather than relying on hidden production fallbacks

### Phase 2: Contract-Driven Routing
Status: completed on 2026-03-08

Objective: make workflow transition observation and routing derive from YAML contracts first.

Tasks:

1. Reduce or remove `workflowNodePolicyOverlay(...)` in `internal/runtime/pipeline/workflow_nodes.go`.
   - Derive visibility from workflow triggers, timers, node ownership, and system-node contracts.
   - Keep only narrow runtime-local policy that cannot be expressed in contracts.

2. Stop using coordinator subscription breadth as the routing authority.
   - Transition routing should come from workflow definitions and node ownership.

3. Add tests that prove operating and timer transitions route correctly without code overlays.

Exit criteria:

- routing behavior is contract-first
- overlay logic is minimal and justified
- operating/timer routing tests pass against contracts alone

Completed:

- extended event-catalog parsing in `internal/runtime/contracts` to include routing semantics such as `intercepted`, `passthrough`, and `runtime_handling`
- replaced the large hand-authored policy map in `internal/runtime/pipeline/workflow_nodes.go` with a contract-derived policy builder
- reduced the runtime overlay to a small set of executor-owned event declarations plus a few explicit runtime-only overrides
- updated wiring verification so it recognizes contract-driven runtime policy events instead of relying on the old overlay function shape
- kept scanner/timer/runtime edge cases explicit where the contracts still do not directly express the exact top-level event-struct requirement used by the interceptor

### Phase 3: Empire Removal From Generic Runtime Layers
Status: in progress on 2026-03-08

Objective: eliminate Empire literals and policy from generic runtime, manager, tools, and agent layers.

Tasks:

1. Remove Empire-specific logic from `internal/runtime/agents/agent_llm.go`.
   - No hardcoded `empire-coordinator`
   - No hardcoded scan-mode inference
   - No Empire-specific remediation rules in generic agent runtime

2. Remove Empire-specific logic from runtime tools.
   - `internal/runtime/tools/executor_emit_normalization.go`
   - `internal/runtime/tools/executor_emit_guardrails.go`
   - `internal/runtime/tools/executor_emit.go`
   - `internal/runtime/tools/executor.go`
   - move product-specific rules behind injected module/tool policy

3. Remove Empire-specific fallbacks from runtime manager.
   - `internal/runtime/manager/runtime.go`
   - `internal/runtime/manager/receipts.go`
   - no hardcoded `empire-coordinator` for escalation, quarantine, or manager resolution

4. Remove or isolate Empire-specific behavior from legacy factory/runtime integration.
   - `internal/factory/pipeline.go`
   - `internal/commgraph/registry.go`
   - if these remain, they must consume injected module wiring rather than embed Empire literals

Exit criteria:

- generic runtime layers contain no product literals or product-specific decision logic
- Empire behavior lives only in Empire module/package or explicit product config
- architecture tests fail on reintroduction of Empire literals

Progress:

- moved Empire-specific agent/tool/manager behavior behind `internal/runtime/productpolicy` with the active Empire implementation in `internal/runtime/productpolicy/empire`
- removed direct `empire-coordinator` / scan-mode literals from non-test files under `internal/runtime/agents`, `internal/runtime/tools`, and `internal/runtime/manager`
- added a runtime architecture guard so those generic runtime layers now fail CI if Empire literals are reintroduced
- replaced the handwritten `internal/commgraph` producer registry with contract-derived producer roles/events from `agent-tools.yaml`, `system-nodes.yaml`, and `event-catalog.yaml`, leaving only non-contract extras explicit
- replaced most handwritten `internal/commgraph` message authority topology with template-derived OpCo hierarchy from `configs/agents/templates/*.yaml`, keeping only true holding-level and peer exceptions manual
- removed direct `empire-coordinator` recipient wiring from `internal/factory` and centralized scan-mode support/defaulting in `internal/factory/contracts_policy.go`
- remaining Phase 3 debt is outside those runtime layers:
  - `internal/commgraph` mailbox topology is still manual
  - `internal/factory` still keeps compatibility rubric/scan-strategy grouping logic in code rather than a first-class contract surface

### Phase 4: Scan/Discovery Platformization
Objective: finish separating scan/discovery behavior from generic pipeline runtime.

Tasks:

1. Move remaining mode taxonomy and complex-directive behavior fully into Empire policy.
2. Keep generic `ScanPolicy` as interface only.
3. Ensure scan campaign manager consumes policy only, not product recipients or product taxonomy.
4. Add regression tests proving no behavior change under injected Empire policy.

Exit criteria:

- generic `pipeline/` contains no Empire scan modes or recipients
- scan/discovery behavior remains green under module injection

Progress:

- generic runtime scan/discovery orchestration already runs through injected scan policy in `internal/runtime/pipeline`
- legacy deterministic factory scan flow now derives direct agent recipients from `event-catalog.yaml` instead of hardcoding `empire-coordinator`
- factory scan-mode support/defaulting is now centralized in `internal/factory/contracts_policy.go` and backed by `contracts/test-vectors/campaign-cycling.yaml`
- factory architecture guards now prevent scattered scan-mode literals or direct `empire-coordinator` recipients from re-entering production files
- `internal/factory` production code now has an architecture guard against reintroducing direct `empire-coordinator` recipient literals
- scan-mode support/defaulting in `internal/factory` is now centralized in a contract-backed policy loader using `contracts/test-vectors/campaign-cycling.yaml`
- remaining gap in this phase is rubric/scan-strategy behavior still being compatibility logic in `internal/factory/contracts_policy.go` rather than a richer first-class contract surface

### Phase 5: Canonical Workflow State Cutover
Objective: make `workflow_instances` the sole orchestration source of truth.

Tasks:

1. Change restore order so workflow state is canonical and legacy tables are projections only.
2. Remove orchestration decisions that still read legacy validation/scoring/scan/dedup state first.
3. Keep in-memory state as cache only, rebuildable from `workflow_instances`.
4. Reduce dual-write dependencies where downstream compatibility no longer requires them.

Exit criteria:

- workflow-stage, timer, revision, scoring, and validation decisions read from `workflow_instances`
- legacy tables are no longer orchestration inputs
- restore works from workflow state alone

Progress:

- `currentWorkflowState()` now hydrates stage, status, and metadata from `workflow_instances` first, merging the persisted `pipeline-coordinator` accumulator bucket before falling back to legacy state
- `WorkflowInstanceStore` now has a mutation API, and workflow stage projection uses that path instead of open-coded load/modify/upsert
- workflow action side effects that mutate metadata, such as `increment_revision_count`, now persist back into `workflow_instances`
- scoring accumulator restore now repopulates live runtime state from `workflow_instances`, and scoring-side updates clear/persist that canonical projection during normal flow
- the synthetic `approved -> operating` workflow overlay has been removed; the runtime now sees only the authoritative operating-phase contract graph
- remaining gap in this phase is that scan/dedup/scoring restore still keeps legacy persistence as a live compatibility input

### Phase 6: Executable Hook Platformization
Objective: replace hardcoded allowlists and switch execution with executable registries.

Tasks:

1. Replace static hook coverage lists in `workflow_runtime_coverage.go`.
2. Make every contract guard/action resolve through:
   - platform builtin executor, or
   - product/module executor
3. Replace manual switch handling in `workflow_transition_engine.go` with registry-backed execution.
4. Fail startup if a referenced contract hook is not executable.

Exit criteria:

- no static “implemented hooks” allowlist remains in the platform path
- contract hook execution is registry-driven
- startup/compliance fail fast on missing executors

Progress:

- guard and action registries now expose executability, not just contract presence
- workflow contract validation now fails if a transition references a guard/action with no executable runtime implementation
- the transition engine now resolves guards/actions through registry definitions first, including `platform_builtin` aliases, before dispatching execution
- non-platform guard/action bodies now dispatch through the Empire module hook executor instead of living directly in generic `workflow_transition_engine.go`
- remaining gap in this phase is that platform bridge actions such as spinup/teardown still have compatibility semantics and are not yet fully realized runtime actions

### Phase 7: Bridge Action Elimination
Objective: remove remaining placeholder workflow actions.

Tasks:

1. Turn metadata-only bridge actions into real runtime actions.
   - `spinup_opco_org`
   - `begin_teardown`
   - any remaining no-op or metadata-only workflow action

2. Remove duplicate or legacy emit paths once the contract-owned action is authoritative.

3. Add transition-level tests proving the action side effects are real and contract-owned.

Exit criteria:

- no workflow action is a placeholder in the runtime path
- contract actions produce real side effects
- legacy duplicate behavior is removed

### Phase 8: Timer Platformization Completion
Objective: finish the workflow timer model under the platform engine.

Tasks:

1. Keep workflow-owned timers entirely stage-driven.
2. Remove leftover compatibility paths where scheduler behavior still bypasses workflow timer state.
3. Ensure timer fire mutates `workflow_instances.timer_state` canonically.
4. Prove all workflow timers work end-to-end:
   - `scan_timeout`
   - `campaign_deadline`
   - `marginal_review`
   - `marginal_kill`
   - `portfolio_digest`

Exit criteria:

- workflow timer lifecycle is fully contract-driven
- timer state is canonical in `workflow_instances`
- no workflow-owned timer depends on legacy scheduling logic

Progress:

- runtime startup now provisions recurring workflow-owned schedules from the authoritative workflow contract instead of hardcoding a dedicated portfolio-digest helper
- the old synthetic startup cron for `timer.marginal_review` has been removed; marginal review remains stage-scoped rather than a global timer
- remaining gap in this phase is the deeper timer-state cutover for non-recurring workflow timers and the remaining scan/campaign compatibility scheduling paths

### Phase 9: Final Architecture Hardening
Objective: make the final architecture enforceable.

Tasks:

1. Tighten `internal/runtime/pipeline/pipeline_architecture_test.go`.
   - remove current Empire bridge whitelist
   - fail on Empire literals in generic runtime code

2. Add architecture guards across runtime/manager/tools if needed.

3. Extend compliance checks to assert:
   - executable hook coverage
   - canonical workflow-state usage
   - contract-driven routing coverage
   - timer coverage

Exit criteria:

- architecture tests enforce the target state
- compliance tests cover the final platform contract/runtime boundary

### Phase 10: Compatibility Cleanup And Re-Baseline
Objective: remove migration-only code and lock the new model in.

Tasks:

1. Delete compatibility paths kept only to preserve pre-platform behavior.
2. Update e2e and integration tests to assert only contract-owned behavior.
3. Write a short closure report summarizing:
   - what legacy paths were removed
   - what platform interfaces are now authoritative
   - what product-specific code remains and where

Exit criteria:

- no migration-only runtime branches remain without explicit justification
- tests reflect the final `v2.1.0` model directly
- closure report is checked in

## Recommended Execution Order

1. Phase 1
2. Phase 2
3. Phase 3
4. Phase 4
5. Phase 5
6. Phase 6
7. Phase 7
8. Phase 8
9. Phase 9
10. Phase 10

## Acceptance Gates

Run after every phase:

```bash
go test ./internal/runtime -run TestContractCompliance -count=1
go test ./internal/runtime/pipeline -count=1
go test ./internal/runtime -count=1
go test ./... -count=1
```

## Definition Of Done

This migration is complete only when all of the following are true:

- generic runtime/platform layers contain no Empire-specific behavior or literals
- workflow routing/execution/timers are driven from authoritative YAML contracts
- `workflow_instances` is the canonical orchestration state model
- product-specific behavior is isolated behind injected module implementations
- architecture and compliance tests enforce the target state
