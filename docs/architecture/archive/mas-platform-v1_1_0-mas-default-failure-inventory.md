# MAS-Default Failure Inventory

Date: 2026-03-10
Author: Codex implementer

## Context

The Empire runtime module now loads the authoritative MAS package tree by default:

- `docs/specs/mas-platform/empire/contracts`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

This inventory records the first failure pass after that switch. These failures are migration inputs, not automatic regressions.

## Source Runs

- `go test ./internal/runtime/pipeline -count=1`
- `go test ./internal/runtime -count=1`

## Classification Summary

### `rewrite`

These tests still express valid intent, but their assertions are pinned to legacy runtime shapes rather than MAS semantics.

- Flat-transition and alias-parity assertions in [workflow_transition_engine_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine_test.go)
  - expects flat transition IDs like `shortlisted_to_researching`
  - expects fallback flat behavior for MAS handler-owned events
  - expects old alias parity for `spec.validation_failed` and `cto.spec_revision_needed`
  - expects older execution-order assumptions like `compute`-led plans
- Runtime topology assumptions in [workflow_nodes_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_test.go) and [runtime_interfaces_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/runtime_interfaces_test.go)
  - expects five runtime nodes
  - expects executor coverage based on the old node model
  - expects older consume policies for edge events
- Workflow instance projection expectations in [coordinator_workflow_instance_projection_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_workflow_instance_projection_test.go)
  - expects legacy workflow identity/version and legacy scan-state projection details
- Runtime/bootstrap tests in [runtime_bootstrap_test.go](/Users/youmew/dev/empireai/internal/runtime/runtime_bootstrap_test.go)
  - currently fail because validation still enforces bridge-era assumptions instead of MAS-aligned semantics
- End-to-end canned workflow tests in [canned_llm_additional_scenarios_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_additional_scenarios_e2e_test.go) and [canned_llm_full_pipeline_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_full_pipeline_e2e_test.go)
  - expected event sequences no longer match MAS handler-first validation flow

### `keep`

These failures look like real runtime/spec mismatches or generic validation issues and should remain blocking until explained or fixed.

- Contract validation failures in [workflow_contract_validation_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation_test.go)
  - missing merged required-agent roles
  - missing event catalog entries
  - handler `advances_to` outside flow states
  - flow terminal states missing from states
  - `fan_out.count` / `data_accumulation.source_event` mismatch checks
- Runtime bootstrap failures in [runtime_bootstrap_test.go](/Users/youmew/dev/empireai/internal/runtime/runtime_bootstrap_test.go)
  - these are downstream of the same validation issues, so they remain meaningful until validation is MAS-correct
- Generic runtime behavior mismatches that may reflect real bugs:
  - duplicate `opco.spinup_requested` in [canned_llm_full_pipeline_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_full_pipeline_e2e_test.go)
  - consumed-vs-processed bookkeeping mismatch in [coordinator_transitions_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_transitions_test.go)

### `delete`

These tests appear to preserve obsolete bridge-era architecture and should be removed once their intent is either covered elsewhere or proven obsolete.

- Legacy “five node model” preservation in [workflow_nodes_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_test.go)
- Flat-transition identity preservation where the runtime is expected to be handler-first by design in [workflow_transition_engine_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine_test.go)

## Immediate Migration Queue

1. Fix or relax MAS-invalid validation assumptions in [workflow_contract_validation.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go).
2. Rewrite transition-engine tests around handler-first MAS semantics instead of flat alias parity.
3. Rewrite runtime node tests around MAS node ownership and execution coverage, not node count.
4. Rebaseline canned LLM scenarios against MAS event sequences after validation flow semantics are settled.
5. Revisit duplicate-event and consumed/processed mismatches as likely real runtime issues after the test rewrites.

## Plan Impact

The phased plan remains coherent, but Phase 1 must explicitly include:

- MAS-default runtime activation
- failure classification
- test migration
- removal of bridge-era test and runtime assumptions

That work is now part of the critical path, not a cleanup after semantic migration.

## Update After Slice 1.1 / 1.2 Progress

Follow-up MAS-default work has already retired several items from the first pass:

- validation/bootstrap alignment is green on the targeted current-bundle checks
- runtime topology tests were rebaselined to MAS node ownership instead of the bridge-era five-node model
- workflow-instance projection tests now seed MAS workflow identity correctly
- `portfolio-node` now has runtime executor coverage for `timer.portfolio_digest`, `runtime.reset`, `budget.threshold_crossed`, and `system.directive`
- scan-state projection now derives `expected_scanners` from MAS `scan.requested` rule fan-out targets instead of legacy placeholder slots
- state-machine and contract-verification tests were rewritten around MAS flow/handler semantics instead of legacy flat workflow identity

As of the latest full `go test ./internal/runtime/pipeline -count=1` run, the remaining red surface is now concentrated in:

- no remaining `pipeline` package failures

Follow-up runtime rebaseline work retired the remaining MAS-default scenario/bootstrap items:

- [canned_llm_additional_scenarios_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_additional_scenarios_e2e_test.go)
  - Scenario 5, 6, and 8 are now green under MAS-default
- [canned_llm_full_pipeline_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_full_pipeline_e2e_test.go)
  - full directive-to-OpCo scenario is now green under MAS-default
- [runtime_bootstrap_test.go](/Users/youmew/dev/empireai/internal/runtime/runtime_bootstrap_test.go)
  - recurring workflow timer expectations were rebaselined to current MAS contracts and are green

The latest broader `go test ./internal/runtime -count=1` run is now red only on:

- [architecture_guards_test.go](/Users/youmew/dev/empireai/internal/runtime/architecture_guards_test.go)
  - still flags pre-existing Empire literals in dashboard code outside this migration slice

That means the inventory has materially shifted from “mixed runtime/test migration noise” to:

- `pipeline`: green under MAS-default
- targeted `internal/runtime` MAS-default migration coverage: green
- remaining full-runtime red surface: unrelated dashboard architecture-guard debt
