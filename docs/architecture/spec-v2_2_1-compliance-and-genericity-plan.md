# `v2.2.1` Compliance And Genericity Plan

## Current State
- `v2.2.1` compliance: about `95/100`
- genericity / platformization: about `72/100`

What is already true:
- Root runtime behavior is aligned with the `2.2.x` 5-node model.
- Effective handler coverage is `47/47`.
- Routing is contract-driven from `event-catalog.yaml`.
- `workflow_instances` is used for real restore and more live reads.
- Scan, discovery, validation, lifecycle, and scoring all have contract-shaped node state buckets.

What is still not done:
- Scoring still uses its dedicated runtime node path instead of the same split-executor architecture as the other four nodes.
- Compatibility buckets and legacy restore/state fallbacks still exist.
- The runtime is product-shaped but not yet fully generic.

## Goal
Reach:
- full `v2.2.1` compliance under root contracts
- a cleaner platform boundary where product logic is mostly injected and contract-wired, not baked into runtime structure

## Phase 1: Finish Root `v2.2.1` Adoption
Goal:
- make root `contracts/*` authoritative at `v2.2.1` and restore a green suite

Tasks:
- update runtime/template/spec version markers from `2.2.0` to `2.2.1`
- fix compliance/test assumptions that still require direct subscription to owned transition triggers
- add any missing handler coverage tests required by `v2.2.1`
- rerun and stabilize:
  - `go test ./internal/runtime -run TestContractCompliance -count=1`
  - `go test ./... -count=1`

Exit criteria:
- root contracts stay at `v2.2.1`
- `TestContractCompliance` passes with no special casing for `v2.2.0`
- full suite is green

## Phase 2: Align Compliance Logic To The `v2.2.1` Ownership Model
Goal:
- make the validation/compliance layer reflect the clarified contract semantics

Tasks:
- distinguish clearly between:
  - event visibility in `event-catalog.yaml`
  - transition ownership in `workflow-schema.yaml`
- remove old checks that require a system node to directly subscribe to every owned transition trigger
- keep checks that still matter:
  - owning node exists
  - runtime executor exists
  - trigger exists in event catalog
  - runtime can observe or process the event under the catalog routing model

Exit criteria:
- compliance checks fail only on real contract/runtime mismatches
- no false failures caused by the old coordinator-era subscription model

Status:
- completed for the current `v2.2.1` contract adoption baseline

## Phase 3: Unify Scoring With The Split Executor Architecture
Goal:
- remove the last major architectural outlier in the 5-node runtime model

Tasks:
- decide the target architecture:
  - either move scoring fully into the split workflow-executor path
  - or formalize the dedicated scoring node as an intentional exception in runtime architecture docs/tests
- if unified:
  - make `workflow_node_scoring.go` own `vertical.discovered`, `vertical.derived`, `score.dimension_complete`, and `scoring.contest_resolved`
  - reduce `scoring_node.go` to a shared service or remove it
- update architecture tests to reflect the final decision

Exit criteria:
- scoring is no longer an ambiguous special case
- runtime architecture matches the 5-node contract model more directly

Current note:
- today scoring is an explicit architectural exception:
  - contracts are correct
  - handler coverage is complete
  - runtime still executes scoring through `scoring_node.go` rather than the same split-executor path used by scan/discovery/validation/lifecycle

## Phase 4: Complete The `workflow_instances` Cutover
Goal:
- make workflow state the clear orchestration source of truth

Tasks:
- reduce remaining live reads that prefer legacy tables or in-memory maps when workflow projection data exists
- keep compatibility projections only for migration/reporting
- tighten restore/read/write rules around:
  - scan state
  - discovery pending state
  - validation state
  - scoring state
- remove dead paths once equivalent workflow-backed behavior is proven

Exit criteria:
- runtime decisions are predominantly workflow-instance-first
- legacy state is clearly secondary

## Phase 5: Strengthen `state_schema` And `entity_schema` Enforcement
Goal:
- move from “contract-shaped writes exist” to “contract-shaped state is enforced”

Tasks:
- enforce field-set validation for the remaining node buckets
- continue reducing compatibility-rich buckets where contract-shaped buckets already suffice
- keep `data_accumulation` writes constrained to declared `entity_schema`
- add targeted tests for schema-only state persistence and restore

Exit criteria:
- node state buckets stay within declared schemas
- undeclared accumulation/state writes fail validation or are rejected consistently

## Phase 6: Remove Stale Coordinator / Interceptor Assumptions
Goal:
- make the old coordinator model impossible to regress into

Tasks:
- remove or minimize remaining coordinator-era compatibility wrappers where they no longer add behavior
- tighten architecture guards so split executors remain the visible owners
- reduce leftover `pipeline-coordinator` compatibility assumptions in runtime tests and state buckets

Exit criteria:
- coordinator compatibility code is thin, obviously legacy, and shrinking
- architecture tests protect the new ownership model

## Phase 7: Push Toward Real Genericity
Goal:
- move from “platformized Empire runtime” toward “generic contract-driven runtime”

Tasks:
- reduce workflow/product-shaped assumptions in generic runtime code
- keep product-specific logic behind module hooks/policies
- avoid hardcoding node-specific semantics where contract-driven or module-driven execution is possible
- continue narrowing compatibility buckets and workflow-specific special cases

Exit criteria:
- generic runtime depends more on contracts and injected module behavior than on Empire-shaped control flow
- adding a second product/workflow would require much less runtime surgery

## Phase 8: Final Audit And Rebaseline
Goal:
- freeze the `v2.2.1` baseline and document remaining intentional exceptions

Tasks:
- re-audit:
  - handler coverage
  - node ownership
  - timer ownership
  - workflow state ownership
  - scoring architecture
- record any deliberate exceptions that remain
- update architecture docs / handoff docs for the next phase

Exit criteria:
- `v2.2.1` is not just green, but explained
- the remaining genericity gap is explicit and bounded

## Acceptance Gates
Run after each phase:

```bash
go test ./internal/runtime -run TestContractCompliance -count=1
go test ./internal/runtime/pipeline -count=1
go test ./internal/runtime -count=1
go test ./... -count=1
```

## Definition Of Done
The work is done when:
- root `contracts/*` are `v2.2.1`
- compliance gates are green without legacy ownership assumptions
- the 5-node runtime model is the real architecture, including scoring or an explicitly documented scoring exception
- `workflow_instances` is the dominant orchestration source of truth
- schema enforcement is real, not just parsed metadata
- remaining product logic is mostly injected/module-owned, not embedded in generic runtime structure
