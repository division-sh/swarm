# Flow Instance Identity Implementation Plan

## Goal

Make flow-instance identity, semantic flow scope, entity targeting, and subject/gate projection use one coherent runtime model instead of loosely-coupled string rules.

This initiative is about removing a recurring class of bugs, not just fixing one failing path.

## Non-Negotiable Design Rule

No exception matrix and no heuristic fallback path.

This work is only successful if the runtime ends up with:

- one explicit model
- one explicit decision path
- one explicit meaning for each identity/scope concept

It is not acceptable to "fix" this by adding more special cases such as:

- if root do one thing, else fallback
- if child path contains X then maybe retarget
- if projection misses by scope key then try instance path

Those mechanisms are exactly how the current brittleness accumulated.

The correct shape is:

- typed concepts
- explicit invariants
- shared helpers
- conformance tests

If a case does not fit the model cleanly, the model needs to improve. We should not patch around it with another exception.

## Recursive Requirement

The model must work for arbitrary nesting depth, not just:

- root
- child
- grandchild

The runtime should satisfy the same semantics for:

- depth 1
- depth 2
- depth N

Examples:

- descendant completion retargeting must compose correctly through any number of intermediate flows
- semantic flow scope must remain distinct from concrete instance path at every depth
- gate and subject projection must not assume a fixed maximum nesting depth

Concrete implication:

- example-based tests for child and grandchild are useful
- but they are not sufficient as the stopping condition
- this workstream also needs recursive or parameterized conformance coverage for deeper chains

## Why This Work Exists

Recent debugging exposed the same architectural smell repeatedly:

- event identity got more centralized, but identity semantics overall are still spread across:
  - event localization/externalization
  - entity targeting
  - child/parent retargeting
  - gate projection
  - boot boundary checks
- some paths reason about semantic flow scope:
  - `child`
- while others reason about concrete flow instance paths:
  - `child/<instance-id>`
- once one seam is corrected, another nearby seam can fail because it was only passing accidentally under the old assumptions

Examples already seen:

- top-level flow outputs incorrectly retargeted to the parent/root entity
- child-flow gates stored as `child/g_validated` but projected using `child/<instance-id>/...`
- root outputs treated differently from flow outputs in boot verification
- cross-flow handler lookup succeeding at routing time but failing at local handler resolution time

## Core Runtime Model

The runtime should reason with these concepts explicitly:

- `flow_template_id`
  - logical flow id from contracts
  - example: `child`
- `flow_scope_key`
  - semantic scope used for flow-local namespacing
  - example: `child`
- `flow_instance_path`
  - concrete instance path
  - example: `child/<instance-id>`
- `logical_instance_id`
  - minted instance token
- `entity_id`
  - persisted entity row id for that flow entity
- `parent_entity_id`
  - entity that handed off or owns the next local flow entity
- explicit authored payload/config fields
  - business correlation carried by product-authored data rather than a platform subject-link primitive
- `source_event_id`
  - causal debugging and proof chain across flow-local entities

Key distinction:

- `flow_scope_key` is not the same thing as `flow_instance_path`
- scope keys drive semantic namespacing
- instance paths identify concrete running instances

## Invariants

1. A handler executes against one explicit entity target.
2. `create_entity` creates one new flow-local entity and preserves source linkage explicitly.
3. Parent retargeting only applies for real child-flow output boundaries, not for all flow outputs.
4. Gate projection uses semantic flow scope, never concrete instance path matching.
5. Root outputs and flow outputs use one consistent boundary model.
6. Subject lineage is explicit and preserved across handoffs.
7. The same identity/targeting rules hold for arbitrarily deep nested flow chains.

## Work Phases

### Phase 1: Guardrails

Write focused conformance tests that lock the model down before more refactors:

- scope key vs instance path are distinct
- create_entity mints:
  - logical instance id
  - flow instance path
  - canonical flow entity id
- same-flow instance path is not treated as a descendant retarget
- descendant completion already targeted to parent is not retargeted again
- gate projection uses scope key, not instance path
- root outputs count as output boundaries too
- recursive nested chains preserve the same semantics at depth N, not only at depth 2 or 3

Exit criteria:

- the core identity/scope invariants are encoded in focused tests
- catalog fixtures are not the only protection
- recursion is tested beyond a single hand-written grandchild example

### Phase 2: Flow-Instance Semantics Helper

Introduce a shared helper/module that owns:

- deriving flow scope key
- deriving flow instance path
- deriving canonical flow entity id
- extracting logical instance id
- resolving parent/subject linkage
- distinguishing:
  - same-flow instance path
  - descendant flow path
  - parent-targeted completion

Exit criteria:

- the runtime has one place for flow-instance semantics instead of repeated string logic

### Phase 3: Centralize Entity-Target Resolution

Move handler target-selection rules onto one shared path:

- handler execution entity
- create_entity target
- child-output parent retargeting
- already-parent-targeted descendant completion
- root/no-flow target behavior

Exit criteria:

- entity-target selection no longer depends on multiple ad hoc call sites

### Phase 4: Centralize Subject/Gate Projection

Unify:

- subject projection
- gate projection
- flow-local semantic scope matching

Exit criteria:

- projection uses the same semantics as entity targeting and flow creation

### Phase 5: Cleanup + Verification

- remove leftover ad hoc string-based scope/path checks
- run:
  - focused runtime suites
  - `go test ./internal/runtime/... -count=1`
  - `go test ./... -count=1`
- update watchlist and implementation docs

## Checkpoint With Spec Writer

Checkpoint after Phase 1.

What to confirm then:

- are `flow_scope_key` and `flow_instance_path` the right conceptual split?
- should the spec explicitly say gate projection is keyed by semantic flow scope rather than full instance path?
- do we want the entity-target rules written down more explicitly once the runtime model is stable?

This should be a clarification checkpoint, not a blocker before implementation.

## Initial Execution Order

1. Add or tighten focused tests for the identity/scope invariants.
2. Extract the shared flow-instance semantics helper.
3. Route entity-target resolution through that helper.
4. Route gate/subject projection through the same semantics.
5. Do the spec-writer checkpoint.

## Current Checkpoint

Completed in this pass:

- added focused guardrail coverage for:
  - scope key vs instance path vs canonical entity id
  - create_entity identity shaping
  - depth-safe descendant detection
  - emitted entity target resolution
- added stronger recursive/depth coverage for:
  - exact ownership vs ancestor scopes at depth 1..8
  - semantic scope extraction at depth 1..8
  - nested gate projection/localization using a real nested fixture source
- introduced shared runtime helper:
  - `internal/runtime/core/flowidentity`
- moved these seams onto the shared helper:
  - create-entity identity shaping
  - emit target resolution
  - gate projection scope selection
  - flow-instance activation identity derivation
  - flow-instance deactivation eligibility and exact flow-path teardown
  - same-flow ownership checks for writes
  - nested template route materialization against exact instance paths
  - canonical flow-entity id generation in the workflow instance store
- removed one more class of split identity semantics:
  - instanced flow rows are now stored and addressed with the same canonical flow entity id
- completed the spec-writer checkpoint and aligned the platform spec on:
  - exact semantic-scope ownership
  - boot-time path construction by `flow_scope_key`, not concrete instance path
- closed the final audit findings:
  - root workflow terminal states no longer masquerade as flow-instance teardown requests
  - nested template subscribers are now materialized against the concrete instance path, not the semantic template scope
- kept runtime coverage green after each step

Final verification from this checkpoint:

- `go test ./internal/runtime/... -count=1`
- `go test ./... -count=1`

## Completion Status

Core implementation goal is met.

Why this now qualifies as complete:

- semantically important flow-instance identity paths are routed through the shared model
- semantic flow scope and concrete instance path are no longer treated as interchangeable in the audited runtime seams
- entity targeting, activation/deactivation, ownership, gate projection, and nested template route materialization now use the same identity model
- recursive coverage exists beyond hand-written child/grandchild examples
- the spec was clarified to match the implemented model
- the full repository test suite is green from this baseline

Non-blocking follow-up still worth doing later:

- add more catalog/conformance fixtures for subject lineage and deep nested compositions
- keep folding lower-value helper wrappers into `core/flowidentity` when touching nearby code
- convert any remaining naming debt (`templateID` as an argument name in some transport surfaces) to terms that better reflect semantic scope

## Definition Of Done

- runtime no longer relies on scattered string heuristics for flow-instance semantics
- semantic flow scope and concrete instance path are treated distinctly everywhere
- entity-target resolution is centralized
- gate/subject projection uses the same model
- focused conformance tests cover the identity/scope seams directly
- recursive nested-flow behavior is covered by tests that go beyond a fixed 2- or 3-level example
- broader runtime and repo test suites stay green
