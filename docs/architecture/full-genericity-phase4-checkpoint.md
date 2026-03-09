# Full Genericity Phase 4 Checkpoint

## Scope
This checkpoint captures the state after Phases 1-3 of [`full-genericity-endgame-plan.md`](/Users/youmew/dev/empireai/docs/architecture/full-genericity-endgame-plan.md) and before Phase 5+ work on compatibility-bucket removal, scoring unification, and declarative node execution.

Dashboard-specific files under `internal/dashboard/**` are intentionally out of scope for the genericity target.

## Current State
- `v2.2.1` contracts are live at root and the full suite is green.
- The `2.2.1` 5-node runtime model is real and tested.
- Generic runtime/shared packages no longer scatter product defaults across:
  - `internal/runtime/**`
  - `internal/factory/**`
  - `internal/commgraph/**`
  - `cmd/empire/**`
- Remaining Empire references in non-test Go files are now concentrated in explicit product/wire surfaces, not generic shared logic.

## Remaining Non-Test Empire Surfaces

### 1. Product Policy
These are intentional product-specific packages and are acceptable at this phase.

- [`internal/runtime/productpolicy/empire/policy.go`](/Users/youmew/dev/empireai/internal/runtime/productpolicy/empire/policy.go)
  - owns Empire-specific control-plane behavior
  - owns Empire-specific emit normalization and transition guardrails
  - owns Empire-specific role classification and fallback agent identity

- [`internal/commgraph/empire/policy.go`](/Users/youmew/dev/empireai/internal/commgraph/empire/policy.go)
  - owns Empire-specific message authority rules
  - owns Empire-specific mailbox round-trip topology

Assessment:
- These are not generic-runtime leaks anymore.
- They are explicit product packages and represent the current intended boundary.

### 2. Wire Compatibility
This is explicit protocol debt, not shared-runtime policy debt.

- [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)
  - still exposes `X-Empire-*` header names and `empire_*` query keys
  - generic callers now depend on neutral constant names, but the wire format itself is still Empire-branded

Assessment:
- This is acceptable for now.
- Renaming the wire protocol is a compatibility migration, not a platformization precondition.
- It should be handled as a later protocol-versioning effort, not mixed into generic-runtime refactoring.

### 3. Excluded Product App Surface
- [`internal/dashboard/**`](/Users/youmew/dev/empireai/internal/dashboard)

Assessment:
- Dashboard remains intentionally product-shaped.
- It should not block generic-runtime progress.

## Audit Result
At this checkpoint, there is no obvious remaining shared-logic debt in non-test Go files outside:
- explicit product policy packages
- explicit wire-compat protocol constants
- dashboard code, which is out of scope

That means Phases 1-4 have done their job:
- the generic/shared runtime no longer quietly depends on Empire defaults
- the remaining product behavior is visible and isolated

## What This Means For Later Phases

### Phase 5
Can now focus cleanly on removing compatibility buckets from live runtime decisions without simultaneously untangling product-default leakage.

### Phase 6
Can focus on contract-shaped restore state rather than product-boundary cleanup.

### Phase 7
Can focus on scoring unification as an architectural exception rather than a product-leak problem.

### Phase 11
Declarative node execution can now be designed against a cleaner platform boundary, because product policy is already concentrated in module/policy packages.

## Spec Writer Implications
No immediate spec correction is required just to preserve the Phase 1-4 genericity boundary.

The useful spec question for later phases is different:
- what contract surface should exist so that product policy currently implemented in:
  - [`internal/runtime/productpolicy/empire/policy.go`](/Users/youmew/dev/empireai/internal/runtime/productpolicy/empire/policy.go)
  - [`internal/commgraph/empire/policy.go`](/Users/youmew/dev/empireai/internal/commgraph/empire/policy.go)
  can be expressed as contract/config data instead of code

Concretely, later genericity phases would benefit from contract/config support for:
- control-plane role identity
- message-authority topology
- mailbox round-trip policy
- directive/scan emission policy
- budget-threshold remediation policy
- protocol/header versioning if wire de-branding is ever desired

## Recommended Next Step
Proceed to Phase 5 of [`full-genericity-endgame-plan.md`](/Users/youmew/dev/empireai/docs/architecture/full-genericity-endgame-plan.md):
- remove compatibility buckets from live runtime decisions
- keep product policy isolated
- do not mix protocol-header renaming into the next platformization slice
