# Implementer Guidelines

This document captures implementation rules that should be applied consistently across the codebase.

These are not optional style preferences. They are execution rules intended to reduce drift, wasted effort, and architectural debt.

## Rules

### 1. Be aggressive on migrations and legacy removal

The system is still early.

Default rule:

- prefer removing legacy schema paths, legacy data shapes, legacy compatibility branches, and stale migration shims instead of preserving them indefinitely

Why:

- long-lived fallback paths increase architectural drift
- compatibility code makes runtime behavior harder to reason about
- speculative legacy support consumes debugging and implementation time
- current product stage does not require maximizing historical data retention at the cost of code quality

Practical implication:

- if a schema or data shape is obsolete, remove it
- if a migration can rewrite data forward cleanly, do that instead of carrying dual semantics
- do not add new fallback branches unless there is a clearly justified operational need
- when choosing between:
  - a clean migration plus code deletion
  - preserving legacy runtime behavior
- prefer the clean migration plus code deletion

Bias:

- optimize for present correctness and architectural simplicity
- do not optimize for hypothetical long-tail backwards compatibility unless explicitly required

Exception standard:

- only preserve a legacy path when there is a concrete, current operational dependency
- if that dependency exists, document it explicitly near the code and in the relevant planning/watchlist document

### 2. Do not use heuristic fallback for core semantics

For core runtime semantics, fallback heuristics are usually architecture debt, not resilience.

Core semantics include:

- identity
- routing
- ownership
- authorization
- state targeting
- scope resolution
- expression evaluation

Default rule:

- if the runtime needs to know what something means, encode that meaning explicitly
- do not infer it from:
  - string prefixes
  - naming accidents
  - field presence alone
  - “best guess” fallback branches

Practical implication:

- avoid logic like:
  - “if this fails, try interpreting it as something else”
  - “if the path looks like X, treat it as Y”
  - “if metadata exists, assume this is a child-flow context”
- if the model cannot represent the case cleanly, improve the model

### 3. Centralize semantics; do not re-implement them in multiple layers

When one concept matters to multiple subsystems, it should have one canonical implementation.

Default rule:

- routing, identity, authorization, scope, and matching semantics should be defined once and consumed everywhere

Why:

- duplicated semantics drift
- verifier/runtime/store/tooling mismatches are expensive and easy to miss

Practical implication:

- if a subsystem needs “almost the same” logic as another subsystem, stop and evaluate whether it should call the shared implementation instead
- prefer shared helpers, typed descriptors, or compiled semantic artifacts over local re-derivation

### 4. Fail closed on invalid configuration or ambiguous semantics

Silent normalization is often worse than explicit failure.

Default rule:

- invalid mode, invalid identity, ambiguous routing, or conflicting ownership should fail explicitly unless there is a deliberate, documented reason not to

Practical implication:

- do not silently collapse unknown values into a default behavior when that changes semantics
- boot should fail on ambiguous routing rather than guess
- runtime should reject invalid control inputs rather than reinterpret them quietly

### 5. Prefer typed runtime descriptors over opaque JSON for control-plane semantics

Opaque JSON payloads are acceptable for extensible payloads.
They are not a good source of truth for core runtime control semantics.

Default rule:

- if a field controls runtime behavior, prefer a typed field/descriptor over repeated JSON decoding

Examples of control-plane semantics:

- flow ownership
- subscriptions
- manager fallback
- runtime mode
- scope
- workspace behavior

Practical implication:

- separate:
  - arbitrary config payload
  - runtime-owned typed semantics
- do not let multiple subsystems each “pull out what they need” from raw JSON blobs

### 6. Keep persistence compatibility behind an explicit boundary

Schema compatibility should be a deliberate architectural boundary, not a scattered runtime behavior.

Default rule:

- store capability/version handling should happen in one explicit place
- hot persistence paths should not guess schema shape by probing columns or matching SQL error strings unless explicitly unavoidable

Practical implication:

- prefer:
  - schema capability descriptors
  - startup negotiation
  - explicit migration steps
- avoid:
  - per-call schema probing
  - substring-based fallback on DB errors

### 7. Separate semantic identity from storage or transport representation

One concept may have multiple representations, but those representations must not become interchangeable by accident.

Default rule:

- name and model distinct concepts separately

Examples:

- semantic flow scope vs concrete flow instance path
- source entity vs target entity
- local event name vs routed event name
- subject lineage vs owning flow entity

Practical implication:

- if two strings look similar but mean different things, they should not share one informal handling path
- use typed helpers/descriptors when the distinction matters operationally

### 8. Test semantic invariants directly, not only through large end-to-end flows

End-to-end tests are necessary but not sufficient.

Default rule:

- any important semantic seam should have focused conformance coverage

Examples:

- ownership
- routing resolution
- identity derivation
- scope handling
- mutation logging
- expression semantics

Practical implication:

- when fixing a subtle bug, add:
  - a focused invariant test
  - and, if useful, an end-to-end regression fixture
- do not rely exclusively on large catalog/E2E fixtures to protect core semantics

### 9. Keep observability aligned with operator debugging needs

If a bug takes too long to diagnose, that is also an observability failure.

Default rule:

- when runtime decisions are semantically important, record them explicitly

Examples:

- why a route matched
- why a write was denied
- which entity was targeted
- whether a persistence step committed before emit
- which identity/scope form was used

Practical implication:

- prefer structured diagnostics over implied behavior reconstructed from multiple tables/logs
- if debugging required manual reconstruction, consider that a gap to close

### 10. Treat unexpectedly hard test-fixing as an architectural escalation signal

When tests are difficult to fix, assume there may be a deeper architectural issue rather than only a local test problem.

Default rule:

- if fixing or stabilizing tests starts taking disproportionately long, stop and explicitly evaluate whether the difficulty is exposing:
  - distributed semantics
  - hidden coupling
  - fallback logic
  - duplicated models
  - missing abstractions

Practical implication:

- do not stay in “just patch the tests” mode for too long
- if multiple tests fail for different-looking reasons after a seemingly local change, treat that as a smell
- if restoring green requires understanding several adjacent subsystems, escalate from test-fix to architectural review

Escalation standard:

- name the likely architectural seam explicitly
- record it in the watchlist if it is real
- prefer fixing the shared model over repeatedly repairing downstream tests
