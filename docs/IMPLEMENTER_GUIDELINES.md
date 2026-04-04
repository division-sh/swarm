# Implementer Guidelines

This document captures implementation rules that should be applied consistently across the codebase.

These are not optional style preferences. They are execution rules intended to reduce drift, wasted effort, and architectural debt.

They are especially important when multiple implementers are working in parallel.

Operational companion:

- apply [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md) before merging non-trivial changes

The default bias of this codebase should be:

- architecture purity over convenience patches
- elegance and clarity over speed of implementation
- explicit semantics over inferred behavior
- one shared model over many local interpretations
- clean migration over compatibility clutter
- core/runtime generality over product-specific leakage
- platform/spec authority over local implementer interpretation

## Platform Spec Authority

The platform spec is the authoritative source for platform/runtime semantics.

Primary authority:

- `docs/specs/swarm-platform/platform/contracts/platform-spec.yaml`

Supporting authority:

- the related platform docs and implementation plans in `docs/`
- only insofar as they clarify or sequence work without contradicting the platform spec

Default rule:

- if runtime behavior, contracts, or refactors touch platform semantics, implementers must treat the platform spec as the source of truth

Practical implication:

- do not invent local semantic rules because the current implementation happens to work that way
- do not preserve a runtime quirk if it conflicts with the spec
- do not resolve ambiguity by choosing the easiest implementation path

If code and spec disagree:

- stop and make the disagreement explicit
- determine whether:
  - the implementation is wrong and should be fixed to match the spec
  - or the spec is incomplete/wrong and needs an explicit update
- do not silently patch around the mismatch

If semantics are unclear or there may be a spec gap:

- stop and ask
- do not infer the intended platform behavior locally
- do not “fill in” the missing semantics with the easiest implementation
- treat uncertainty itself as a design/specification issue that must be resolved explicitly

Exception standard:

- if a temporary deviation from the spec is unavoidable, it must be documented explicitly in:
  - code
  - tests
  - and the relevant issue/plan/watchlist documents

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

Absolute rule:

- do not knowingly ship two semantically meaningful implementations of the same concept in different layers
- if two implementations already exist, consolidation is usually higher-value than adding a third call site

DRY expectation:

- if the same semantic rule appears in more than one place, assume the code is drifting unless there is a clearly documented canonical owner
- prefer one clean shared abstraction over repeated local copies with tiny variations

### 4. Fail closed on invalid configuration or ambiguous semantics

Silent normalization is often worse than explicit failure.

Default rule:

- invalid mode, invalid identity, ambiguous routing, or conflicting ownership should fail explicitly unless there is a deliberate, documented reason not to

Practical implication:

- do not silently collapse unknown values into a default behavior when that changes semantics
- boot should fail on ambiguous routing rather than guess
- runtime should reject invalid control inputs rather than reinterpret them quietly

Anti-patterns to avoid:

- permissive fallback after semantic failure
- “try one meaning, then reinterpret as another”
- ambiguous normalization that changes runtime behavior without surfacing an error
- complicated branching trees where semantics depend on which path happened to match first

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

### 6. Do not let product-specific behavior leak into core runtime layers

Core/runtime layers should stay generic unless a product-specific rule is explicitly part of the runtime contract.

Core/runtime layers include:

- runtime core helpers
- pipeline
- bus
- engine
- manager
- tools
- boot verification
- persistence/store compatibility logic

Default rule:

- if a behavior exists only because of one product/workflow/prompt/dashboard convention, keep that behavior at the product boundary rather than embedding it into shared runtime semantics

Examples of product leakage to avoid:

- one-off prompt or workflow naming conventions encoded in runtime logic
- product-specific allowlists hardcoded in generic subsystems
- dashboard-specific interpretation changing runtime persistence shape
- Empire-specific assumptions baked into generic routing/authorization/state code

Practical implication:

- ask:
  - “is this truly a platform/runtime rule?”
  - or:
  - “is this a product/workflow/app convention?”
- if it is product-specific, keep it out of shared runtime code unless there is a deliberate platform contract update

Exception standard:

- if product behavior must become a platform behavior, make that promotion explicit in:
  - code
  - tests
  - and the relevant spec/plan docs

### 7. Keep persistence compatibility behind an explicit boundary

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

### 8. Separate semantic identity from storage or transport representation

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

### 9. Treat divergent logic as a code smell, not a normal implementation detail

When two layers make the same decision by different code paths, assume divergence risk immediately.

Default rule:

- if gateway, executor, validator, store, dashboard, boot verifier, or test harness each have their own version of a rule, treat that as a likely architecture problem

Examples:

- one layer decides visibility while another decides callability differently
- one layer localizes names while another compares raw names
- one layer reconstructs meaning from storage while another uses typed state

Practical implication:

- prefer deleting duplicate decision code over refining it in place
- if divergence remains temporarily, document:
  - which implementation is canonical
  - which one is transitional
  - and the exit plan

### 10. Prefer elegant control flow over intricate branching

Complicated branching logic is often a sign that the model is wrong or incomplete.

Default rule:

- if a function needs many conditionals, nested switches, or layered special cases to express core semantics, stop and look for the missing abstraction

Why:

- branch-heavy code is hard to reason about
- branch ordering often becomes hidden semantics
- special-case trees are where fallback behavior and semantic drift accumulate

Practical implication:

- prefer:
  - explicit typed descriptors
  - table-driven policy
  - smaller focused functions
  - one canonical decision point
- avoid:
  - long if/else chains for semantic classification
  - duplicated branch trees in different layers
  - “just add one more case” as the default fix

Escalation signal:

- if making a change requires extending an already intricate branch tree, assume there may be an architecture issue to fix instead

### 11. Apply DRY aggressively for semantic logic

DRY is not only about reducing line count.
In this codebase it mainly means:

- one semantic concept
- one owner
- one implementation

Default rule:

- duplicate semantic logic should be treated as debt even if the copies are currently consistent

Practical implication:

- do not copy logic between:
  - gateway and executor
  - runtime and boot verifier
  - runtime and dashboard
  - validator and executor
  - store and runtime helpers
- instead:
  - extract the canonical logic
  - move consumers onto it
  - delete the duplicates

### 12. Optimize for long-term design quality, not short-term implementation speed

The right metric is not “fastest patch landed.”
The right metric is:

- clearer model
- fewer semantic owners
- less future debugging cost

Default rule:

- do not choose a faster local patch when a slightly slower but cleaner architectural change is clearly available

Practical implication:

- it is acceptable to spend more time:
  - introducing a shared abstraction
  - migrating callers
  - deleting transitional logic
- it is not acceptable to save time by introducing:
  - a new heuristic
  - another compatibility branch
  - another private semantic implementation

### 13. Test semantic invariants directly, not only through large end-to-end flows

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

### 14. Keep observability aligned with operator debugging needs

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

### 15. Treat “smelly fallback” code as debt even when tests pass

Passing tests do not legitimize:

- heuristic reinterpretation
- layered fallback chains
- string-based semantic guessing
- duplicate semantic ownership

Default rule:

- if the code smells like it is compensating for a missing abstraction, treat that as debt to remove rather than as a stable pattern to copy

Practical implication:

- when adding a new feature or fix, do not cargo-cult an existing fallback-heavy pattern just because it already exists
- prefer a smaller cleanup to a larger propagation of the smell

### 16. Treat unexpectedly hard test-fixing as an architectural escalation signal

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

## Working Standard For Multiple Implementers

When several implementers are working in the repo, each change should be judged against this question:

- does this make the semantic model more unified, or does it add another local interpretation?

Default expectation:

- new work should reduce semantic owners, not increase them
- if a patch would introduce:
  - a product-specific exception
  - a compatibility branch
  - a duplicate decision path
  - a heuristic fallback
  - a second interpretation of a core concept
- stop and redesign before merging
