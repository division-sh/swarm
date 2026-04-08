# Implementer Guidelines

This document captures implementation rules that should be applied consistently across the codebase.

These are not optional style preferences. They are execution rules intended to reduce drift, wasted effort, and architectural debt.

They are especially important when multiple implementers are working in parallel.

Operational companion:

- apply [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md) before merging non-trivial changes
- apply [SEMANTIC_DRIFT.md](/Users/youmew/dev/swarm/docs/SEMANTIC_DRIFT.md) when a change touches semantic ownership, identity, validation, lifecycle, or cross-surface parity
- use [PROMPT_TEMPLATES.md](/Users/youmew/dev/swarm/docs/PROMPT_TEMPLATES.md) for the default implementer and reviewer prompt shapes
- use [PROCESS_CHECKLIST_TEMPLATES.md](/Users/youmew/dev/swarm/docs/PROCESS_CHECKLIST_TEMPLATES.md) when you need short copy-paste templates for pre-audits, gate decisions, follow-up decisions, or parent-state updates

The default bias of this codebase should be:

- architecture purity over convenience patches
- elegance and clarity over speed of implementation
- explicit semantics over inferred behavior
- one shared model over many local interpretations
- clean migration over compatibility clutter
- core/runtime generality over product-specific leakage
- platform/spec authority over local implementer interpretation

Operational policy:

- fail fast when a semantic/runtime issue appears
- prefer boot/runtime failure over permissive fallback
- aggressively migrate forward to the canonical shape
- preserve zero legacy behavior by default
- do not add compatibility seams unless the lead explicitly approves that exact seam in writing

## Process Summary

Default process:

1. Issue is created
- the issue writer should frame the broadest failure class they can honestly defend
- the issue only needs to be good enough to assign, not fully pre-analyzed

2. Lead or triage assigns category
- category examples:
  - local
  - failure-class
  - parity
  - semantic-drift
  - high-risk maintenance
- the category determines whether stricter proof and gate requirements apply
- the issue writer may suggest a category
- the implementer may escalate the category
- the reviewer may reclassify it later if the original categorization was too light
- at issue intake or assignment time, the issue should also be mapped to an existing watchlist node when possible
- if no suitable watchlist node exists, that gap should be called out so the pre-audit can create or refine the node rather than proceeding without semantic map context

3. Pre-audit happens before coding
- this is the real gate for starting implementation
- the implementer must test whether the issue framing is too narrow
- the implementer must not let the observed failing step define the failure class by itself
- the implementer must identify the broadest plausible failure class
- the implementer must explicitly state the chosen working failure class for the PR, as broad as possible while still naming one coherent owner and one honest implementation scope
- if a broader parent failure class exists above the chosen working failure class, the implementer must state that parent class explicitly
- the implementer must explicitly state the class chain:
  - observed symptom
  - chosen working failure class
  - immediate parent failure class
  - broadest plausible parent failure class
  - and the reason the class stops where it stops
- the implementer must identify the canonical owner(s), the repo-wide consumer set, the manifestation table, and the intended closure level
- after identifying the parent failure class, the implementer must probe sibling seams under that parent strongly enough to assess whether the parent is still live, already clean, a different class, or still unproven
- if the issue concerns a multi-step user-visible flow, the implementer must enumerate the full relevant execution path in order before choosing the working class
  - include earlier gates on that path such as startup, readiness, auth, routing, and action endpoints when they exist
  - for each gate on that path, classify it as exactly one of:
    - same chosen class
    - different semantic concept, with proof
    - explicitly split / tracked separately
- a PR may take a child slice of a broader parent failure class, but once the working failure class for the PR is chosen, the PR must aim to eliminate that chosen class entirely rather than only improve one manifestation inside it
- immediately after the pre-audit, before implementation starts, the implementer must make an explicit action decision for the parent failure class:
  - absorb the parent class now
  - keep first-slice scope
  - open or update a dedicated follow-up stream
  - or leave the parent explicitly open as still unproven
- when making that parent-class action decision, the implementer and reviewer must consult the mapped watchlist node as active evidence, not just as bookkeeping:
  - verify what broader parent class is already tracked there
  - verify whether the node already suggests additional live sibling manifestations or consumer families beyond the proposed slice
  - if the watchlist evidence makes broader parent closure the honest default, promote scope before coding rather than leaving that pressure hidden
- the implementer must also identify which watchlist node the work belongs to, or create/refine one if the existing watchlist does not capture the failure class cleanly
- the watchlist should be treated as a semantic trie / failure-class map, not as passive notes
- for failure-class, parity, semantic-drift, and similar high-risk semantic work, the main effort is expected to go into manifestation identification and classification before coding starts
- that is normal and desirable
- the expensive work is identifying the true failure class, enumerating the manifestation set as extensively as possible, and deciding which manifestations are same-class versus separate-class before code narrows the thinking
- the pre-audit is the vehicle for that work
- if that effort feels disproportionate, the usual answer is to extract a cleaner slice after the audit, not to make the audit shallower

4. Independent pre-audit review gate applies to risky categories
- required by default for:
  - failure-class work
  - parity work
  - semantic-drift / canonical-owner work
  - other high-risk semantic work likely to have multiple manifestations or consumers
- the gate is lightweight and checks whether the pre-audit is honest enough to start coding
- the independent reviewer or lead who evaluates the pre-audit must record the gate outcome explicitly on the issue thread before coding starts
- if the gate outcome is not explicitly recorded on the issue thread, the gate is not yet satisfied
- valid outcomes:
  - pre-audit approved
  - pre-audit approved as first slice
  - pre-audit insufficient; widen class
  - pre-audit insufficient; escalate broad refactor

Default closure rule:

- the default option is always complete closure of the broadest class that can honestly be closed in the PR
- staged follow-up is an exception, not the baseline
- `pre-audit approved as first slice` must be justified against the default of complete closure
- if reviewer/implementer are unsure whether full closure is feasible, the bias is to pressure-test wider remediation first, not to assume follow-up is acceptable
- do not choose a staged slice merely because:
  - the local seam is nearby
  - the patch is smaller
  - the rollout feels easier
  - follow-up seems convenient
  - the broader class would require more thought

Meaning of `pre-audit approved as first slice`:

- this is an exception to the default rule of complete closure, not a neutral option
- use this only when all of the following are true:
  - a broader parent failure class exists above the chosen working failure class
  - the chosen working failure class is still a real coherent class boundary, not only a local seam or one manifestation
  - sibling probing under the parent was actually performed
  - the mapped watchlist node was checked as active evidence when deciding whether broader parent closure should be required now
  - the parent-class action decision is explicit
  - the parent-class tracking record is independently verified and valid
  - the PR can still honestly aim to eliminate its chosen working failure class entirely
- this approval means:
  - implementation may proceed on the narrower chosen class
  - the broader parent class is not being claimed closed
  - the parent remains explicitly open, superseded, or otherwise durably tracked
  - the default expected closure level is `touched seam canonicalized`, not `failure class eliminated`
- do not use `approved as first slice` when:
  - the chosen class is really only one manifestation inside itself
  - sibling probing was shallow or missing
  - the parent tracker is stale, closed without proof, missing, or ambiguous
  - the parent is small enough and actively broken enough that full parent remediation is the honest default
  - the narrower scope exists only because the implementer started near one file/helper and never pressure-tested the broader class

When to push back and require complete remediation instead of approving a first slice:

- start from the presumption that complete remediation is the right answer
- push back and ask for full parent-class remediation when any of the following are true:
  - the parent failure class is small enough to close honestly in one PR
  - multiple sibling seams under the parent are already known broken and live in the same owner cluster
  - the chosen narrow class is not a real semantic class boundary
  - the proposed PR would only improve one manifestation inside its own chosen class
  - parent tracking is invalid or missing, so a first-slice story would strand the remaining obligation
  - leaving the siblings behind would preserve live same-concept interpreters in a way that keeps the semantic drift active
  - the cost/risk of doing the parent now is lower than the process and architecture cost of creating another staged slice
- in those cases, the honest gate outcomes are usually:
  - `pre-audit insufficient; widen class`
  - or `pre-audit insufficient; escalate broad refactor`

5. Implementation starts only after the pre-audit gate is satisfied
- if broad duplication or new sibling manifestations are discovered while coding, stop and escalate rather than silently narrowing the class
- if sibling probing shows the broader parent failure class is actively broken in multiple same-class seams, prefer tackling the parent failure class directly in the PR when feasible
- when deciding whether to promote from child-slice to broader parent closure, explicitly re-check the watchlist node for:
  - the currently tracked broader class
  - any sibling manifestations or consumer families already named there
  - whether the watchlist evidence makes broader parent closure the honest default
- if closing the parent class is not feasible, the pre-audit and PR proof audit must say so explicitly and keep the remaining parent-class obligation tracked in the issue/watchlist record
- if a concrete remaining same-class seam is discovered during sibling probing and is not being absorbed now, create or update the follow-up issue/stream immediately rather than leaving it as narrative-only review debt
- if the implementer smells a deeper architecture or type-model issue while doing the pre-audit or implementation, they should state it explicitly even if the current PR remains narrowly scoped
- architecture feedback is encouraged in both pre-audit and post-audit when it helps explain:
  - why the local failure class keeps recurring
  - where the current type model or ownership model is weak
  - what larger refactor direction would reduce long-run semantic drift
- this architecture feedback does not by itself widen scope, but it must not be suppressed just because the current PR is a first slice
- if that architecture feedback is concrete enough to name an honest long-run obligation, create or update a tracked issue/stream instead of leaving it only in audit prose
- if the architecture feedback is interesting but not yet concrete enough to assign, record it explicitly as watchlist refinement or residual risk rather than letting it disappear

6. PR proof audit happens before merge
- this is the real gate for calling the work complete
- the PR proof audit must state the achieved closure level and provide manifestation-level proof for that claim

7. Review checks the closure claim against the proof
- do not approve because the touched seam is cleaner
- approve only when the achieved closure level is explicit and actually supported by the proof

8. Watchlist state is updated when the semantic map changed
- every failure-class, parity, semantic-drift, or similar high-risk semantic review must explicitly decide one of:
  - no watchlist change needed
  - existing node refined
  - new manifestation added to existing node
  - new watchlist branch/node created
- update the watchlist during pre-audit if the class map is missing or too weak
- update it again during review close-out or after merge if the work changed the semantic map, closed a node, split a node, or revealed a missed sibling manifestation
- after any PR that closes only a child slice of a broader parent class, update the parent issue/watchlist record to say:
  - what child failure class was closed
  - what broader parent class still remains open or unproven
  - what next same-class stream or sibling seam is already known, if any
- do not leave a meaningful watchlist refinement as chat-only commentary

9. Follow-up creation happens when remaining work is concrete enough to assign
- do not leave a concrete remaining same-class seam only in review prose, residual-risk notes, or chat
- create or update a tracked follow-up immediately when:
  - a concrete same-class seam remains live and the PR is not absorbing it
  - sibling probing shows the broader parent class is still live in another seam
  - review discovers a broader class than the issue/PR was using
  - a temporary seam or staged plan needs an explicit removal target
  - a worthwhile follow-up is specific enough that another implementer could pick it up honestly
- update an existing issue/stream when:
  - the new finding is still part of the same parent class
  - the remaining work belongs to an already-open umbrella/watchpoint
  - the remaining obligation is refinement of an already-tracked parent issue
- create a new issue when:
  - the remaining work is a new concrete child slice
  - the remaining work is a different class
  - the parent issue would become too muddy if the concrete slice were left only there
- watchlist-only is acceptable when the concern is reusable but not yet concrete enough to assign
- residual-risk-only is acceptable only when the concern is still too weak to name a concrete seam or honest assignable obligation
- if the seam, class, and remaining obligation can be named clearly, track it now

Default role split:

- issue writer:
  - should frame the broad failure class when possible
  - should include known reproducers, sibling manifestations, and spec refs when known
- implementer:
  - must verify or correct the issue framing in the pre-audit
  - must not start coding until that framing is reviewable
- reviewer:
  - must independently test whether the framing is still too narrow
  - must review adversarially at every layer: pre-audit, implementation, and post-audit
  - must actively try to falsify parent-class, follow-up, closure, and "already tracked by X" claims before accepting them
  - must record the independent pre-audit gate outcome on the issue thread when the category requires that gate
  - must not let seam-level improvement be presented as failure-class elimination without proof
  - must also verify that the watchlist mapping/update decision is explicit when the work touches a failure-class family

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

For in-flight semantic changes:

- do not update the authoritative `platform-spec.yaml` on `main` before the matching implementation exists
- if a workstream needs a proposed semantic change, use a review-spec draft file first
- only update the authoritative `platform-spec.yaml` in the same branch that makes the semantic change true in code/tests

Spec-update workflow when implementation is blocked by a spec gap:

- stop implementation immediately when the issue depends on semantics the current spec does not define clearly enough
- draft the proposed semantic change first in a review-spec file under:
  - `docs/specs/swarm-platform/platform/contracts/review/`
- review that draft as a spec decision before resuming implementation
- once the draft is approved, copy the approved draft into the implementer's worktree before handoff
- the implementation PR must then do both in the same branch:
  - promote the approved draft into the authoritative `platform-spec.yaml`
  - implement the matching runtime/store/code change
- do not implement against a draft spec without promoting it in the same PR
- do not promote the draft into the authoritative spec without the matching implementation in the same PR
- the implementer should claim touched-seam compliance with the newly promoted authoritative spec, not total platform compliance

Rule:

- approved review-spec drafts are not optional background reading
- when a draft is the basis for implementation, that exact approved draft must be the source promoted into the authoritative spec on the implementation branch

Default review-spec convention:

- draft files live in a dedicated local-only review directory
- name them with a workstream-specific suffix, for example:
  - `docs/specs/swarm-platform/platform/contracts/review/platform-spec.session-audit-split.yaml`
  - `docs/specs/swarm-platform/platform/contracts/review/platform-spec.expression-model.yaml`

Rule:

- `platform-spec.yaml` must describe the semantics that are actually true on `main`
- review-spec drafts may describe proposed future semantics, but they are not authoritative until merged with the implementation
- review-spec drafts are local working files and should stay gitignored until their contents are ready to be copied into the authoritative YAML on an implementation branch

Exception standard:

- temporary deviation from the spec is not an implementer choice
- if a deviation is believed unavoidable, stop and escalate to the lead/spec writer
- do not merge a speculative local deviation just because it seems operationally convenient
- any approved deviation must be:
  - explicitly approved in writing
  - documented in code
  - documented in tests
  - documented in the relevant issue/plan/watchlist documents

## Rules

### 1. Be aggressive on migrations and legacy removal

The system is still early.

Default rule:

- prefer removing legacy schema paths, legacy data shapes, legacy compatibility branches, and stale migration shims instead of preserving them indefinitely
- default to zero legacy behavior
- if a clean forward migration exists, take it and delete the old path

Why:

- long-lived fallback paths increase architectural drift
- compatibility code makes runtime behavior harder to reason about
- speculative legacy support consumes debugging and implementation time
- current product stage does not require maximizing historical data retention at the cost of code quality

Practical implication:

- if a schema or data shape is obsolete, remove it
- if a migration can rewrite data forward cleanly, do that instead of carrying dual semantics
- do not add new fallback branches
- do not add backward-compatibility read-path or write-path shims
- do not preserve older persisted shapes with new fallback matching just because it is locally convenient
- if old data would stop working under the new canonical shape, that is acceptable unless the lead explicitly requires a temporary migration seam
- when choosing between:
  - a clean migration plus code deletion
  - preserving legacy runtime behavior
- prefer the clean migration plus code deletion

Bias:

- optimize for present correctness and architectural simplicity
- do not optimize for backwards compatibility
- do not preserve old behavior because it was once accepted or because some old data might still exist

Exception standard:

- compatibility is banned by default
- if the lead explicitly requires a temporary migration seam, keep it:
  - narrow
  - time-bounded
  - isolated to one explicit boundary
  - documented as temporary removal work, not normal architecture
- otherwise, prefer canonical-only behavior and fail closed semantics

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
- heuristic behavior is disallowed in semantic/runtime code unless the lead explicitly approves that exact heuristic in writing
- do not infer it from:
  - string prefixes
  - naming accidents
  - field presence alone
  - “best guess” fallback branches
  - partial projections
  - path patterns when a canonical owner exists
  - loose string matching when an explicit contract exists

Practical implication:

- avoid logic like:
  - “if this fails, try interpreting it as something else”
  - “if the path looks like X, treat it as Y”
  - “if metadata exists, assume this is a child-flow context”
- if a heuristic seems necessary, stop and escalate instead of implementing it by default
- if the model cannot represent the case cleanly, improve the model

Absolute rule:

- do not add compatibility heuristics for core semantics
- do not keep an old interpretation alive “just in case”
- if an issue or PR relies on heuristic ownership, heuristic classification, or heuristic fallback without explicit written approval, treat the work as incomplete

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

- invalid mode, invalid identity, ambiguous routing, conflicting ownership, or unexpected startup probe results should fail explicitly
- do not downgrade semantic uncertainty into a warning, fallback, or best-effort pass

Practical implication:

- do not silently collapse unknown values into a default behavior when that changes semantics
- boot should fail on ambiguous routing rather than guess
- runtime should reject invalid control inputs rather than reinterpret them quietly

Anti-patterns to avoid:

- permissive fallback after semantic failure
- “try one meaning, then reinterpret as another”
- ambiguous normalization that changes runtime behavior without surfacing an error
- complicated branching trees where semantics depend on which path happened to match first
- startup or validation checks that pass on unknown errors instead of failing closed

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

### 7. When changing a derived surface, check the adjacent canonical invariant

Read models, operator projections, validation filters, and “summary” surfaces often sit next to a more canonical writer/store/runtime owner.

Default rule:

- do not change a derived selector, projection, or reader in isolation
- explicitly compare it against the adjacent canonical owner that already defines the same semantic boundary

Common examples:

- operator backlog selector vs canonical pending-work selector
- dashboard/read-model lifecycle state vs canonical persisted delivery/session/flow state
- validation filter vs canonical schema/trust-boundary rule
- summary/read surface vs canonical write-time owner

Practical implication:

- ask which existing writer/store/runtime selector already defines this meaning
- prove the touched derived surface stays aligned with it
- if the two differ, either:
  - make them share one owner rule
  - or document the intentional difference explicitly in code, tests, and the PR

Test expectation:

- include at least one counterexample that would pass if the derived surface drifted from the canonical owner
- do not rely only on happy-path tests for the newly emphasized states

Anti-pattern:

- fixing the local issue wording while silently drifting from the broader canonical invariant in the adjacent seam

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

## Implementer Assignment Completion Standard

A coding assignment is not complete when code only exists locally.

Default rule:

- assignment completion requires:
  - code in the implementer's worktree
  - focused tests for the touched seam
  - a commit with an intentional message
  - a pushed branch
  - an open PR against `master`

PR expectations:

- include the correct issue linkage in the PR body:
  - `Closes #...` only when the PR fully completes the tracked obligation for that issue
  - `Part of #...` or `Related to #...` when the issue remains open after this PR
  - never use a closing keyword on a parent or umbrella issue that still has pending work
- include either:
  - exact governing spec references for the touched seam
  - or an explicit statement that the PR is non-semantic maintenance and why no platform spec section governs it
- include a short human summary
- include tests run
- include residual risk
- include any required spec escalation or follow-up issue

Issue and PR spec-reference rule:

- every implementation issue should cite the exact governing spec section(s) for the touched semantic seam
- every implementation PR should repeat those exact spec references in the PR body
- if the change is non-semantic maintenance, the PR may explicitly say that no platform spec section governs it and justify that claim
- if a semantic/runtime seam has no exact spec section, the issue and PR must say so explicitly and treat the work as a spec-gap / ambiguity escalation rather than normal implementation
- if implementation reveals that the current code already violates the cited spec and that drift is not already captured in the issue, stop and escalate instead of silently absorbing the mismatch into the current change
- do not rely on issue prose alone when the platform spec already defines the semantics

Issue readiness vs implementation readiness rule:

- an implementation issue does not need to arrive fully pre-analyzed before assignment
- the issue must be good enough to assign:
  - intended failure class if known
  - reproducer if known
  - governing spec refs if they exist
  - any already-known sibling manifestation
- the `Pre-Implementation Coverage Audit` is the hard gate for starting code
- if the issue is symptom-shaped or under-analyzed, the implementer must produce the missing canonical-owner mapping, repo-wide consumer sweep, manifestation table, and closure target in the pre-audit before implementation begins
- do not delay assignment waiting for fake completeness in the issue body
- do not begin implementation until the pre-audit makes the seam and intended closure level reviewable
- every failure-class / parity / semantic-drift / high-risk semantic issue must either map to an existing watchlist node or create/refine one during pre-audit
- if the watchlist has a relevant node, the pre-audit must use it to widen the sweep rather than ignoring it
- if no watchlist node exists, the pre-audit should say so explicitly and create or refine the node as part of class clarification

Pre-implementation rule:

- before writing code, re-read the exact spec section(s) cited in the issue
- treat those cited sections as the binding semantic contract for the work
- if the current code, tests, or issue wording disagree with the cited spec, stop and escalate before implementation
- do not rely on memory of the spec or on the issue summary alone for semantic changes

Symptom vs failure-class rule:

- this rule applies to semantic, runtime, loader, bootverify, persistence, read-model, and other spec-governed work
- it does not require the same broad semantic reassessment for purely non-semantic maintenance, mechanical cleanup, or docs-only work
- assume the reported issue may be only the first visible symptom of a broader semantic defect
- before writing code, take a step back and reassess the full semantic rule the issue belongs to
- inspect adjacent logic in the same seam to determine the true scope of the problem, not just the first failing example
- use the cited spec section(s) to decide whether sibling contexts should behave the same way
- if the issue appears narrower than the actual failure class, stop and escalate or update the issue before treating the work as complete
- do not stop at fixing the first symptom if adjacent logic is governed by the same semantic rule

Narrow patch vs broad semantic proof rule:

- for semantic/runtime/spec-governed work, keep the implementation patch narrow but do not keep the semantic assessment narrow
- after reading the cited spec, assess the full rule family in the touched seam, not only the first failing example
- inspect adjacent logic that is governed by the same semantic rule
- design tests to cover the rule in that area, including adjacent valid, invalid, mixed-case, malformed, rollback, or fail-closed paths where relevant
- do not treat a narrow code diff as sufficient proof if the touched semantic rule spans a broader area

Generic proof and self-audit rule:

- for semantic/runtime/spec-governed work, a PR is not review-ready until the implementer posts a self-audit comment on the PR
- that self-audit must explicitly include:
  - the broader failure class the issue belongs to
  - whether the issue described the full class or only the first visible symptom
  - which sibling contexts in the touched seam were checked
  - the generic reproducer, fixture, or focused failing proof used to capture the failure class
  - or, if none existed, the generic proof created as part of the work
- do not rely on “tests pass” as sufficient proof without naming the generic failing proof that the fix resolves
- if no generic reproducer exists yet for a semantic/runtime failure class, deriving one is part of the implementation task
- if the implementer cannot derive a clean generic proof for the failure class, stop and escalate before treating the work as complete

Minimum spec-reference block:

```text
Spec references:
- docs/specs/swarm-platform/platform/contracts/platform-spec.yaml: <section/path>
- governing rule: <short plain-language statement>

or

Spec references:
- none; this PR is non-semantic maintenance
- justification: <why no platform spec section governs this change>

If no exact spec reference exists:
- explicit ambiguity / gap:
- escalation owner:
```

Default lead prompt shape:

```text
Pick up issue `#NNN`.

Before coding:
- read the full issue body/thread
- re-read the exact cited spec section(s) and treat them as binding
- if the issue says there are no exact spec refs, treat the issue body/thread as binding context
- follow:
  - `docs/IMPLEMENTER_GUIDELINES.md`
  - `docs/SEMANTIC_DRIFT.md`

Important steering:
- spec is the source of truth
- fail fast, fail closed, aggressive migration, zero legacy behavior
- no heuristics, no compatibility shims, no preserving old behavior “just in case”
- close the semantic gap in the PR if it can be closed cleanly
- do not treat migration as a reason to preserve dual semantic ownership
- do not treat “shared owner introduced” or “same seam” as proof by themselves
- stop and escalate on any spec gap, ambiguity, contradiction, or uncaptured off-spec behavior

Mandatory before implementation:
- post a `Pre-Implementation Coverage Audit` comment on the GitHub issue

That issue comment must state:
- the exact semantic concept or concepts being changed
- every relevant canonical owner for those concepts
- the touched consumers of each owner
- an exhaustive systematic-consumption audit for every currently known sibling seam that should consume each owner
- the broader failure class
- every currently known manifestation in scope
- the exact proof planned for each manifestation
- required supported-surface / end-to-end proof if this is a parity issue
- any blocking ambiguity or split condition

If the pre-audit shows broad duplication and a narrow fix would be dishonest:
- pause
- post a `Broad Refactor Escalation`
- wait for explicit lead response

Mandatory before review:
- post a `Post-Implementation Proof Audit` comment on the PR

Required workflow:
- implement in your worktree
- run focused tests
- run required supported-surface proof for parity issues
- commit
- push
- open PR against `master`
- include the correct non-stale issue link in the PR body:
  - `Closes #NNN` only if this PR actually finishes issue `#NNN`
  - otherwise use `Part of #NNN` or `Related to #NNN`
- include spec refs in the PR body
- report back with the PR number

Deliverable is not complete until the PR is open and both audit artifacts exist.
```
