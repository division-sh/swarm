# Implementer Guidelines

This document captures implementation rules that should be applied consistently across the codebase.

These are not optional style preferences. They are execution rules intended to reduce drift, wasted effort, and architectural debt.

They are especially important when multiple implementers are working in parallel.

Operational companion:

- apply [IMPLEMENTER_REVIEW_CHECKLIST.md](IMPLEMENTER_REVIEW_CHECKLIST.md) before merging non-trivial changes
- apply [SEMANTIC_DRIFT.md](SEMANTIC_DRIFT.md) when a change touches semantic ownership, identity, validation, lifecycle, or cross-surface parity
- use [PROMPT_TEMPLATES.md](PROMPT_TEMPLATES.md) for the default implementer and reviewer prompt shapes
- use [PROCESS_CHECKLIST_TEMPLATES.md](PROCESS_CHECKLIST_TEMPLATES.md) when you need short copy-paste templates for pre-audits, gate decisions, follow-up decisions, or parent-state updates

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

- `platform-spec.yaml`

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
  - `docs/specs/swarm-platform/platform/review/`
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
  - `docs/specs/swarm-platform/platform/review/platform-spec.session-audit-split.yaml`
  - `docs/specs/swarm-platform/platform/review/platform-spec.expression-model.yaml`

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
- if a semantic/runtime seam has no exact governing spec section, the issue and PR must say so explicitly and name the binding governing context:
  - issue body/thread
  - repro / classification artifact when present
  - nearest adjacent contract/spec sections if they constrain the seam
- absence of one exact governing section is not by itself a merge blocker or mandatory spec-gap escalation
- treat the work as a spec-gap / ambiguity escalation only when the semantics are actually ambiguous, contradictory, or underdetermined enough that issue/repro context plus adjacent contract sections are not sufficient to govern the change honestly
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

- before writing code, re-read the exact spec section(s) cited in the issue when they exist
- treat those cited sections as the binding semantic contract for the work when they exist
- if no exact governing section exists, explicitly use the named binding governing context for the work:
  - issue body/thread
  - repro / classification artifact when present
  - adjacent contract/spec sections cited for constraint
- if the current code, tests, issue wording, or cited adjacent contract sections disagree in a way that leaves semantics ambiguous, stop and escalate before implementation
- do not rely on memory of the spec or on the issue summary alone for semantic changes

Symptom vs failure-class rule:

- this rule applies to semantic, runtime, loader, bootverify, persistence, read-model, and other spec-governed work
- it does not require the same broad semantic reassessment for purely non-semantic maintenance, mechanical cleanup, or docs-only work
- default assumption: the issue framing is probably narrower than the true failure class until the pre-audit proves otherwise
- assume the reported issue may be only the first visible symptom of a broader semantic defect
- before writing code, take a step back and reassess the full semantic rule the issue belongs to
- inspect adjacent logic in the same seam to determine the true scope of the problem, not just the first failing example
- use the cited spec section(s) to decide whether sibling contexts should behave the same way
- if the issue appears narrower than the actual failure class, stop and escalate or update the issue before treating the work as complete
- do not stop at fixing the first symptom if adjacent logic is governed by the same semantic rule
- do not treat the issue title, first reproducer, or first failing helper as the failure class by default
- the pre-audit must explicitly answer:
  - what is the broadest plausible failure class?
  - what is the chosen working failure class for this PR?
  - what is the parent failure class above it, if any?
  - is the issue framing narrower than that class?
  - after probing sibling seams under the parent, does the parent appear broken, apparently clean, different-class, or still unproven?
  - if the issue framing is narrower, is this work still honestly a first slice or does the issue need to be widened or split?

Pre-Implementation Coverage Audit rule:

- for semantic/runtime/spec-governed work, implementation must not begin until the implementer posts a `Pre-Implementation Coverage Audit` comment on the GitHub issue
- this is mandatory when:
  - the issue is about a failure class rather than one isolated bug
  - the issue names more than one manifestation
  - the reproducer or triage record names more than one manifestation
  - the issue concerns parity between surfaces such as verify vs boot, boot vs runtime, or reader vs writer
- the purpose of this audit is to make failure-class coverage reviewable before coding starts, not after the patch already exists
- for failure-class work, the pre-audit must contain an explicit manifestation coverage table and an explicit intended closure level
- for failure-class, parity, and semantic-drift work, the default expectation is that manifestation identification may require considerably more effort than the implementation itself
- do not treat that as process overhead to be optimized away
- treat it as the main mechanism preventing narrow framing, false closure, and repeated same-class fixes

Broad-first pre-audit rule:

- the pre-audit must start from the broadest plausible semantic concept, not the first local bug site, helper, file, or failing warning
- the implementer must assume the concept is broader until narrower scope is proven
- narrowing is allowed only by explicit proof that a seam consumes a different semantic concept
- “nearby files checked” is not sufficient
- the purpose of the pre-audit is to discover every currently known consumer of the concept before code shape narrows the thinking
- the watchlist should be used as a starting semantic map for that sweep when relevant, but never as a substitute for doing the sweep
- when the issue concerns a multi-step user-visible flow, the broadest plausible concept must be tested against the full relevant execution path, not only the first failing endpoint
- ask explicitly:
  - what must succeed before the observed failing step is reachable?
  - what earlier or adjacent gates on that same path can still fail the same user-visible flow?
  - if the observed endpoint were fixed, could the user still fail earlier on that same path?
- if yes, the working class must absorb that broader path or explicitly split and track it

Entry-point rule:

- the reproducer, failing line, failing helper, or error spot is an entry point, not the audit boundary
- pre-audit must treat that entry point as the starting coordinate for mapping the surrounding semantic territory
- do not mistake “I inspected the code around the error spot” for an honest pre-audit

Semantic-territory audit rule:

- pre-audit must audit all of the following, not just the local code section where the error surfaced:
  - failure-class selection
  - parent-chain selection
  - relevant execution path
  - repo-wide consumers / interpreters of the same concept
  - currently known manifestations
  - proof surface required for honest closure
  - tracker state
- if any of those are missing, the pre-audit is incomplete even when the local code section is well understood

Owner-boundary and closure-feasibility rule:

- pre-audit must ask explicitly:
  - is the chosen owner a real semantic owner or only the first helper/file encountered near the error?
  - can the chosen working failure class actually be closed in one PR?
  - if the local endpoint were fixed, would another live interpreter of the same concept still remain?
  - is the current remediation mostly code movement, or mostly a better narrative around unchanged ownership?
- if the answer shows the chosen owner is fake, the class cannot be closed, or another same-concept interpreter would remain live, widen, split, or escalate before coding

Adversarial review rule:

- every pre-audit, implementation review, and post-implementation proof audit must be reviewed adversarially, not clerically
- default reviewer posture is to assume the record may be wrong until independently verified
- reviewer must actively try to falsify:
  - chosen-class framing
  - parent-class framing
  - sibling classification
  - follow-up references
  - watchlist references
  - "already tracked by X"
  - "already closed by X"
  - closure-level claims
- if a pre-audit or PR audit says a broader parent class remains tracked by issue or stream `X`, the reviewer must independently verify that `X` is:
  - still open
  - or explicitly superseded
  - or truly closed by proof
- if none of those are true, the gate/review is not clean and the record must be corrected before merge

Repo-wide consumer sweep rule:

- before implementation, the implementer must perform an exhaustive repo-wide sweep for currently known consumers of each relevant semantic concept / canonical owner
- this sweep must include runtime, store, validation, diagnostics, harness, conformance, and operator/read-model consumers where relevant
- the pre-audit must name every currently known consuming seam discovered by that sweep
- if the implementer cannot complete a credible repo-wide sweep, implementation must not begin

Required issue-comment format:

- `Concept`
  - state the exact semantic concept or concepts being changed
- `Canonical owner(s)`
  - identify every relevant canonical owner for the issue's semantic model
  - if the issue spans multiple coupled concepts, name each owner separately
- `Consumers of each owner`
  - for each named owner, list the main touched callers, readers, validators, selectors, or runtime surfaces that should consume it
- `Systematic consumption audit`
  - for each named owner, exhaustively list every currently known sibling seam that should consume it
  - for each seam, classify it as exactly one of:
    - already consumes the canonical owner
    - moved to the canonical owner in this work
    - still bypasses the canonical owner and is explicitly split / escalated
- `Old non-authoritative paths`
  - for each named owner, list any old helpers, readers, writers, or interpreters that become invalid, non-canonical, or removal candidates
- `Failure class`
  - state the broader generic failure class, not just the first symptom
  - state the observed symptom separately from the chosen class
  - state the chosen working failure class for the PR
  - if a broader immediate parent exists between the chosen class and the broadest plausible parent, state that immediate parent explicitly
  - if a broader parent exists, state the parent failure class above the chosen class
  - explicitly say whether the current issue framing is:
    - already broad enough
    - narrower than the true class but still acceptable as a first slice
    - too narrow to implement honestly without widening or splitting
  - if the issue concerns a multi-step user-visible flow:
    - enumerate the full relevant execution path in order
    - classify each gate on that path as:
      - same chosen class
      - different semantic concept, with proof
      - explicitly split / tracked separately
- `Intended closure level`
  - state exactly one intended closure target for this work:
    - local symptom fixed
    - touched seam canonicalized
    - failure class eliminated
  - if the target is `failure class eliminated`, include one explicit statement defending why this issue is believed to cover the full currently known class rather than only one extracted slice
- `Currently known manifestations in scope`
  - list every currently known manifestation from:
    - the issue body
    - the issue thread
    - triage / reproducer notes
    - prior review comments if they already exist
    - relevant watchlist node(s)
- `Manifestation coverage table`
  - include one row per currently known manifestation
  - make the table as extensive as possible from the evidence currently available
  - default bias: over-enumerate plausible same-class manifestations and then classify them, rather than collapsing them into one broad prose row
  - do not optimize for a short table; optimize for not missing a sibling manifestation that should have been named explicitly
  - each row must name:
    - manifestation
    - canonical owner
    - planned coverage status
    - exact proof planned
    - required supported-surface or end-to-end proof, if any
    - whether it is being treated as the same class or explicitly split as a separate class
  - if the issue concerns a multi-step user-visible flow, include one row for each gate on the relevant execution path that can fail the same user-visible flow unless it is explicitly proven to be a different concept
- for each manifestation, provide exactly one planned coverage status:
  - direct reproducer and fix
  - execution proof through the same corrected path
  - split / escalate as a separate class
- for each manifestation, also state:
  - why it is believed to be the same seam or a separate seam
  - the exact proof that will be used
  - whether that proof is:
    - focused unit/integration proof
    - generic reproducer
    - supported-surface / end-to-end proof
- `Parent-class sibling probe`
  - if a broader parent failure class exists, list the sibling seams probed under that parent
  - classify each probed sibling as exactly one of:
    - broken now
    - apparently clean, with proof
    - different class, with proof
    - still unproven
  - estimate the remaining child slices likely needed to close the parent failure class:
    - estimated remaining child slices: <number or range>
    - rough grouping of those remaining slices
    - confidence level:
      - high
      - medium
      - low
  - state whether that estimate argues for:
    - absorb the parent class now
    - keep first-slice scope
    - open or update a dedicated follow-up stream
    - leave the parent explicitly open as still unproven
  - state the explicit post-pre-audit action decision for the parent failure class:
    - absorb the parent class now
    - keep first-slice scope
    - open or update a dedicated follow-up stream
    - leave the parent explicitly open as still unproven
  - if not absorbing now, state exactly where the remaining parent-class obligation is tracked
- `Tracker-state decision`
  - state whether the current issue still matches the audited class model
  - choose exactly one:
    - current issue remains correct as written
    - current issue must be updated before coding
    - new child issue required before coding
    - parent issue / umbrella issue must be updated before coding
    - older issue / stream superseded and must be marked accordingly before coding
    - watchlist-only refinement is sufficient
  - if the audited class understanding changed, repair the affected issue / parent / follow-up / watchlist record before implementation starts
  - do not leave tracker repair only in comments while the canonical issue text remains stale
- `Independent pre-audit gate status`
  - for failure-class, parity, semantic-drift, and other high-risk semantic work, record the reviewer/lead gate outcome on the issue thread before coding starts
  - valid outcomes:
    - pre-audit approved
    - pre-audit approved as first slice
    - pre-audit insufficient; widen class
    - pre-audit insufficient; escalate broad refactor
  - if the gate is waived, record the waiver explicitly on the issue thread before coding
- `Chosen-class closure commitment`
  - state plainly that this PR aims to eliminate the chosen working failure class entirely
  - do not describe the PR as a fix for only one manifestation inside the chosen class
- `Required closure proof`
  - list the focused proof required
  - list the supported-surface or end-to-end proof required
  - if this is a parity issue, name each surface that must be exercised before closure
  - if this is a multi-step user-visible flow, name the upstream gates on the relevant execution path that the proof preserves rather than stubbing away
- `Unsafe assumptions you are NOT making`
  - explicitly name anything that might look “probably covered” but is not yet proven
  - do not say “same seam” without naming the execution proof that will show it
- `Blocking ambiguities or split conditions`
  - list any reason the issue may need to split before implementation
  - if none, say none

Absolute rules:

- do not begin implementation if the audit cannot identify every relevant canonical owner for the touched semantic concept or concepts
- do not begin implementation if the broad semantic concept is not stated
- do not begin implementation if the pre-audit does not state whether the issue framing is broad enough, narrower-but-acceptable, or too narrow
- do not begin implementation if the repo-wide consumer sweep is missing, shallow, or not credible
- do not begin implementation if the audit does not include an exhaustive systematic-consumption audit for the currently known sibling seams of each named owner
- do not begin implementation if any currently known consuming seam is unclassified
- do not begin implementation on multi-step user-visible flow issues if the pre-audit does not enumerate the full relevant execution path and classify every gate on that path
- do not narrow a seam out of scope without explicit proof that it consumes a different semantic concept
- do not begin implementation if any currently known manifestation is missing from the audit
- do not begin implementation on failure-class work if the pre-audit does not include a manifestation coverage table
- do not begin implementation if the manifestation coverage table is artificially narrow, collapsed, or obviously less extensive than the currently available issue/thread/triage/review evidence supports
- do not begin implementation if the chosen working failure class is not stated explicitly
- do not begin implementation if a broader parent failure class exists but the pre-audit does not probe sibling seams under that parent enough to assess parent state
- do not begin implementation if a broader parent failure class exists but the pre-audit does not estimate the remaining child-slice tail likely needed to close that parent class
- do not begin implementation if the PR is only planning to improve one manifestation inside its own chosen working failure class rather than close that chosen class entirely
- do not begin implementation if a broader parent failure class exists and there is no explicit post-pre-audit action decision for that parent
- do not begin implementation if a concrete remaining same-class seam is discovered but no follow-up issue/stream was created or updated when the work is not absorbing it now
- do not begin implementation if the pre-audit changed the audited class model but the tracker-state decision is missing
- do not begin implementation if the tracker-state decision requires issue / parent / follow-up repair before coding and that repair was not completed
- do not begin implementation on failure-class / parity / semantic-drift / high-risk semantic work if the required independent pre-audit gate outcome is not explicitly recorded on the issue thread
- do not begin implementation on failure-class / parity / semantic-drift / high-risk semantic work if there is no explicit watchlist decision:
  - maps to existing node
  - existing node refined
  - new manifestation added
  - new node needed
- do not begin implementation if the intended closure level is not stated explicitly
- do not begin implementation if the pre-audit claims `failure class eliminated` without an explicit defendable statement of why the full currently known class is in scope
- do not classify a manifestation as “same seam” without naming the execution proof that will show it flows through the corrected path
- do not use “shared owner introduced”, “same validator”, “same helper”, or “same architecture seam” as sufficient coverage by themselves
- if a manifestation is not directly reproduced, the audit must explain exactly how it will be execution-proven through the same corrected path
- if that proof cannot be named before coding starts, mark the manifestation as split / escalate instead
- for parity issues, supported-surface proof is mandatory; synthetic agreement tests alone are not enough if the issue was discovered through a supported surface
- for multi-step user-visible flow issues, synthetic tests that force earlier gates on the same path to unconditional success may prove a local fix, but they cannot bear closure for the whole path

Broad-refactor escalation rule:

- this step happens after the `Pre-Implementation Coverage Audit` and before implementation
- if the pre-audit and systematic-consumption audit show that the named canonical owner is not used systematically across the relevant sibling seams, the implementer must decide whether the issue can still be solved honestly as a first slice
- if not, implementation must pause and the implementer must post a `Broad Refactor Escalation` comment on the issue or PR before writing code

Required escalation content:

- the exact semantic concept
- the named canonical owner
- the sibling seams currently bypassing that owner
- why a narrow fix would be insufficient or dishonest
- the proposed broader refactor scope
- what would move now
- what would remain out of scope
- the main migration, behavior, test-surface, and coordination risks

Lead decision:

- the lead must respond explicitly with exactly one of:
  - `approved as broad refactor`
  - `denied; keep first-slice scope`
  - `split: do first slice now, open canonicalization follow-up`

Absolute rules:

- do not silently widen a first-slice issue into a broad refactor without an explicit lead decision
- if the pre-audit shows that failure-class elimination would require systematic adoption of the canonical owner across multiple sibling seams, do not begin implementation until the lead has approved or denied the broad-refactor escalation

Close-the-gap rule:

- if a semantic gap can be closed cleanly in the current PR, close it in the current PR
- do not leave a sibling bypass seam, duplicate owner, or non-authoritative interpreter in place just because staging feels safer
- the default expectation is full semantic closure in the touched seam, not managed coexistence

Migration rule:

- migration exists to move persisted state, schema, or deployment ordering safely to the canonical model
- migration does not justify dual semantic ownership
- migrate data or state forward, then delete the old semantic path
- keep one canonical owner unless the lead explicitly approves a temporary seam in writing

Temporary coexistence exception:

- temporary coexistence of two semantic owners is banned by default
- it may exist only when all of the following are true:
  - atomic closure is operationally impossible
  - the remaining seam is narrowly isolated
  - the authoritative owner is explicitly named
  - the non-authoritative seam is explicitly marked temporary
  - a concrete tracked removal follow-up exists
  - the lead explicitly approves that seam in writing

Absolute rules:

- if the implementer proposes leaving the old semantic path in place, they must prove why full closure is not feasible in the same PR
- “smaller patch”, “easier rollout”, “less risky by default”, or “follow-up is easy” are not sufficient reasons by themselves
- if a first-slice issue knowingly leaves the same concept duplicated inside the same effective boundary, the implementer must escalate instead of calling the work complete
- without the temporary-coexistence exception above, close the semantic gap in the PR

Invalid pre-audit example:

- tool drift: direct reproducer and fix
- event-schema drift: same local class, should be covered by shared validator refactor

Why invalid:

- it does not name the execution proof for the event-schema manifestation
- it assumes an architecture change is enough proof by itself
- it does not require supported-surface proof before closure

Narrow patch vs broad semantic proof rule:

- for semantic/runtime/spec-governed work, keep the implementation patch narrow but do not keep the semantic assessment narrow
- after reading the cited spec, assess the full rule family in the touched seam, not only the first failing example
- after identifying the failure class, explicitly determine whether the touched seam has multiple currently known manifestations
- if yes, design proof for each manifestation before treating the patch as complete
- do not let a narrow patch implicitly redefine the issue as narrower than the tracked failure class
- inspect adjacent logic that is governed by the same semantic rule
- design tests to cover the rule in that area, including adjacent valid, invalid, mixed-case, malformed, rollback, or fail-closed paths where relevant
- do not treat a narrow code diff as sufficient proof if the touched semantic rule spans a broader area

Post-Implementation Proof Audit rule:

- for semantic/runtime/spec-governed work, a PR is not review-ready until the implementer posts a post-implementation proof audit comment on the PR
- for failure-class issues, that audit must include a failure-class coverage table, not only a narrative summary
- the PR audit must explicitly include:
  - the exact semantic concept or concepts being changed
  - every relevant canonical owner for those concepts after the change
  - which touched callers, readers, validators, selectors, or runtime surfaces now consume each owner
  - an exhaustive systematic-consumption audit for the currently known sibling seams that should consume each owner, with each seam marked as exactly one of:
    - already consumes the canonical owner
    - moved to the canonical owner in this PR
    - still bypasses the canonical owner and is explicitly split / escalated
  - which old producers, readers, or interpreters are now invalid, non-authoritative, or still surviving for each owner
  - the broader failure class the issue belongs to
  - the chosen working failure class the PR claims to close entirely
  - the parent failure class above it, if any
  - whether the issue described the full class or only the first visible symptom
  - the achieved closure level, marked as exactly one of:
    - local symptom fixed
    - touched seam canonicalized
    - failure class eliminated
  - if the achieved closure level is `failure class eliminated`, one explicit statement of why the full currently known class is now closed rather than only the touched seam
  - if a broader parent failure class exists, the sibling seams probed under that parent and whether the parent still remains open after the PR
  - if a broader parent failure class exists, the updated estimate of the remaining child slices likely needed to close that parent class:
    - estimated remaining child slices: <number or range>
    - rough grouping of those remaining slices
    - confidence level:
      - high
      - medium
      - low
  - whether implementation/review changed that estimate and why
  - which sibling contexts in the touched seam were checked
  - the generic reproducer, fixture, or focused failing proof used to capture the failure class
  - or, if none existed, the generic proof created as part of the work
  - optional general feedback section for broader engineering notes, provided it is clearly non-closure-bearing
- for issues with multiple currently known manifestations, include one row per manifestation with exactly one final status:
  - reproduced and fixed
  - execution-proven through the same corrected path
  - split / escalated as separate class
- for failure-class work, that manifestation table is mandatory even if only one manifestation remains in scope at merge time
- that table must be as extensive as possible from the evidence available by merge time, not only the minimum set needed to justify the chosen patch
- for each manifestation row, also name:
  - the exact proof used
  - any mandatory end-to-end or supported-surface proof used for closure
  - any manifestation that remains unproven
- do not rely on “tests pass” as sufficient proof without naming the generic failing proof that the fix resolves
- do not rely on “shared owner introduced”, “same seam”, or “architecture now looks cleaner” as substitutes for manifestation-level proof
- if the implementer cannot identify every relevant canonical owner for the touched semantic concept or concepts, the PR is not review-ready
- if the PR does not state exhaustively who still bypasses each named canonical owner, the PR is not review-ready
- if any currently known manifestation lacks one of the required statuses above, the PR is not review-ready
- if the manifestation table is obviously under-enumerated relative to the issue body, issue thread, triage notes, reproducers, or review context, the PR is not review-ready
- if the PR does not state the achieved closure level explicitly, the PR is not review-ready
- if the PR claims `failure class eliminated` without defendable manifestation-level proof, the PR is not review-ready
- if no generic reproducer exists yet for a semantic/runtime failure class, deriving one is part of the implementation task
- if the implementer cannot derive a clean generic proof for the failure class, stop and escalate before treating the work as complete
- before merge, explicitly decide whether the watchlist needs:
  - no change
  - node refinement
  - new manifestation row / note
  - new branch/node
- if the PR closed only a child slice of a broader parent class, update the parent issue/watchlist record before or at merge time to state:
  - which child class was closed
  - whether the parent class remains open or still unproven
  - what next same-class stream or sibling seam is already known, if any
- if review discovered a broader class, missed sibling manifestation, or corrected canonical owner understanding, update the watchlist rather than leaving that learning only in the PR thread

Surface-parity closure rule:

- for verify-vs-boot, boot-vs-runtime, reader-vs-writer, or other surface-parity issues, closure requires at least one proof at each relevant surface
- targeted verify proof alone is never sufficient for a verify-vs-boot issue
- synthetic unit agreement tests are useful but do not replace a supported end-to-end or supported-surface smoke when the issue was discovered through a supported surface
- for multi-step user-visible flows, relevant surfaces include earlier readiness/auth gates on the same path, not only the final action endpoint
- if the issue names multiple semantic classes or manifestations in scope, each one must have explicit closure evidence

Minimum spec-reference block:

```text
Spec references:
- platform-spec.yaml: <section/path>
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
