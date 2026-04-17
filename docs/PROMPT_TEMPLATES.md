# Prompt Templates

These are the default prompt templates for semantic/runtime/spec-governed work.

They are short operational prompts. The full rules still live in:

- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)
- [SEMANTIC_DRIFT.md](/Users/youmew/dev/swarm/docs/SEMANTIC_DRIFT.md)

## Default Implementer Prompt

Use this in two phases:

- Phase 1: pre-audit / gate
- Phase 2: implementation / PR proof

Do not combine them by default. Phase 1 should end at the gate decision. Phase 2 starts only after the issue is gated for implementation.

### Phase 1: Pre-Audit / Gate

```text
Pick up issue `#NNN`.

Phase:
- pre-audit first
- do not start implementation yet

Before the audit:
- read the full issue body/thread
- re-read the exact cited spec section(s) and treat them as binding when they exist
- if no exact governing spec section exists, explicitly identify the binding governing context:
  - issue body/thread
  - repro / classification artifact when present
  - nearest adjacent contract/spec sections if they help constrain the seam
- follow:
  - `docs/IMPLEMENTER_GUIDELINES.md`
  - `docs/SEMANTIC_DRIFT.md`

Important steering:
- spec is the source of truth
- fail fast, fail closed, aggressive migration, zero legacy behavior
- default to complete closure rather than staged follow-up
- treat first-slice scope as an exception that must be justified, not a neutral starting point
- no heuristics, no compatibility shims, no preserving old behavior “just in case”
- close the semantic gap in the PR if it can be closed cleanly
- do not treat migration as a reason to preserve dual semantic ownership
- do not treat “shared owner introduced” or “same seam” as proof by themselves
- stop and escalate on any spec gap, ambiguity, contradiction, or uncaptured off-spec behavior

Mandatory in this phase:
- post a `Pre-Implementation Coverage Audit` comment on the GitHub issue

That issue comment must state:
- the issue category
- the observed symptom
- the exact semantic concept or concepts being changed
- the chosen working failure class for the PR
- the immediate parent failure class above it, if any
- the parent failure class above it, if any
- whether the issue framing is broad enough, narrower-but-acceptable as a first slice, or too narrow
- confirmation that the observed failing line / helper / reproducer was treated as an entry point rather than the audit boundary
- if the issue concerns a multi-step user-visible flow:
  - the full relevant execution path in order
  - what must succeed before the observed failing step is reachable
  - every gate on that path, each marked as:
    - same chosen class
    - different semantic concept, with proof
    - explicitly split / tracked separately
- every relevant canonical owner for those concepts
- whether the chosen owner is a real semantic owner or only the first local helper/file encountered
- the touched consumers of each owner
- an exhaustive systematic-consumption audit for every currently known sibling seam that should consume each owner, with each seam marked as:
  - already consumes the canonical owner
  - moved to the canonical owner in this work
  - different semantic concept, with proof
  - still bypasses the canonical owner and is explicitly split / escalated
- the old non-authoritative producers/readers/interpreters that become invalid or removal candidates for each owner
- the broader failure class
- every currently known manifestation in scope
- the exact proof planned for each manifestation
- the parent-class sibling probe and post-pre-audit parent action decision
- the tracker-state decision:
  - current issue remains correct as written
  - current issue must be updated before coding
  - new child issue required before coding
  - parent issue / umbrella issue must be updated before coding
  - older issue / stream superseded and must be marked accordingly before coding
  - watchlist-only refinement is sufficient
- the watchlist-backed promotion check:
  - what broader class is already tracked in the mapped watchlist node
  - whether that node suggests additional live sibling manifestations or consumer families beyond the proposed slice
  - whether that watchlist evidence means the parent should be absorbed now rather than left for follow-up
- the estimated remaining child-slice tail needed to close the parent failure class, if any, with rough grouping and confidence level
- the watchlist mapping or refinement decision
- the intended closure level
- the chosen-class closure commitment: this PR aims to eliminate its chosen working failure class entirely
- explicit closure-feasibility check:
  - can this chosen class actually be closed in one PR?
  - if the local endpoint were fixed, would another same-concept interpreter still remain live?
- required supported-surface / end-to-end proof if this is a parity issue
- any deeper architecture issue / type-model smell noticed and the long-run better direction, if any
- explicit tracking decision for that architecture feedback:
  - create/update issue
  - watchlist only
  - residual risk only
- any blocking ambiguity or split condition

Do not start implementation if:
- any relevant canonical owner is missing
- the systematic-consumption audit is not exhaustive
- any manifestation is labeled “same seam” without named execution proof
- a broader parent exists but sibling probing or the parent action decision is missing
- the pre-audit changed the audited class model but the tracker-state decision is missing
- the tracker-state decision requires issue / parent / follow-up repair before coding and that repair was not completed
- a broader parent exists but the watchlist-backed promotion check was not done
- a required watchlist decision is missing
- a required independent pre-audit gate outcome is not explicitly recorded on the issue thread
- the intended closure level is not stated
- supported-surface proof is required but not named
- a multi-step user-visible flow issue names only the final endpoint and not the full relevant execution path
- the pre-audit is only an audit of the local error spot rather than the surrounding semantic territory

If the pre-audit shows broad duplication and a narrow fix would be dishonest:
- pause
- post a `Broad Refactor Escalation`
- wait for explicit lead response:
  - `approved as broad refactor`
  - `denied; keep first-slice scope`
  - `split: do first slice now, open canonicalization follow-up`

Deliverable for this phase:
- the issue thread contains the `Pre-Implementation Coverage Audit`
- any required tracker/watchlist repair is already done
- the required independent pre-audit gate outcome is explicitly recorded on the issue thread
- stop here and report the gate result
```

### Phase 2: Implementation / PR Proof

```text
Resume issue `#NNN` after the pre-audit gate is satisfied.

Phase:
- implementation
- use the approved gate boundary as binding

Before coding:
- re-read the approved `Pre-Implementation Coverage Audit`
- re-read the recorded gate outcome and conditions
- re-read the exact cited spec section(s) and binding governing context
- follow:
  - `docs/IMPLEMENTER_GUIDELINES.md`
  - `docs/SEMANTIC_DRIFT.md`

Important steering:
- stay inside the approved child/class boundary
- if implementation discovers another live same-concept interpreter or a wider parent obligation than the gate allowed, stop and repair the issue/gate record before continuing
- do not silently widen scope
- do not preserve compatibility seams or dual semantic ownership unless explicitly approved
- do not rely on “shared owner introduced” or “same seam” as proof by themselves

Mandatory before review:
- post a `Post-Implementation Proof Audit` comment on the PR

That PR comment must include:
- the exact semantic concept or concepts changed
- the chosen working failure class the PR claims to close entirely
- the parent failure class above it, if any
- every relevant canonical owner after the change
- the touched consumers of each owner
- an exhaustive systematic-consumption audit for each owner
- old paths now invalid / non-authoritative / still surviving
- broader failure class
- whether the issue was symptom-shaped
- the achieved closure level
- whether the parent class remains open after this PR, if any
- the updated estimate of the remaining child-slice tail needed to close the parent failure class, if any, and whether implementation/review changed that estimate
- sibling contexts checked
- generic failing proof used or created
- the explicit watchlist decision
- any deeper architecture issue / type-model smell noticed and the long-run better direction, if any
- explicit tracking decision for that architecture feedback and where it is now tracked
- optional general feedback section for broader engineering notes, clearly treated as non-closure-bearing
- a manifestation coverage table with one row per known manifestation, each marked as exactly one of:
  - reproduced and fixed
  - execution-proven through the same corrected path
  - split / escalated as separate class
- the exact proof used for each row
- required supported-surface proof actually run

A PR is not review-ready if:
- either audit is missing
- any canonical owner is missing
- the owner-consumption audit is not exhaustive
- any manifestation lacks explicit proof
- the PR improves only one manifestation inside its chosen working failure class instead of closing that chosen class
- closure relies only on “shared owner”, “same seam”, or “cleaner architecture”

Required workflow:
- implement in your worktree
- run focused tests
- run required supported-surface proof for parity issues
- commit
- push
- open a normal reviewable PR against `master`, not a draft PR
- use the required PR title format:
  - `[agent-x][issue #123] Short workstream title`
- use commit subjects in the required format:
  - `<type>: <short summary>`
- include the correct issue link in the PR body:
  - `Closes #NNN` only if this PR actually completes issue `#NNN`
  - otherwise use `Part of #NNN` or `Related to #NNN`
- include governing refs/context in the PR body:
  - exact spec references when they exist
  - otherwise an explicit statement that no exact governing spec section exists, plus the binding issue/repro context and any adjacent contract sections used
- for any semantic migration, include in the PR body:
  - the new canonical owner
  - the old producer / reader / writer path now invalid
  - which production paths were checked for surviving old behavior
  - whether the migration is complete
- report back with the PR number

Deliverable is not complete until the PR is open and both audit artifacts exist.
```

## Default Reviewer Prompt

```text
Review this PR using the repo review process, not an informal summary.

Required process:
1. Read the PR description.
2. Read all existing PR conversation comments.
3. Read all inline review comments and treat unresolved comments as blocking until verified obsolete on current head.
4. Read the linked issue, the issue thread, and the exact spec section(s) cited in the issue/PR before judging the code.
5. Verify the issue category and, if required, the recorded independent pre-audit gate outcome on the issue thread before judging the patch.
6. If the PR is semantic/runtime work and it does not cite exact governing spec section(s), treat the review as incomplete and say so.
7. Review adversarially at every layer: pre-audit, implementation, and post-audit.
   - do not trust parent-class, follow-up, watchlist, closure, or “already captured by X” claims without independent verification
   - actively try to falsify those claims before accepting them
   - if the record says a broader parent class remains tracked by issue/stream `X`, verify that `X` is still open, explicitly superseded, or truly closed by proof
   - if none is true, treat the review/gate as incomplete until the tracking record is repaired
   - start from the default presumption that complete closure is the right answer and require explicit justification for any first-slice / follow-up story
8. Do a lead-level framing check:
  - what is the broadest plausible semantic concept or concepts in play?
  - what chosen working failure class did the PR actually take?
  - what parent failure class does that chosen class belong to, if any?
  - what repo-wide consumers or sibling contexts use that same concept?
  - does the issue describe the full class or only the first visible symptom?
  - did the implementer narrow scope only by proof, or merely by local code proximity?
  - for multi-step user-visible flows, what is the full relevant execution path and what gates must succeed before the failing endpoint is reachable?
9. Verify the issue-level `Pre-Implementation Coverage Audit` exists when required.
10. In that pre-audit, verify:
   - the chosen working failure class is explicit
   - the parent failure class is explicit when a broader parent exists
   - every relevant canonical owner is named
   - the touched consumers of each owner are named
   - the systematic-consumption audit is exhaustive for each owner
   - every known manifestation is listed
   - sibling seams under the parent were probed enough to assess parent state
   - the post-pre-audit parent action decision is explicit
   - the tracker-state decision is explicit
   - if the audited class model changed, the issue / parent / follow-up / watchlist record was repaired before coding
   - the watchlist decision is explicit
   - the mapped watchlist node was checked as active evidence when deciding whether broader parent closure should be required now
   - the intended closure level is explicit
   - seams narrowed out as “different concept” are supported by explicit proof
   - each manifestation is classified as exactly one of:
     - direct reproducer and fix
     - execution proof through the same corrected path
     - split / escalate as separate class
   - the audit does not rely on “same seam”, “same validator”, or “shared owner” without named execution proof
   - for parity issues, supported surface(s) required for closure are named
   - any parent/follow-up tracker reference is independently verified rather than accepted from the audit text
11. If the pre-audit shows multiple live interpreters of the same concept that are not all being moved now, verify there is an explicit lead-approved staged broad-refactor plan before implementation proceeded.
12. Review the code and tests against:
   - the cited spec section(s)
   - `docs/IMPLEMENTER_GUIDELINES.md`
   - `docs/IMPLEMENTER_REVIEW_CHECKLIST.md`
   - `docs/SEMANTIC_DRIFT.md`
13. Compare any derived selector/projection/validation/summary surface against its adjacent canonical owner.
14. Run focused local tests when possible.
15. Build an explicit finding list before posting comments.
16. Verify the PR-level `Post-Implementation Proof Audit` exists.
17. In that PR audit, verify:
   - the chosen working failure class the PR claims to close is explicit
   - the parent failure class is explicit when one exists
   - every relevant canonical owner after the change is named
   - the owner-consumption audit is exhaustive for each owner
   - every known manifestation has a row
   - the achieved closure level is explicit
   - the parent-class residual state is explicit when a broader parent exists
   - every row is marked as exactly one of:
     - reproduced and fixed
     - execution-proven through the same corrected path
     - split / escalated as separate class
   - each row names the exact proof used
   - any required supported-surface or end-to-end proof is named explicitly
   - any parent/follow-up tracker reference is independently verified rather than accepted from the PR audit text
18. Stop review and mark the PR not review-ready if:
   - either audit is missing
   - any relevant canonical owner is missing
   - the owner-consumption audit is not exhaustive
   - any known manifestation lacks explicit proof
   - the required pre-audit gate outcome was never recorded on the issue thread
   - a parent or follow-up tracker reference is stale, unverified, or inconsistent with the closure story
   - multiple currently known live codepaths still interpret the same concept without explicit lead-approved staged handling
   - the PR does not aim to close its chosen working failure class entirely
   - the PR leaves dual semantic ownership in place without explicit lead-approved temporary-seam justification
19. Do not accept “shared owner introduced”, “same seam”, “same validator”, or “cleaner architecture” as closure evidence by themselves.
20. For parity issues, require proof at each relevant surface.
21. If the issue was discovered through a supported helper or supported boot/runtime surface, require supported-surface closure evidence before saying the failure class is unlikely to reproduce.
22. On multi-step user-visible flows, do not accept closure proof that stubs earlier gates on the same path to unconditional success unless separate proof preserves the real gate behavior at those earlier surfaces.
23. Do not approve semantic work merely because the touched seam is better; approve only if the PR materially reduces semantic drift for the concept in scope.
24. After every review pass, leave both required GitHub artifacts on the PR:
   - one human-readable substantive review comment
   - one short checklist-style PR comment with guideline checks, residual risk, risk level, and merge recommendation
25. In the checklist-style PR comment, classify PR risk as exactly one of:
   - `low`
   - `medium`
   - `high`
26. If risk is `high`, do not recommend merge until a second reviewer has also reviewed it.
27. Verify the PR itself follows the required hygiene rules:
   - normal reviewable PR rather than draft PR
   - PR title format:
     - `[agent-x][issue #123] Short workstream title`
   - commit subjects:
     - `<type>: <short summary>`
28. If review reveals worthwhile follow-up work, do not leave it as chat-only commentary.
29. On approval, explicitly state:
   - spec compliance assessment
   - failure-class coverage assessment
   - closure level assessment
   - whether all currently known live consumers of the concept are now moved, proven different, or explicitly covered by an approved staged plan
   - whether any broader parent class remains open and where it is tracked
   - whether no worthwhile follow-up remains or where it is already tracked

Output to me:
- findings first, ordered by severity with file references
- then what GitHub comments/review actions you posted
- then tests run and residual risk
- then follow-up issues raised
- then lead feedback
```
