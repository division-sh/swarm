# Prompt Templates

These are the default prompt templates for semantic/runtime/spec-governed work.

They are short operational prompts. The full rules still live in:

- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)
- [SEMANTIC_DRIFT.md](/Users/youmew/dev/swarm/docs/SEMANTIC_DRIFT.md)

## Default Implementer Prompt

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
- an exhaustive systematic-consumption audit for every currently known sibling seam that should consume each owner, with each seam marked as:
  - already consumes the canonical owner
  - moved to the canonical owner in this work
  - still bypasses the canonical owner and is explicitly split / escalated
- the old non-authoritative producers/readers/interpreters that become invalid or removal candidates for each owner
- the broader failure class
- every currently known manifestation in scope
- the exact proof planned for each manifestation
- required supported-surface / end-to-end proof if this is a parity issue
- any blocking ambiguity or split condition

Do not start implementation if:
- any relevant canonical owner is missing
- the systematic-consumption audit is not exhaustive
- any manifestation is labeled “same seam” without named execution proof
- supported-surface proof is required but not named

If the pre-audit shows broad duplication and a narrow fix would be dishonest:
- pause
- post a `Broad Refactor Escalation`
- wait for explicit lead response:
  - `approved as broad refactor`
  - `denied; keep first-slice scope`
  - `split: do first slice now, open canonicalization follow-up`

Mandatory before review:
- post a `Post-Implementation Proof Audit` comment on the PR

That PR comment must include:
- the exact semantic concept or concepts changed
- every relevant canonical owner after the change
- the touched consumers of each owner
- an exhaustive systematic-consumption audit for each owner
- old paths now invalid / non-authoritative / still surviving
- broader failure class
- whether the issue was symptom-shaped
- sibling contexts checked
- generic failing proof used or created
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
- closure relies only on “shared owner”, “same seam”, or “cleaner architecture”

Required workflow:
- implement in your worktree
- run focused tests
- run required supported-surface proof for parity issues
- commit
- push
- open PR against `master`
- include `Closes #NNN` and spec refs in the PR body
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
5. If the PR is semantic/runtime work and it does not cite exact governing spec section(s), treat the review as incomplete and say so.
6. Do a lead-level framing check:
   - what exact semantic concept or concepts are in play?
   - what broader failure class does the issue belong to?
   - what sibling contexts or manifestations use that same rule?
   - does the issue describe the full class or only the first visible symptom?
7. Verify the issue-level `Pre-Implementation Coverage Audit` exists when required.
8. In that pre-audit, verify:
   - every relevant canonical owner is named
   - the touched consumers of each owner are named
   - the systematic-consumption audit is exhaustive for each owner
   - every known manifestation is listed
   - each manifestation is classified as exactly one of:
     - direct reproducer and fix
     - execution proof through the same corrected path
     - split / escalate as separate class
   - the audit does not rely on “same seam”, “same validator”, or “shared owner” without named execution proof
   - for parity issues, supported surface(s) required for closure are named
9. If the pre-audit shows broad duplication that would require a broader refactor, verify there is an explicit lead decision before implementation proceeded.
10. Review the code and tests against:
   - the cited spec section(s)
   - `docs/IMPLEMENTER_GUIDELINES.md`
   - `docs/IMPLEMENTER_REVIEW_CHECKLIST.md`
   - `docs/SEMANTIC_DRIFT.md`
11. Compare any derived selector/projection/validation/summary surface against its adjacent canonical owner.
12. Run focused local tests when possible.
13. Build an explicit finding list before posting comments.
14. Verify the PR-level `Post-Implementation Proof Audit` exists.
15. In that PR audit, verify:
   - every relevant canonical owner after the change is named
   - the owner-consumption audit is exhaustive for each owner
   - every known manifestation has a row
   - every row is marked as exactly one of:
     - reproduced and fixed
     - execution-proven through the same corrected path
     - split / escalated as separate class
   - each row names the exact proof used
   - any required supported-surface or end-to-end proof is named explicitly
16. Stop review and mark the PR not review-ready if:
   - either audit is missing
   - any relevant canonical owner is missing
   - the owner-consumption audit is not exhaustive
   - any known manifestation lacks explicit proof
   - the PR leaves dual semantic ownership in place without explicit lead-approved temporary-seam justification
17. Do not accept “shared owner introduced”, “same seam”, “same validator”, or “cleaner architecture” as closure evidence by themselves.
18. For parity issues, require proof at each relevant surface.
19. If the issue was discovered through a supported helper or supported boot/runtime surface, require supported-surface closure evidence before saying the failure class is unlikely to reproduce.
20. After every review pass, leave both required GitHub artifacts on the PR:
   - one human-readable substantive review comment
   - one short checklist-style PR comment with guideline checks, residual risk, risk level, and merge recommendation
21. If review reveals worthwhile follow-up work, do not leave it as chat-only commentary.
22. On approval, explicitly state:
   - spec compliance assessment
   - failure-class coverage assessment
   - whether all relevant canonical owners are now systematically consumed in the touched seam
   - whether no worthwhile follow-up remains or where it is already tracked

Output to me:
- findings first, ordered by severity with file references
- then what GitHub comments/review actions you posted
- then tests run and residual risk
- then follow-up issues raised
- then lead feedback
```
