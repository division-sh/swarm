# Implementer Review Checklist

Use this checklist before merging any non-trivial change.

This is a practical companion to [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md).

Short copy-paste operational blocks live in [PROCESS_CHECKLIST_TEMPLATES.md](/Users/youmew/dev/swarm/docs/PROCESS_CHECKLIST_TEMPLATES.md).

If any answer below is "no", "not sure", or "this patch needs an exception", stop and review the design before merging.

## Process Summary

Review the work against this process, in order:

1. Was the issue good enough to assign?
- broad failure class when known
- reproducer when known
- governing spec refs when known
- already-known sibling manifestations when known

2. Was the category assigned correctly?
- local
- failure-class
- parity
- semantic-drift
- high-risk maintenance
- was the issue mapped to the right watchlist node when one existed?

3. Was the pre-audit good enough to start coding?
- broadest plausible failure class tested
- chosen working failure class stated explicitly
- parent failure class stated explicitly when a broader parent exists
- observed symptom, chosen class, immediate parent, and broadest plausible parent all stated explicitly
- did the implementer treat the reproducer / failing line / error spot as an entry point rather than the audit boundary?
- if the issue concerns a multi-step user-visible flow, was the full relevant execution path enumerated in order rather than only the observed failing endpoint?
- for each gate on that path, was it classified as same class, different concept with proof, or explicitly split/tracked?
- issue framing challenged rather than accepted
- canonical owner(s) named
- did the pre-audit test whether the chosen owner is a real semantic owner rather than only the first helper/file near the error?
- repo-wide consumer sweep done
- manifestation table present when required
- sibling seams under the parent failure class probed enough to assess parent state
- was the mapped watchlist node consulted as active evidence when deciding whether to absorb the parent now versus keep first-slice scope?
- did the watchlist node suggest any additional live sibling manifestations or consumer families that should have changed that decision?
- explicit post-pre-audit action decision made for the parent failure class when a broader parent exists
- explicit tracker-state decision made after pre-audit:
  - current issue remains correct as written
  - current issue must be updated before coding
  - new child issue required before coding
  - parent issue / umbrella issue must be updated before coding
  - older issue / stream superseded and must be marked accordingly before coding
  - watchlist-only refinement is sufficient
- if the audited class understanding changed, were the issue / parent / follow-up / watchlist records repaired before coding started?
- intended closure level stated explicitly
- did the pre-audit test closure feasibility explicitly rather than assuming the chosen class can be closed?
- did the pre-audit ask whether another same-concept interpreter would remain live even if the local endpoint were fixed?
- did the pre-audit distinguish real code-level remediation from narrative-only reconciliation around unchanged ownership?
- deeper architecture issue / type-model smell stated when the implementer noticed one
- explicit watchlist decision made:
  - maps to existing node
  - existing node refined
  - new manifestation added
  - new node needed
- for failure-class / parity / semantic-drift work, was manifestation identification treated as the main effort rather than rushed as lightweight paperwork?

4. If the category required it, did an independent pre-audit review gate happen before coding?
- and did the reviewer/lead record that gate outcome explicitly on the issue thread before coding started?
- and was that gate review adversarial rather than clerical?
- did the reviewer start from the default presumption of complete closure rather than treating follow-up staging as normal?
- if the outcome was `approved as first slice`, did the reviewer verify that:
  - the chosen class is a real coherent class boundary
  - sibling probing under the parent was actually done
  - the mapped watchlist node was checked as active evidence before accepting staged scope
  - the parent tracker is valid and independently verified
  - the PR can still honestly eliminate its chosen class entirely?
- if those conditions were not true, did the reviewer push back and require wider or complete remediation instead?

5. Was the PR proof audit good enough to merge?
- achieved closure level stated explicitly
- did the PR claim closure of its chosen working failure class, not only one manifestation inside it?
- manifestation-level proof provided
- any split or residual seam stated explicitly

6. Does the merge recommendation match the proven closure level?
- did the reviewer explicitly decide whether the watchlist needs to be updated at review close-out or merge time?
- if the PR closed only a child slice of a broader parent class, was the parent issue/watchlist record updated to say what child closed and what parent obligation remains?
- if the implementer surfaced deeper architecture feedback, did the reviewer evaluate whether it should remain guidance only, trigger a broad-refactor escalation, or become a tracked follow-up?
- if the architecture feedback was concrete and worthwhile, did the reviewer ensure a new or existing issue/stream captured it rather than leaving it only in audit prose?

## Stop-Ship Questions

- Does this change agree with the platform spec, or does it make a spec/implementation mismatch explicit?
- For semantic/runtime/spec-governed work, did the reviewer step back and test whether the issue is describing the full failure class rather than only the first visible symptom?
- If the semantics were unclear, did the implementer stop and escalate instead of inferring locally?
- If this is a non-trivial semantic change, was there a reviewed spec delta before code?
- Is the semantic owner of this behavior explicit and singular?
- Did this change reduce semantic drift instead of adding another local interpretation?
- Did this avoid heuristic fallback for core semantics?
- Did this avoid product-specific leakage into shared runtime layers?
- Did this avoid adding another compatibility branch when a clean migration would work?
- Did this avoid branch-heavy decision trees for core semantics?
- Did this avoid duplicating logic that already exists elsewhere?
- Did this preserve the repo policy of fail-fast and zero legacy behavior?

## Architecture Purity

- Is the platform spec still the authoritative source for the semantics touched here?
- If code and spec disagreed, was that disagreement resolved explicitly rather than patched around?
- If there may be a spec gap, was that gap surfaced explicitly instead of silently filled by implementation choice?
- Is the change improving the model instead of patching around a model gap?
- If a concept matters in multiple layers, is there one canonical implementation?
- Are invalid or ambiguous cases failing closed rather than being normalized silently?
- Are control-plane semantics represented explicitly rather than repeatedly decoded from JSON or strings?
- Are semantically different concepts still kept distinct in code and types?

## DRY And Elegance

- Did we avoid copying semantic logic between gateway, executor, validator, boot verifier, store, dashboard, or tests?
- Did we remove or consolidate existing duplicate logic where practical?
- Is the control flow simple enough to explain clearly?
- If the code needed many branches, did we first check for the missing abstraction?
- Did we prefer a cleaner shared abstraction over the fastest local patch?

## Product Leakage

- Is every behavior in shared runtime code truly a platform/runtime rule?
- If any behavior comes from one product/workflow/prompt/dashboard convention, is it kept at the boundary instead of embedded in core runtime logic?
- If product behavior was promoted into platform behavior, was that promotion made explicit in code, tests, and docs?

## Fallbacks And Compatibility

- Did we avoid "try one meaning, then reinterpret as another" behavior?
- Did we avoid string-prefix or naming-convention guessing for core semantics?
- Did we avoid per-call schema probing or runtime compatibility guessing in hot paths?
- Did we remove legacy behavior instead of preserving it behind a compatibility seam?
- If any fallback or compatibility path remains, did the lead explicitly approve that exact seam in writing and mark it as temporary?
- Do unknown startup/validation/probe failures fail closed instead of being treated as acceptable?

## Tests

- Did we add or update focused invariant tests for the semantic seam touched by this change?
- If there is an end-to-end regression risk, did we also add or update a broader regression test?
- If getting tests green was unexpectedly difficult, did we treat that as an architectural smell and record it?
- If this PR changes a canonical owner or trust boundary, do tests cover the production migration seams and not only the primary path?
- If this PR changes a derived selector, projection, validation filter, or summary surface, did we compare it against the adjacent canonical writer/store/runtime owner and add a counterexample test for selector drift?

## Observability

- If this change affects an important runtime decision, can operators see that decision clearly?
- Did we avoid forcing future debugging to reconstruct semantics from indirect logs or tables?
- If a denial, retarget, routing choice, or persistence precondition matters, is it surfaced explicitly?

## Final Merge Check

- Before code review, did the reviewer read the implementer's PR summary, tests-run list, residual risk, and not-in-scope notes?
- Before merge recommendation, did the reviewer read the existing PR conversation comments and review comments for substantive concerns, and explicitly check whether they are still unresolved or already obsolete on the current head?
- Does the PR description include:
  - a short human summary in plain language
  - an explicit issue link:
    - `Closes #...` for full completion
    - or `Part of #...` for partial work
    - and no closing keyword for an issue that still has pending work
  - either:
    - exact governing spec references for the touched seam
    - or an explicit statement that this PR is non-semantic maintenance and why no platform spec section governs it
  - what changed
  - why this is needed
  - scope boundaries
  - exact tests run
  - residual risk
  - follow-up or explicitly not-in-scope items
- If the issue or PR does not cite an exact spec section, did it explicitly state that the seam is a spec gap / ambiguity instead of presenting local issue wording as authoritative?
- Before implementation, did the implementer re-read the cited spec section(s) rather than relying on issue prose or memory?
- If the PR cites spec references, did the reviewer compare the implementation against the cited spec section(s), not just against the issue summary?
- For semantic/runtime/spec-governed work, did the reviewer explicitly ask:
  - what broader semantic rule this symptom belongs to
  - what sibling contexts use that same rule
  - whether the PR proves the failure class rather than only the first example
- For semantic/runtime/spec-governed work, did the implementer post a self-audit comment that explicitly states:
  - the broader failure class
  - whether the issue was symptom-shaped
  - the sibling contexts checked
  - the generic failing proof used or created
- If no generic reproducer existed before implementation, did the reviewer verify that the PR now creates one?
- If the PR claims non-semantic maintenance, did the reviewer verify that no semantic/runtime contract behavior is being changed under that label?
- If implementation uncovered existing off-spec behavior outside the issue's stated scope, did the implementer stop and escalate instead of silently widening the change?
- If the issue appears narrower than the actual failure class, did the reviewer require the issue/PR framing or tests to be widened before calling the work complete?
- If the PR description lists follow-up work, did the reviewer check whether any follow-up item is narrow enough to be absorbed into the current PR instead of becoming another tiny issue?
- If a follow-up item is left out of the PR, is there a clear reason it should remain separate:
  - meaningful additional scope
  - real cross-boundary coordination
  - or materially higher risk than the current PR
- After this substantive review pass, did the reviewer leave a short PR conversation comment that checks this change against the relevant implementation rules?
- Did that PR comment explicitly call out only the rules that matter for this change, rather than mechanically filling a giant template?
- Did that PR comment include:
  - the relevant guideline checks
  - residual risk
  - merge recommendation
- Did the reviewer classify the PR risk as `low`, `medium`, or `high`?
- If risk is `high`, did a second reviewer also review it before merge?
- Was this opened as a normal reviewable PR rather than a draft PR?
- Does the PR title follow the required format:
  - `[agent-x][issue #123] Short workstream title`
- Do the commit subjects in the PR follow the required `<type>: <short summary>` format?
- For any semantic migration:
  - what is the new canonical owner?
  - what old producer / reader / writer path is now invalid?
  - which production paths were checked for surviving old behavior?
  - is the migration complete?
- For any semantic refactor or canonicalization claim:
  - did the PR reduce the number of live interpreters of the concept in scope?
  - after the PR, do multiple conflicting codepaths still interpret the same concept?
  - if yes, was approval blocked unless the remaining seams were proven different or explicitly covered by a lead-approved staged plan?
- For any compatibility or migration seam left in place:
  - was it explicitly approved by the lead?
  - is it time-bounded and isolated to one boundary?
  - is there a concrete removal follow-up rather than an indefinite shim?
- For any claimed migration:
  - is it moving state/schema/deployment ordering to one canonical model rather than preserving dual semantic ownership?
  - if dual semantic ownership remains, was that treated as an exception rather than a default migration choice?
- For any persisted read-model or canonical surface touched here:
  - is there one canonical write-time owner?
  - is the consuming reader explicit?
  - what bypass or drift path remains, if any?
- For any derived reader/projection/selector touched here:
  - which adjacent canonical owner already defines the same semantic boundary?
  - did the reviewer compare the two directly instead of reviewing the new query in isolation?
  - what counterexample state would reveal drift between them?
- For any external side effect claimed in review or close-out:
  - was the authoritative external system re-checked after the action?
  - is the completion claim based on verified state rather than narration?
- Would another implementer understand the canonical owner of this behavior from the code alone?
- Would this change still make sense if two more implementers touched the same subsystem next week?
- Did this patch make the codebase more elegant, more unified, and easier to reason about?
- If not, should this be reframed as an architecture task instead of merged as-is?

Reviewer completion rule:

- do not approve semantic work merely because the touched seam is better
- approve only if the PR materially reduces semantic drift for the concept in scope and does not leave currently known conflicting interpreters alive without explicit approval

Recommended PR comment shape:

```text
Guideline check:

- Canonical owner: pass / concern
- Fallback / compatibility shim: pass / concern
- Fail-closed behavior: pass / concern
- Semantic duplication: pass / concern
- Scope discipline: pass / concern

Residual risk:
- ...

Merge recommendation:
- approve / changes needed
```

Only comment on the rules that are actually relevant to the PR.
The goal is to force explicit rule-based review after each real review pass, not to create a ritual template right before merge.

Recommended PR description shape:

```text
Spec references:

- docs/specs/swarm-platform/platform/contracts/platform-spec.yaml: <section/path>
- governing rule: <short plain-language statement>

or

- none; this PR is non-semantic maintenance
- justification: <why no platform spec section governs this change>

## Human Summary

Short plain-language explanation of what this PR changes, why it matters, and which larger goal it supports.

## What Changed

- ...

## Why This Is Needed

- ...

## Scope Boundaries

- In scope:
  - ...
- Not in scope:
  - ...

## Tests Run

- `...`
- `...`

## Residual Risk

- ...

## Follow-Up

- ...
```

Default rule for follow-up items:

- do not create a new issue for a narrow follow-up that should have been included in the current PR
- if a follow-up is small and same-seam, flag it during review and ask for it to be absorbed before merge
- only leave follow-up work separate when it is meaningfully broader, riskier, or cross-boundary
