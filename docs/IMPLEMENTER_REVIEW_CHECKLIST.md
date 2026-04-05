# Implementer Review Checklist

Use this checklist before merging any non-trivial change.

This is a practical companion to [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md).

If any answer below is "no", "not sure", or "this patch needs an exception", stop and review the design before merging.

## Stop-Ship Questions

- Does this change agree with the platform spec, or does it make a spec/implementation mismatch explicit?
- If the semantics were unclear, did the implementer stop and escalate instead of inferring locally?
- If this is a non-trivial semantic change, was there a reviewed spec delta before code?
- Is the semantic owner of this behavior explicit and singular?
- Did this change reduce semantic drift instead of adding another local interpretation?
- Did this avoid heuristic fallback for core semantics?
- Did this avoid product-specific leakage into shared runtime layers?
- Did this avoid adding another compatibility branch when a clean migration would work?
- Did this avoid branch-heavy decision trees for core semantics?
- Did this avoid duplicating logic that already exists elsewhere?

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
- If any fallback remains, is there a concrete current operational reason and explicit documentation for it?

## Tests

- Did we add or update focused invariant tests for the semantic seam touched by this change?
- If there is an end-to-end regression risk, did we also add or update a broader regression test?
- If getting tests green was unexpectedly difficult, did we treat that as an architectural smell and record it?
- If this PR changes a canonical owner or trust boundary, do tests cover the production migration seams and not only the primary path?

## Observability

- If this change affects an important runtime decision, can operators see that decision clearly?
- Did we avoid forcing future debugging to reconstruct semantics from indirect logs or tables?
- If a denial, retarget, routing choice, or persistence precondition matters, is it surfaced explicitly?

## Final Merge Check

- Before code review, did the reviewer read the implementer's PR summary, tests-run list, residual risk, and not-in-scope notes?
- Does the PR description include:
  - a short human summary in plain language
  - what changed
  - why this is needed
  - scope boundaries
  - exact tests run
  - residual risk
  - follow-up or explicitly not-in-scope items
- If the PR description lists follow-up work, did the reviewer check whether any follow-up item is narrow enough to be absorbed into the current PR instead of becoming another tiny issue?
- If a follow-up item is left out of the PR, is there a clear reason it should remain separate:
  - meaningful additional scope
  - real cross-boundary coordination
  - or materially higher risk than the current PR
- Before merge, did a reviewer leave a short PR conversation comment that checks this change against the relevant implementation rules?
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
- For any semantic migration:
  - what is the new canonical owner?
  - what old producer / reader / writer path is now invalid?
  - which production paths were checked for surviving old behavior?
  - is the migration complete?
- For any persisted read-model or canonical surface touched here:
  - is there one canonical write-time owner?
  - is the consuming reader explicit?
  - what bypass or drift path remains, if any?
- For any external side effect claimed in review or close-out:
  - was the authoritative external system re-checked after the action?
  - is the completion claim based on verified state rather than narration?
- Would another implementer understand the canonical owner of this behavior from the code alone?
- Would this change still make sense if two more implementers touched the same subsystem next week?
- Did this patch make the codebase more elegant, more unified, and easier to reason about?
- If not, should this be reframed as an architecture task instead of merged as-is?

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
The goal is to force explicit rule-based review, not to create a ritual template.

Recommended PR description shape:

```text
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
