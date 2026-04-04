# Implementer Review Checklist

Use this checklist before merging any non-trivial change.

This is a practical companion to [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md).

If any answer below is "no", "not sure", or "this patch needs an exception", stop and review the design before merging.

## Stop-Ship Questions

- Is the semantic owner of this behavior explicit and singular?
- Did this change reduce semantic drift instead of adding another local interpretation?
- Did this avoid heuristic fallback for core semantics?
- Did this avoid product-specific leakage into shared runtime layers?
- Did this avoid adding another compatibility branch when a clean migration would work?
- Did this avoid branch-heavy decision trees for core semantics?
- Did this avoid duplicating logic that already exists elsewhere?

## Architecture Purity

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

## Observability

- If this change affects an important runtime decision, can operators see that decision clearly?
- Did we avoid forcing future debugging to reconstruct semantics from indirect logs or tables?
- If a denial, retarget, routing choice, or persistence precondition matters, is it surfaced explicitly?

## Final Merge Check

- Would another implementer understand the canonical owner of this behavior from the code alone?
- Would this change still make sense if two more implementers touched the same subsystem next week?
- Did this patch make the codebase more elegant, more unified, and easier to reason about?
- If not, should this be reframed as an architecture task instead of merged as-is?
