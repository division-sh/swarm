**Phase 3 Completion Plan**

1. Finish generic runtime API cleanup.
- Remove remaining platform-owned `vertical` terms where the platform controls the type or method name.
- Focus areas:
  - `internal/runtime/manager`
  - `internal/runtime/tools`
  - `internal/runtime/llm`
  - `internal/runtime/mcp`
  - `internal/runtime/workspace`
- Rule:
  - add or flip to entity-oriented names
  - keep old vertical names only as compatibility wrappers where needed

2. Shrink generic actor/config compatibility.
- Continue moving generic code to `AgentConfig.EntityID` first.
- Keep `VerticalID` as compatibility only.
- Audit remaining generic reads of `actor.VerticalID` and replace them where the code is platform-owned.

3. Eliminate remaining Empire payload/type ownership in generic runtime.
- Finish the long tail:
  - manager/runtime payload structs
  - helper DTOs
  - any product-owned structs still defined in generic packages

4. Clean remaining generic runtime vocabulary.
- Burn down product terms in generic package names, helper names, logs, constants, and comments where safe.
- Especially:
  - `opco`
  - `vertical`
  - `brand`
  - `geography`
  - `holding`
  when they are not part of an intentional Empire compatibility schema.

5. Continue legacy `verticals` retirement outside core runtime.
- Move outward from platform core to product/reporting surfaces:
  - `internal/dashboard`
  - `cmd/empire`
  - `internal/ops`
  - `internal/empire/factory`
- Goal:
  - make `workflow_instances` the active truth
  - leave `verticals` only as explicit legacy/reporting compatibility where still needed

6. Re-audit tests against the original Phase 3 test plan.
- Re-run the original lexical/package audit for:
  - rewritten tests
  - moved product-owned tests
  - remaining generic Empire vocabulary in test files

7. Keep expanding the conformance runner honestly.
- Turn on only the catalog tiers and features the harness truly supports.
- Remaining runner work:
  - guard semantics
  - richer accumulation
  - list-processing assertions
  - event-loop and dead-letter assertions

8. Final Phase 3 closure audit.
- Generic runtime packages should not own Empire DTOs, payloads, or helper files.
- Product code should live under `internal/empire/**`.
- `verticals` should no longer be the live authority for platform runtime.
- Remaining Empire vocabulary in generic runtime should be explainable as compatibility only, not architecture.
