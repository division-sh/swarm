# Maintenance And Cleanup Watchlist

Canonical home for decomposition, dead code, conformance-fixture coverage, and legacy compatibility removal.

## Active Issues

- `#125` Wire excluded runtime catalog conformance fixtures into e2e execution.
- `#126` Split boot verification into boundary-owned modules and close major test gaps.
- `#127` Remove or implement the inert post-turn contract enforcement path in the LLM agent.
- `#128` Consolidate event bus publish paths and remove dead publish variants.
- `#157` Remove legacy schema and error fallback helpers from runtime/store hot paths.
- `#158` Remove the exact-schedule compatibility shim after canonical identity lands.
- `#159` Legacy compatibility removal wave.

## Reserve Backlog

- `30.` Add short canonical-owner comments at key architecture seams.
  - combined follow-on priority: issue `#173`
- `33.` Collapse duplicate turn canonicalization ownership behind one canonical persistence adapter.
  - combined follow-on priority: issue `#171`
- `34.` Add typed structs for canonical `turn_blocks` payload shapes.
  - combined follow-on priority: issue `#171`
- `40.` Reduce `runtime_state` JSON sidecars as control-plane owners.
  - follow-on priority: issue `#172`
- `43.` Collapse schema-capability branching toward a narrower canonical store schema.
  - already covered by issue `#157`
- `44.` Add reusable invariant suites for completed architecture seams, not only issue-local regressions.
  - combined follow-on priority: issue `#173`
- `Lower Priority 5.` Keep maintenance-only file decomposition work moving in oversized seams that no longer own active semantic migration.
  - follow-on priority: issue `#174`
