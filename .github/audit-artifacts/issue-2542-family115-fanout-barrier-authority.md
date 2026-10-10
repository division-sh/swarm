# Cohort 115: shared fan-out barrier mutation fixture authority

Binding: approved #2542/#2151 existing domain/selected mutation owners. The
advanceFanOutBarriersForTest recipe already delegates to the exact selected
fixture mutation and original pipeline barrier owner. It never consumes SQL.
Remove its unused DB parameter and all 27 forwarding arguments across 16 complete
functions/six files, including fixed-revision fork, completion consumption,
optional effects, rollback/fresh sibling, hostile schedule and history controls.

No read/mutation is moved to a new adapter. Original selected/restarted store,
run/intent, exact clocks, concurrent candidate/supersession cuts, rollback,
completion and physical assertions are unchanged. Other raw setup, history and
fault observations in these private owner tests remain counted, not canonized.

Committed finite AST propagation permits exactly parameter/argument index3
deletion and compares every complete function with actual source. Coverage is
16 functions/27 calls. Hostile selected owner, clock, identity, mutation/error and
barrier assertion changes do not normalize away. Focused race runs four roots
on both stores: mixed lifecycle, restart/isolation, concurrent supersession and
fold rollback/fresh sibling. Candidate type checking covers remaining callers.
Downward census must add no site identity; all original guards remain required.

Value: shared mutation recipe confinement, eliminating stale raw capability
propagation in the whole family; no measured flake-rate claim or removal of all
barrier fixture debt. No production semantics/spec change/framework/compatibility
or new tracker. Parent completion and unchanged SQLite fork deadline remain open.
