# Cohort 124: public run-control fixture

The complete PostgreSQL pause/continue/stop fixture now uses native construction
and the existing RequireRun lifecycle recipe with identical origin/hash/start.
All public idempotency, repeated-operation typed refusal and postcommit recovery
assertions remain. The only status-reader consumer moves to ReadRunStopStorage.

That existing original snapshot returns the identical runs LEFT JOIN control
state and COALESCE empty default; the finite oracle compares physical predicates
with only whitespace, alias and UUID representation normalized. Extra pending
and revision fields are not credited as new proof. No SQL or owner added.

Existing whole-root recipe is extended, not duplicated. Complete source/hostile
owner and assertion controls cover the three state reads and sole helper. Focused
race runs the original public root plus the original both-store read-owner control.
No production runtime/spec change or final closure; sibling fixtures remain debt.
