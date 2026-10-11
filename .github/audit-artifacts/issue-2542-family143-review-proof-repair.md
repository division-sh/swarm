# #2542 Family 143 Review Proof Repair

Binding review: issue comment 6090506635; base 61c4029b1. This checkpoint
repairs F143-1 and F143-2 only. It does not change production admission,
persistence, timer policy, routing or deadlines. No freshness rebase, core/full
run, new issue, PR, framework, compatibility path or vendoring.

## Owner And Consumer Corrections

The reopened compiled-timer witness attaches an observer to the actual
successor coordinator before replay and wires its timer publication, dispatch
and logging ports to that observer. Duplicate wakeup/accepted-event and
same-stage stale-generation windows now require zero successor publications
and commits as well as the predecessor's original single publication. Exact
durable workflow, history and activation checks and joined predecessor teardown
remain unchanged. All seven operation variants run on both backends.

The foreign human-task read collaborator changes only one coordinate. Decision
still changes EntityID only. Deferred and expired outcomes change FlowInstance
only to provider/foreign, restoring the original separate refusal. Every case
retains native card/decision/deferral/expiry, complete unchanged continuation,
zero refused writes/publications/dispatch and successful exact requester delivery
with one post-persistence dispatch. The collaborator grants no write authority.

Canonical owners remain the selected native construction/mutation, timer
lifecycle, publication planner, original bus/dispatcher, card/continuation,
reply-context, expiry and requester-admission owners from the family143 audit.
These repairs consume those same owners, not new interpreters.

Sibling inspection: workflow_timer_owner_test.go's sibling-declaration,
recurring-coordinate and rollback/cancellation reopen witnesses bind restored
schedules to the new coordinator, without inspecting a predecessor bus for
successor publication. nativeExactWorkflowJoinHarness.installCoordinator
rebinds the retained observer's Bus and publication planner on restart.
nativeActivityBoringFixtureForTest.reopen installs a fresh observer on the
returned coordinator. No additional predecessor-only replay observer found.

## Exact Proof

Receipts use ~/.cache/swarm-2542-family143- on vemew, Go1.26.8,
GOMAXPROCS=3, with the real selected PostgreSQL harness and SQLite stores.

| Manifestation | Disposition | Exact execution proof |
| --- | --- | --- |
| Successor duplicate/reconstructed timer republication | reproduced and fixed | review-repair-timer-human-race.jsonl: TestPipelineCompiledTimerTransitionEvidenceOnBothStores, all 14 operation/backend cells; successor-observation-negative.jsonl: duplicate cut injects real successful DispatchPostCommit and requires the publication refusal in advance, emit_only and loop_emit_and_self_advance on both stores |
| Stale same-stage generation dispatch escaping reopened observer | reproduced and fixed | Same race root; negative receipt's stale_generation cut injects actual successor DispatchPostCommit, requires its success marker and the unchanged-lifecycle refusal in loop_emit_and_self_advance on both stores |
| Decision foreign EntityID refusal | execution-proven through the same corrected path | review-repair-timer-human-race.jsonl: TestHumanTaskDecisionRoutesDirectlyToRequesterInOneMutationOnBothStores |
| Deferred foreign FlowInstance refusal | reproduced and fixed | Same receipt: TestHumanTaskDeferredAndExpiredOutcomesUseRequesterRouteOnBothStores, expired=false on SQLite/PostgreSQL |
| Expired foreign FlowInstance refusal | reproduced and fixed | Same receipt/root, expired=true on SQLite/PostgreSQL |

Positive aggregate PASS 70.490s under race. The injected-publication outer
control PASS 148.375s: all six duplicate-cut and two stale-cut backend cells
fail only after the real dispatch success marker at the required assertion.
Compilation/setup failures, absent subtests and successful mutants cannot pass
the control. No product defect is inferred from these intentional mutations.

Unchanged 84-root family143 evidence is reused, not relabeled exact-head
aggregate qualification. review-repair-census.jsonl PASS 53.047s confirms the
unchanged 10,942 / 7,946 / 67 debt totals and the existing registry. Seven finite
retirement/successor/preflight roots PASS 2.633s in
review-repair-recipe-controls.jsonl. review-repair-codemod-inert.json remains
Write=false, Changes=[]. review-repair-unused.log is the successful native
default/race/issue2413 sweep, not Linux/Darwin union qualification. The exact-head
complexity report is required before push and accompanies the issue checkpoint.
platform-spec.yaml parses; historical recipe pins and collector are unchanged.

## Residual And Disposition

Debt before this proof repair: 10,942 findings / 7,946 raw sites / 67 excluded
uncertainties; this repair introduces no raw authority or collector change.
Ten disclosed historical workload-model failures remain integration/landing
blockers under the separately bounded authorization in the same review.
No aggregate historical codemod pass, SQLite fork-deadline qualification,
zero-debt completion or parent closure is claimed. #2542/#2151 stay open.
Existing watchlist refinement acbd003 covers observer ownership across
replacement and coordinate-specific refusal; no additional node or issue.
