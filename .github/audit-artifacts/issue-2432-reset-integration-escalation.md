# #2432 Reset Integration Contract Escalation

Agent-g, 2026-09-09. Frozen implementation head 34d86df18, incorporating
origin/master@05528a92e. This is a bounded integration finding, not a new issue,
framework proposal, or repeated full pre-audit. #2439 remains a separate repair.

## Finding And Binding Contradiction

G's new agentpersistence/projectLifecycleDiagnosticTx joins pending occurrences to
existing runs and then rejects any outbox row without its run, including rows that
were already acknowledged. That poisons future projection after an admitted reset.

The authoritative platform-spec.yaml section
platform_tables.destructive_reset_cleanup_policy.invariants explicitly retains
agent lifecycle operations, transition facts and diagnostic outbox audit authority
after deleting concrete agents/runs. Its retained table catalog includes the outbox.
The existing cleanup owner implements that policy on both stores. This rule was
missed in G's implemented cleanup census; this is not a new SQLite defect.

#2432 gate 5601046597 explicitly requires selected-fork destructive discard to
remove the queue/event relation and extends coherent aggregate disposition to
whole-parent deletion/reset/nuke. G implemented the selected discard relation but
did not reconcile that condition with the separate reset retention contract. The
candidate proof's generic reset/whole-parent alignment statement is withdrawn.
Neither deleting retained audit facts nor accepting arbitrary missing provenance
is authorized by this report.

## Execution Proof

Disposable tree /tmp/agent-g-2432-reset-probe at the exact frozen head. Companion
issue-2432-reset-probe.go.txt is the complete executable test. It consumes existing
agentfixture.CommitStatic, ProcessCapability, admitRetainedResetCleanupProof,
ApplyDestructiveResetQuiescence and ApplyDestructiveResetCleanup. There is no
direct DELETE, fake cleanup result, sleep, retry, altered production owner or
network/provider call. Required cleanup preconditions succeed before the failing
projector is reached.

Command: go test ./internal/store/internal/runtimepersistence -run
'^TestGDiagnosticAfterAdmittedResetProbe$' -count=3 -timeout=2m

| Store / diagnostic before cleanup | Result per repetition |
| --- | --- |
| SQLite / projected | run=0, outbox=1, later projector returns lifecycle diagnostic run is missing; 3/3 |
| SQLite / pending | same; 3/3 |
| PostgreSQL / projected | same; 3/3 |
| PostgreSQL / pending | same; 3/3 |

Total 12 failures; test package 4.301s. Full log is
/tmp/agent-g-2432-reset-probe.log, SHA-256
2b4ec4c36a4fc9853a34c4d1b5c187f7650b2c031516ef66b65075df994e0e4a.
This probe does not claim coverage of retained
causal or selected-fork provenance after aggregate deletion; those require the
same explicit policy reconciliation, not a missing-run waiver.

## Requested Bounded Disposition

PR #2440 independently implements diagnostic settlement under B/#2436, including
reset-history proof. G/#2432 additionally changes immutable causal/fork provenance,
both activation validators and selected discard. Please record a single canonical
owner and serialized integration direction for these overlapping branches. Preserve
the existing reset retention contract unless an exact authoritative change is
explicitly ruled, distinguish selected discard from reset, and specify projection
versus retained acknowledgement when admitted cleanup removed original run/event
or selected execution history. Do not resolve this by a local mutex, blind missing-
row success, forged causal fields, event/run recreation or a second log owner.

No new issue or POTENTIAL_ISSUES entry. #2432 and #2436 own the concrete overlap;
#2250 retains wider architecture debt. Existing independent #2439 approval stands.

## Verification And Freeze

Before current-master integration the combined full-profile swarm-test exited 0 at
the eb4c8c613 code checkpoint; later source/provenance tests are separate evidence.
That green suite did not exercise this admitted-reset diagnostic combination.
Integration commit 34d86df18 preserves reset complete-set staging/publication and
the existing single-use PreparedStartup owner. Focused dual-store reset candidate,
cancellation, topology-before-timer/schedule, private mock/scaffold, spec and
persistence-inventory tests pass. Those are not diagnostic-reset closure.

The exact-head final swarm-test was queued, then cancelled before acquiring its
slot after this contradiction was confirmed. Only G's queue entry was cancelled;
no other test process was interrupted. No final-head/full-suite pass is claimed.
The earlier PostgreSQL golden-burst timeout remains unclassified despite separate
unchanged focused/full passes; exact failure facts and evidence limits are posted
on #2321 at comment 5603368828. No extra live calls or settled-delivery replay.

Implementation is frozen pending the bounded integration disposition. No combined
PR is claimed review-ready and no final failure-class elimination audit is posted.
