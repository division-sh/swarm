#2735 conflict-only rebase onto current delivered master.

Source: 7c40e3d7eee94ca1e56c83935d0188e9327ca3e7, signed GOOD.
Tree: 6326ac1900a9a1476cd9d50ca47973b91fcfd7fe.
Base: a1b901ebb3ca387084c8e7a1c7e176d7a90aebb2.
Requested source: 7dc9ac1eab1391d2371e4accb150ed635085d4d3.
Old merge base: 1d3cb55f05f738dc40fb112bac78bd975490e778.

Range-diff: 26 unchanged (=), one obsolete patch dropped (<), no modified or
new patches. All production/memo patches replay unchanged. The only conflict
was embedded_spec_successors_1801_test.go: master #2744 deleted the old whole-
population test and its helpers. The final N19 comparison patch served only
that deleted consumer. Stage-2 and delivered-master blobs were identical;
skipping that now-obsolete patch preserves the complete master file byte-for-
byte. No dead test or helper is resurrected, no extra test is removed.

All five files changed by delivered #2744 are byte-identical to current master;
both ls-tree inventories are retained and cmp passes. No generated row changed
outside an owner; no generated refresh was needed unless a later guard says so.
Actual/expected production LOC for this reconciliation: 0. Adds 0 roots.

This supersedes the unpushed 378df08141 review request. There has been no PR
branch push or hosted run for this head. The signed source is on a non-PR
backup ref so exact-head preflight can verify its uploaded identity.

Original RED #4 remains the known pre-existing #2701 recovery-scope signature,
with this branch's contribution UNCLASSIFIED. No new reproduction campaign,
production repair, timeout raise or test deletion. Recurrence at the eventual
hosted head routes to #2701, with no further run.

Qualification stays lifecycle / local focused, no extra units. Focused guard
and exact-head preflight evidence is appended after execution, with exact
head attribution. No hosted qualification is claimed by this packet.

Affected codemod package: all 374 roots PASS, no failures/skips, 401.992 seconds.
Full log and the machine-extracted root list are retained. Fresh exact-head
preflight was running at the original request; its terminal receipt follows.

FINAL exact-head preflight PASS: 514.209 seconds, all 25 phases passed,
complete=true, zero unstarted. Published preflight/local at the source above.
No generated-row refresh was required. Canonical receipt and all phase logs
are under preflight/; final stdout and fresh machine footer are retained.
Earlier request-machine-receipt remains historical and explicitly stale, not
credited for this source. This is not a second LIGHT request or hosted run.
