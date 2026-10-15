# Channel fault acknowledgement repair

Track: FULL. Binding authorization: reviewer-b6184610797. Bounded repair of
a2a21b66dd, stacked after metadata-only777e1d59ff; no new migration credit,
baseline/collector change, historical caller rewrite, workflow or timeout change.

## F1: Responsibility Is Not A COMMIT Receipt

Both opaque faults now retain cleanup responsibility as soon as their existing
selected-coordinator mutation callback starts. An install error is returned
unchanged; it no longer proves that storage is unchanged. Pre-admission
cancellation still executes no callback. An outstanding cleanup prevents another
install, and only an acknowledged reconciliation discharges responsibility.

The existing channeldelivery receipt owner recognizes the exact captured original
render as a no-op, including intact retained history after a current-plan advance.
Otherwise restoration retains the original exact render/plan/card CAS. The
runforkpersistence receipt owner recognizes absent binding plus the exact captured
ordinary run as a no-op; otherwise its original exact binding DELETE applies.
Neither owner overwrites changed replacements. Failed restoration stays armed,
including when physical restoration committed but its acknowledgement was lost.
No new public SQL/transaction callback or generic fault mechanism is introduced.

The regression uses the existing stopCommitConnector around the real native
driver and original selected coordinator. Each injection invokes physical Commit
or Rollback exactly once and returns its independent acknowledgement error.
Storage is inspected after the cut and after two cleanup calls. Both stores and
both faults exercise install-committed, install-rolled-back, restore-committed and
restore-rolled-back:16 cases. Original error identity and complete original
observation are asserted, not a committed flag or successful API return alone.

The deliberate old-wrapper-policy counterexample fails all four committed-install
cells (SQLite/PostgreSQL x render/binding), leaving the durable fault visible.
The repaired policy passes the16-cell matrix under race. Ordinary native controls
retain every prior scope, cancellation, foreign/absent, successor, full-identity
and zero-execution-authority assertion. Added checks cover repeated cleanup,
unchanged retained history after stale install and refusal to clean another
capture's installed binding. Only the two authorized complete-source digests
advance; historical caller Before/After records and the third native pin remain.

## F2: Exact Qualification Holds

The original audit now records reviewer-b's accepted162/122, not the proposed
180/136. Abrupt-input contributes an additional held6/5 at
proveChannelInputCommitDeath: SQLite plain true/false and chosen true passed;
chosen false failed. PostgreSQL plain true/false passed, but PostgreSQL chosen
and both skip branches are unproven. The filtered root PASS is not the complete
12-cell matrix. No additional abrupt-input run or causal attribution is claimed.

The F1 render12/9 and existing selected-binding9/7 remain separately visible
pending review/qualification; this repair does not pre-award held caller credit.
Physical4646/2788, new batch holds88/74, prior holds87/70, qualification-adjusted
4821/2932 and accepted cumulative1812/1490 remain the authoritative accepted
ledger until reviewer-b dispositions this repair. The physical36/26 residual is
already in the baseline, not a second qualification hold.

## Receipts

Evidence prefix: /home/youmew/.cache/swarm-2542-family145/channel-fault-.
The bounded repair uses affected controls only; unchanged migration, source and
ratchet receipts carry. No broad local tier, server2 run, push or rebase.

| Manifestation | Proof |
| --- | --- |
| Durable install with failed acknowledgement | uncertain-commit-old-policy-red.jsonl: all four counterexamples RED; commit-and-boundaries-final-race.jsonl: corrected matrix. |
| Actual rollback/no write | Same16-cell matrix: physical original state and successful no-op cleanup. |
| Restore committed or rolled back with failed acknowledgement | Same matrix: exact physical cut, original error, two successful subsequent cleanups. |
| Foreign/stale/replacement and cancelled operations | native-controls-final-race.jsonl; individual completed cells, not aggregate success, are authoritative. |
| Historical source/accounting preservation | source-controls-pinned-final.jsonl: three exact finite/inverse/history/native-inventory roots PASS24.816s. |
| Abrupt-input coverage gap | Original receipts are transcribed exactly into the qualification ledger; no new execution claim. |

Final commit-and-boundaries-final-race.jsonl PASS123.157s:16 physical commit cuts
plus four closed event/delivery/lifecycle/selected-store structural roots. The
standalone first16-cell receipt also passes35.934s. The old-policy counterexample
is intentionally RED20.610s. No race/timeout/retry policy changed.

native-controls-final-race.jsonl is RED102.867s, not a green aggregate. The render
PostgreSQL cell passes21.700s, and both binding cells pass35.330s/11.700s. Render
SQLite fails at the existing5s event.publish call BEFORE capture or install
(main_runtime_test.go:5147). It was not rerun; this receipt does not classify its
cause or prove the complete served render control. The subsequently extracted
replay/foreign-cleanup assertion helpers contain the identical executed checks;
the final exact-source inventory is green. No native assertion was removed.

Touched semantic/regression functions are below both30-point complexity limits;
the strengthened native assertions are factored into named helpers to avoid new
hotspots. Diff whitespace and final U1000 pass. The final compile-only receipt
passes0.043s; neither constitutes runtime qualification.
Plain go vet reports five unkeyed composite literals in the untouched
scenario_setup_observation_test.go and standing_diagnostic_error_test.go. Those
files are byte-identical to777e1d59ff, so that command is disclosed RED, not
misreported as a green quality receipt or clean-master inheritance.

No preflight is claimed on this pre-preflight retained base; the real command
still waits for the authorized A+D integration rebase and eventual push. The
existing2353/2542 runtime holds stay open; no new issue or architecture stream
is needed. This changes test-fault cleanup only, not a platform runtime contract.
