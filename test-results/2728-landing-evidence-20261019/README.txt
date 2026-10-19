#2728 landing-source proof packet (no hosted qualification claimed).

Source: fca3652b94470037515a4d2b5f1d6f4f2686cfc3, signed GOOD.
Tree: 3da545007815880cf2d05cdf5b48e1403f85475e.
Delivered master: d282de4ddb5e690c60e30c0534c8db4c9e33fcf5.
Old approved source: ffa979bbaa7ca5cf8e3120e16f6249670ba136de.
Three source patches rebase unchanged (=), without conflicts. Only subsequent
change: canonical registry retirement of the two reviewed deleted-owner rows.
Exact delta check proves zero row additions, two typed-process-local callback
row deletions; policy, dispositions, baselines and all negative controls stay.
Reviewer-d confirmed the live exact-accounting invariant and authorized this
retirement, relayed by the orchestrator. All original RED evidence remains:
https://github.com/division-sh/swarm/issues/2728#issuecomment-6255203907

Production +222/-840, net -618, 1062 touched. Tests +130/-536.
Zero new roots. Twelve ledger-only roots removed under the user's ruling.
EffectiveFieldSources/EffectiveSourceForField remain unchanged. Compiled pin
provenance and pin/connect coordinates remain byte-identical to current master
and are OUT of this PR as fail-closed admission inputs, per LEAD 6254446730.

Final-source focused: 62 roots pass across ten packages, zero failures/skips;
includes all 45 compiled describe cells in update-OFF mode, exact diagnostic
coordinates and absence fallback, typed alias refusals, retained-source native
execution on SQLite/PostgreSQL, and registry hostile controls. No race x3 is
needed because no synchronization or concurrency semantics changed.

All ten scoped JSON goldens differ from current master only by deletion of
effective_provenance; 346838 lines removed. All other parsed JSON matches.
The active event characterization drops only its retired Provenance member;
entries, errors and error types are unchanged. Immutable history is not edited.

Actual planner/policy/inventory selection: lifecycle + catalog-replay-2-3,
78 units / 13166 roots. All 46 directly touched surviving roots have exactly
one primary owner. This is selection mapping, not hosted qualification.

Historical evidence remains linked from the earlier accepted source checkpoint:
https://github.com/division-sh/swarm/issues/2728#issuecomment-6254959503
Its original incomplete preflight receipts are not recast as passes. The earlier
optional native-unused result remains disclosed; no unchanged helper repair or
suppression was added. This packet's final preflight receipt is appended upon
completion and is authoritative for its exact source head only.

Final exact-head preflight: PASS, 1065.785 seconds, all 25 phases complete,
zero unstarted. Published preflight/local at fca3652b94470037515a4d2b5f1d6f4f2686cfc3.
Canonical receipt: preflight/receipt.json. Full logs and fresh machine footer
are included. Earlier incomplete or stale receipts are not credited as passes.
