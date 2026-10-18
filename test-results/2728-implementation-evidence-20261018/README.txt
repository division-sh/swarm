#2728 local implementation checkpoint

Signed source: ffa979bbaa7ca5cf8e3120e16f6249670ba136de
Base: fe9fb54200cff883ce337b377c02e791d6d195bd
Source backup: agent-d/backup-2728-ledger-deletion-20261018
Ruling: https://github.com/division-sh/swarm/issues/2728#issuecomment-6253279779
Gate D: https://github.com/division-sh/swarm/issues/2728#issuecomment-6254426648

No PR or hosted qualification has been opened. Landing still waits for the
#2735/#2741 prerequisites and the pending LEAD pin/connect scope interpretation.
The separate pin/connect coordinate owners have not been edited. Effective
agent field-source classifications remain live and unchanged.

Production: +222/-840, 1062 touched, net -618. Tests: +130/-536.
Zero new roots; twelve ledger-only roots deleted. No baseline, budget, timeout,
workflow, selector or accounting-policy change.

Proof attribution:
- 2728-module-compile-direct.log: all module packages compiled in the earlier
  implementation, no tests executed. The later private decoder change is
  compiled by the final focused run; no exported signature changed afterwards.
  The preceding swarm-test raw ./... request was refused before execution; its
  log is retained separately, not claimed as a completed compile.
- 2728-focused-final.log: 53 roots passed, one normalization fixture failed.
  The fixture's column-presence mutation no longer matched its new pin shape.
  2728-describe-check-and-census.log closes that failure: seven roots passed,
  including the complete 45-cell compiled describe matrix in update-OFF mode.
- 2728-affected-guards.log: 243 roots passed across the six affected packages.
  No skipped or failing root. This is a retained WIP receipt, not a claim of a
  complete exact-final-head rerun: the pure decoder simplification followed.
  The final preflight retains its own frozen-source guards and interruption.
- 2728-lookup-final-check.log: ten roots pass at the final lookup implementation,
  including original exact coordinates, escaped/inherited slots, absence fallback,
  retained pack bodies and the mixed agent alias/grant reach.
- The retained-artifact native execution leaves passed on SQLite and PostgreSQL
  in 2728-focused-final.log; source bytes/readbacks remain unchanged afterwards.
- Original complexity refusals and the final green canonical owner measurement
  are retained. No complexity policy was changed.
- Final preflight is INCOMPLETE, not green. Every executed proof passed, but its
  trusted-origin/master comparison refused after that ref moved during the run
  from fe9fb54200 to 1d3cb55f05. Eight phases were not started. Do not claim them.
  No repeat was attempted against the moving ref.
- Native unused is NOT green. Its only tracked finding is the unchanged,
  definition-only cataloge2e helper platformSpecPathFromCatalogE2E. The four
  other findings came from an old ignored diagnostic .go copy, now retained as
  .go.evidence with identical SHA256 a1405ddf5c6d220a282ef377a9ee335f3478cfc4f0dee4241b7be343ff0d7910.
  No unrelated source deletion or unused suppression was applied here.

Generated output:
The ten scoped JSON/graph goldens lose only effective_provenance; all other
parsed JSON is exactly equal to the base. The active event-admission corpus
loses only its retired Provenance member; every entry/error/error type remains.
Historical accounting evidence and corpus Before/After records are untouched.

Plan-emission files use the real planner/policy/weights/inventory at this source:
lifecycle + catalog-replay-2-3, 78 units, 13177 selected roots. All 46 directly
touched surviving roots have exactly one primary owner. This is mapping, not
hosted qualification. The dropped overlay unit was not restored.

#1745 handoff: its future expansion implementation must compute provenance when
describe runs, from the admitted expansion, as one text line. No _embed semantics
or eager replacement store is implemented by this patch.
