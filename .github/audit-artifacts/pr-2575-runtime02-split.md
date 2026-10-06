# PR 2575: User-Approved Runtime Full-02 Split

The inherited full-02 overrun is addressed by partitioning its workload, not
by changing runtime behavior, allowances, timing budgets or timeouts. This
follows the user's explicit split instruction after the serveapp split.

## Selection And Evidence

- `store-runtime-flow-lifecycle`: all 32 complete `TestFlow...` roots.
- `store-runtime-full-02`: the other 128 roots selected by the original D-F
  complement. Existing fan-out/process and fork-generation units stay separate.
- Both units execute exactly once in lifecycle/full and remain absent from
  core. Same package, count-1, `ci-postgres-gateway-empty-v1`, broad budget
  class and unset Go timeout. The 240s baseline / 312s fixed buffered ceiling
  is unchanged for each unit. No model weights are seeded from projections.
- Twenty-four original required-child entries move with their flow roots.
  Every SQLite/PostgreSQL, fault, replay, corruption and acknowledgment cell
  stays intact, in its original order. The other eight entries stay with their
  original roots. A source-pinned digest of the combined required-child map
  proves exact preservation rather than merely checking that both stores occur.
- Update the timing-contract inventory, add the complete runtime-persistence
  partition to the catalog inventory, and reconcile the existing flow-constructor
  and fan-out partition consumers. The catalog previously checked serveapp and
  catalog roots but did not enumerate runtime-persistence; it now independently
  covers that entire package as well. No independent partition check is removed.
- The original-union and whole-package source checks prove no dropped, added,
  overlapping or backend-partial root. Negative controls reject omission,
  overlap, foreign selection, partial backend selection, missing backend cells,
  extra cells and duplicate required-evidence ownership.

## Measured Balance

Run37480444184 (PR216760912, hosted merge tree `de424a138`) passed all160
selected roots /1677 test records. Its primary command took386s and package
took342.507s. Top-level roots do not pause or overlap; their measured elapsed
sum is342.480s, so parent durations are meaningful for this particular unit:

| Complete selection | Roots | Observed root seconds | Projected command seconds |
| --- | --- | --- | --- |
| Flow lifecycle | 32 | 189.710 | approximately233 |
| Remaining D-F | 128 | 152.770 | approximately196 |

Command projections reuse that run's approximately43.5s primary overhead.
They are not new-unit qualification receipts: duplicated bootstrap/build,
hosted pressure and queue delay remain variable. The largest indivisible
root is the original both-store cleanup/retirement matrix at126.44s, retained
whole. Master83482f4ad measured409s and a4bad4116 measured387s for the old
command, per the supplied inherited-overrun record; neither is converted into
a passing current budget receipt. Hosted final-head lifecycle must execute
both new selections and pass the unchanged timing evaluator.

## Testpostgres Warning

The generated package weight is22.713s from historical run36230213922;
current run37480444184 measured38.385s. The actual retained historical
primary for that package is23.237s on `2f45f96d1` (2026-09-26), not the
generated22.713s estimate. Comparing those primary receipts directly:

- Executed passing roots grew from93 to113; existing deferred subprocess
  helpers remain explicit and are not counted as fresh passing proof.
- New-root workload accounts for11.83s. Three capacity/quoted-path roots
  account for11.71s: native capacity refusal4.51s, native capacity admission
  4.47s, quoted-path startup2.73s.
- Shared roots account for approximately3.32s additional elapsed time,
  principally descendant-authority joining (+2.19s) and creator-fence process
  lifetime (+0.68s). These timings alone do not prove a new runtime defect.
- Neither #2569 nor #2572 changed `internal/testpostgres` source/tests.
  Capacity coverage was introduced by238ed9414 and then repaired through the
  existing quoted/native fixture stream. #2575 also changes none of that
  package. This warning is mainly workload growth against stale history,
  not evidence that the fixture migrations introduced those slow tests.

No trivial, assertion-preserving correction was identified. Keep the warning
visible; do not skip native capacity/restart proof, lower its realism, rerun
until green, overwrite timing history or inflate its budget in this hotfix.

## Qualification And Scope

Fresh vemew checks: full `internal/testplanning` PASS35.945s, full
`internal/testtiming` PASS6.151s, `TestCatalogRequiredCIProofSelection`
PASS0.516s. Focused original-union, whole-package, pinned both-store/fault
evidence, envelope and mutation controls PASS1.307s. Re-run structural
store guards and independent exact-commit complexity before the single push.
Evidence is under `/home/youmew/.cache/swarm-2542-local-20261003/pr2575-*`.

This is the user's lean shipping choice. G's independently prepared reconnect
deadlock repair is not included unless its exact commit AND reviewer clearance
arrive before this candidate is ready. Future core static-guard promotion belongs
to #2574 after this merges, not this PR. Batch3 migrations remain separate/local.
