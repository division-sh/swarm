# Family 139: whole served delivery-quiescence caller family

## Conversion and Canonical Owner

149 existing Go call sites in 80 functions now receive a domain read function,
not a database and dialect selector. 73 whole-function finite recipes are
mechanical: an independent AST oracle changes only the owner argument and removes
the dialect argument, preserving every other statement and assertion. 20 separate
source-pinned recipes cover the shared consumer and construction/handoff work.
Together this is 93 recipes, with no prior recipe overlap.

The real owner is the existing delivery lifecycle RunSummary implementation.
In-process construction captures DeliveryStore.SummarizeRun from the exact serve
dependency projection before startup and binds it after readiness. Restarts
capture the next original dependency projection; no pool lookup or reconstruction.
The shared runtime fixture carries the read function without giving the waiter
SQL, mutation, registration or close authority.

Child/release processes use the existing independently owned ReadOnlyInspection
and InspectSnapshot. Its schema is intentionally not runtime-admitted. The first
attempt to call the runtime-admitted summary method correctly refused both stores.
The correction adds ReadServedRunDeliverySummary: the existing original read
transaction calls the original delivery owner SummarizeRunTx. No new query,
runtime admission, bootstrap, writer or coordinator is introduced. Three exact
private transaction operations are classified fixture-2151, not globally waived.
Invalid/uninitialized owners refuse. Inspection close remains joined and owned.

## Predicate and Execution Proof

The exact old predicate is Pending + InProgress, NOT RunSummary.Settled and NOT
the broader run.diagnose active-deliveries field, which includes retry-scheduled
work. Four consecutive zero observations, reset on active work, the same 25ms
poll interval and the same deadline remain. Read errors, foreign run identity and
invalid summaries fail closed. Timeout diagnostics now include the canonical
complete summary rather than using a raw database diagnostic helper.

| Manifestation | Proof and disposition |
| --- | --- |
| All149 original callers | Whole owned-Go caller inventory:80 functions/149 calls, plus one new unit call. All3-argument domain readers; no raw/dialect wait survives. 73 AST-equivalence and foreign-owner hostile controls; all93 exact candidate snapshots typecheck. |
| Active/stable/retry status cut | TestServedRunDeliveryQuiescenceReaderPreservesStableStatusCut: pending/in-progress reset stability, exactly four terminal samples, retry-scheduled remains outside the old predicate. |
| Original native served owner | TestReceiverCompositionMixedAgentBothStores and TestServedForkSourceCompletionWithoutForkBothStores, race, both stores. |
| Reused-store process/cursor restart | TestServedMailboxCursorRetainedRestartParity, race, both stores. |
| Fresh scratch/fork epoch | TestRunServeRuntimeDevScratchRunForkLifecycleSQLite, race; no false PostgreSQL claim for this explicit SQLite surface. |
| Real child graceful drain/abrupt death/replay | TestServedMailboxCompletionProcessBoundariesBothStores, four cells under race, passed after repairing the read-only/admitted-role distinction. Existing exact receipt/history/replay assertions unchanged. |
| Missing or foreign native read owner | TestServedRunDeliverySummaryRequiresOriginalInitializedOwner, race, six refusal inputs. |
| Excluded issue2413 source | The two calls are mechanically converted, the tag and deferred R7 acceptance are unchanged. Explicit issue2413 compile-only passes; no deferred acceptance execution/qualification claimed. Ratchet disposition BLOCKED below. |

Seven positive roots /19 pass records across the affected executions. The first
aggregate was RED solely at four read-only/schema-refusal cells; those four were
subsequently repaired and executed, not retried without a change. Other positive
roots are unaffected by that bounded private-port repair.

## Checkpoint and Residual Boundary

304 codemod roots /665 finite recipes pass, including candidate type overlays,
full-workload preservation and hostile controls. All78 structural guards pass.
Registry verifies13,923 exact findings. Native guard/16 children pass. Inert
codemod Changes=[]; issue2413 compile-only is explicitly not execution evidence.
Native default/race/issue2413 unused exits0, not a Linux/Darwin union claim.
Held-head complexity is not yet qualified because this family is not committed.
No production behavior/spec, timeout, budget, tier, source inventory exemption,
new framework or compatibility change. Remaining raw reads/construction in these
caller files and broader fake manager/pipeline protocols remain #2542/#2151 debt;
this is whole quiescence-consumer closure, not whole served-fixture closure.
Archived future-capability .go.txt material is not live Go and remains an explicit
non-execution exception, not a compatibility path or restoration candidate.

The unchanged ratchet initially failed on ONE changed excluded declaration fingerprint:
internal/serveapp/a2_nested_portfolio_isolation_test.go,
TestA2NestedPortfolioSamePeriodAndMemberIsolationBothStores.
Old facfe2d5f45cc0bfd4df3a8a37eddc3609a5f880fd001b91a277966bf6d9a802;
new 02a974ebc762c71be9999ece645b4d35076f0518811a05b09851433a9a67ff06.
The exact two-call conversion changes its conservative whole-body uncertainty
identity. Ruling6074823946 authorizes exactly this forward comparison transition.
Extraction, the tag, source context and uncertainty count stay unchanged. All
three subset comparisons, independent source extraction and multiplicities are
preserved. Both identities, reverse/foreign/multiple debt, unrelated additions
despite lower totals and old-source resurrection reject. The transition becomes
inert when the trusted source contains the new identity. No generic exception,
permanent old-hash alias, rebootstrap or classifier weakening is added.

The exact hashed ratchet-policy transition is b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7
to12eb747fcb2ea9928e6a57cfaf50bdb0b5b2236345df6c799bc44f7ee91a08ff.
Origin and permission code remain hashed; only the pinned destination constant
uses the existing non-self-referential G01 arrangement. The new fingerprint is
written to the baseline, with no old identity retained.

The full affected30-root control set and real downward refresh pass121.232s.
The normal unmodified/non-refresh ratchet separately passes36.643s. The baseline
is now12,462 findings/9,154 raw sites, down153/152, zero added/increased debt,
all67 uncertainties retained. The accounting transition earns ZERO reduction.
Unchanged family-native/race,665-recipe,78-guard and unused receipts are reused;
only affected accounting controls, the fresh census and inert check are rerun.
Exact committed-head complexity is required before push. No tier/closure claim,
new issue or PR. Existing parent/watchlist own the operational boundary. Receipts:
~/.cache/swarm-2542-local-20261009-family139-* and increment138-139-*.
