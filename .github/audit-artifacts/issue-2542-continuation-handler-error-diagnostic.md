# Handler-Error Diagnostic Read

Routine exact-storage migration under #2151/#2542 and the authoritative
raw_sql_policy.allowed private bounded fixture rule. The last handler failure
dump consumes eventrecord.ReadLatestHandlerErrorLog through the original
selected read coordinator and its detached storetest bridge. Its fixed type,
details.action predicate, all-run scope, created_at DESC/LIMIT 1 and native
payload bytes remain. Missing rows yield no diagnostic, not runtime success.
Read failure is now explicit in the fatal message rather than silently ignored.

The primary handler success assertion, failure bytes and every receipt/replay
check remain. Both trigger and handler assertions no longer interpret h.db
presence as read authority: the existing native reader resolution refuses
missing ownership. Existing complete-body receipt oracles reconstruct both
reviewed projections and retain every other statement. Mutants reject lost
owner/context/read error and inverted primary failure. The sole new finite
recipe drops h.db from the real both-store receipt control without changing
its workloads or assertions; cumulative receipt recipes retain both callers.

| Manifestation | Proof |
| --- | --- |
| Latest matching diagnostic and newer unrelated action | TestCatalogHandlerErrorDiagnosticPreservesLatestPhysicalFilterBothStores writes canonical RuntimeLogFacts, authenticates payloads against the admitted source, persists through PersistRuntimeLog and checks the exact selected result on both stores under race. |
| Missing/canceled/closed/foreign reader | Same proof returns no diagnostic evidence on absence/refusal; successful read requires one original commit, zero writes and no active transaction. |
| Receipt authority independent of raw presence | TestCatalogLatestPipelineReceiptUsesOriginalReadOwnerBothStores passes after h.db is removed, retaining real trigger, handler and replay assertions and joined shutdown. |
| Failure display/selection drift | Complete-body and fixed owner-projection oracles preserve the primary assertion and exact query; negative mutations fail. |

Initial generic event publication was correctly rejected: runtime logs require
their named persistence operation. The control now uses that operation, not a
raw seed or admission bypass. All receipt/diagnostic errors remain meaningful.
Finite inventory: 279 recipes. Per-family guard/census/finite proofs precede
the commit; the next increment sweep also supplies full codemod/type/hostile,
native unused, contracts, inert replay and exact-head complexity before push.
Receipts: /home/youmew/.cache/swarm-2542-local-20261008/catalog-handler-diagnostic-*.
No production semantics, spec rule, collector permission or framework change.
Existing watchlist mapping suffices; remaining catalog construction, zero debt,
strict guards, fork deadline and final integrated qualification remain open.

Per-family proofs: 110 passing roots, zero failures/skips. Debt decreases from
13,937 findings / 10,261 raw sites to 13,934 / 10,258: three findings/sites
removed, zero added. Five exact private read/scan/coordinator occurrences are
source-classified fixture-2151 under unchanged policy.
