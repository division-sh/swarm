# Selected Tool PostgreSQL Construction

Parent #2542 / #2151. The shared newPostgresHumanTaskToolStoreForTest
constructor no longer receives a sandbox SQL pool or reconstructs a selected
store from it. It consumes the existing StartPostgresRuntimeStoreWithReopen
factory and returns only the original native selected owner. This retains the
canonical platform bootstrap and identical test payload admission. Native close
is registered after sandbox cleanup, so it completes before sandbox retirement.
No reopen is invoked by these consumers and no compatibility path is retained.

Exhaustive consumers: imported entity contract, typed human card/continuation,
sparse generated mutation, entity-field acknowledged/refused response, retired
create refusal, and human-card acknowledged/replayed response. Each remains an
unchanged execution root; the committed AST caller inventory rejects omissions
or added consumers. The independent finite-construction oracle pins predecessor,
new owner/admission/cleanup, and rejects substituted or prematurely closed
ownership. The named completion guard rejects raw constructor parameters.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Shared native construction and six tool workloads | reproduced and fixed | All six inventoried roots under race, retaining their SQLite sibling cases and PostgreSQL execution. |
| Complete finite rewrite and unchanged caller inventory | execution-proven through the same corrected path | TestNativeToolPostgresConstructionRetainsCanonicalAdmissionAndCleanup and TestNativeToolPostgresConstructionCallerInventoryIsComplete, complete candidate overlay and inert replay. |
| Ownership/substitution regressions | execution-proven through the same corrected path | TestNativeToolPostgresConstructionRejectsLostOrSubstitutedAuthority and the raw-parameter native-handler adversary. |

Closure is this constructor family only, not all tool persistence fixtures.
SQLite's different admission posture, entity harness raw returns and unrelated
fork/setup/observation debt remain in the existing parent, without a new issue or
watchlist branch. Existing canonical native construction is reused; no new type
model or lifecycle abstraction is introduced. Final zero-debt/integrated proof
remains open. No PR or aggregate tier run is claimed.

Binding spec: selected_runtime_store_projection.raw_sql_policy,
entity_tool_postgres_construction_fixture. Grouped census/guards, overlay,
complexity and definitive unused qualification are required before push.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment19-tool-postgres-*.
