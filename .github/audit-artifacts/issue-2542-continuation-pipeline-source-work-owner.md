# Shared Native Pipeline Source Work Ownership

Priority source: #2542 fixture-brittleness map6049458044 and reproduced
source-specific fixture failure recorded with increment30 at6049786287.
Existing native pipeline bus/coordinator setup has 55 calls in 51 functions;
the work fixture is its shared existing lifetime owner, not a new framework.

The deterministic race probe reproduced requested sourceB receiving sourceA's
RuntimeOccurrence. The fixture cache keyed only testing.T and ignored every
later source fact. All occurrence identity stays with the canonical core
worklifetime.RuntimeIdentity; no grant, attachment or source-set policy changes.

Repair: retain ONE Process per test; serialize runtime construction on that
original process and cache child occurrences by the complete RuntimeIdentity.
Validate the requested SourceArtifactFact before cache lookup. The bus's
automatic work owner consumes its actual selected fact rather than a default
fact; explicitly supplied work owners remain untouched. Every other bus
registration, payload admission, durable port, descriptor and finalizer
statement is preserved by the finite recipe.

Cleanup first fences the process and every runtime, then joins every runtime
and finally the process through existing lifetime operations. Failed joining
retains fixture responsibility; successful joining alone removes its cache
entry. The unchanged5-second fixture bound is not increased. No persistence
callback, raw getter, fake transaction/authority or compatibility is added.

## Consumption And Exact Proof

| Surface | Disposition / proof |
| --- | --- |
| Source-specific fixture/context callers and automatic bus setup | Same corrected owner; exact requested source identity, stable same-source reuse, unchanged caller signatures and one shared process from actual lease contexts. |
| Concurrent lookup of the same source | Reproduced and fixed; sixteen concurrent lookups under race return one occurrence, with one retained runtime in the original process. |
| Multiple source teardown | Real leases on both source occurrences settle before automatic cleanup; after child completion both reject new work and process active count is zero. |
| Failed teardown with an actual held source lease | Cancelled join fails, both runtimes are fenced, the live process and fixture responsibility remain; settling the exact lease permits the existing close operation to complete. No added retry policy or delay. |
| Handler receiver claim/receipt/deferred handoff and timer replay | Named existing roots run on both stores under race through the corrected shared fixture; every original assertion stays. |
| Source substitution, foreign process, wrong cache identity or process-only join | Finite recipe shape/hostile controls plus current-source/type overlays refuse the changes. |
| Other raw fixture/construction families | Still explicit parent debt; this repair earns no SQL-removal credit or class-wide closure. |

Receipts use `/home/youmew/.cache/swarm-2542-local-20261008/pipeline-source-owner-*`.
Initial new proof mistakes are retained: cleanup callback signature failed to
compile, and a context without an actual lease did not carry process authority.
The repaired proof reads the canonical process owner from real lease contexts;
no production assertion or workload was weakened.

Fresh guards/census/registry, complete codemod/types/inert replay, native unused,
contracts and exact committed-head complexity precede the next increment push.
Existing watchlist suffices; no tracker, public semantic migration or production
architecture change. Parent zero debt, strict completion guards, SQLite fork
deadline and final integrated qualification remain open.

Final vemew sweep: 252 passing roots with no selected failures/skips; seven
focused lifetime/handler/timer roots under race, including all selected backend
cells; 153 complete codemod/type/hostile roots and all247 finite recipes;
the exact debt/registry root set plus all16 native family children, all78
structural guards and five contracts. Native unused explicitly exited0;
inert replay has no changes. The ledger stays14,024 findings /10,332 raw sites:
this reliability/identity correction earns no fabricated SQL removal credit.

Sibling probe: runtimeTestWorkFixture caches ONLY its process, not a runtime
occurrence by an ignored source argument, so it does not share this source-cache
defect. Other construction/role/wiring families remain in the priority ledger.
