# Catalog Observation Clock Ownership

Routine bounded read migration within approved #2151/#2542. The catalog's
shared observation-start helper was an entry point, not the whole catalog
construction boundary. Its PostgreSQL SELECT NOW() and SQL handle input now
stay behind the original native selected read coordinator, through the
existing clock observation package. The public fixture port returns only a
timestamp, never SQL, a pool, a transaction, a callback or a reconstructed
store. Foreign/SQLite/absent owners refuse PostgreSQL clock observation.

The application-time capture, PostgreSQL UTC normalization, earlier-of-two
clock rule and one-second overlap are unchanged. SQLite still uses exactly
its application-clock-minus-one-second path; it neither acquires a server
clock nor gains a new backend policy. The constructor still samples an
initial clock boundary unconditionally, then replaces it with the transcript's
original observation boundary during replay; this preserves the old behavior.

## Consumption And Proof

| Surface | Disposition and exact proof |
| --- | --- |
| Shared constructor's initial observation boundary | Moved to native PostgreSQL owner; cumulative finite constructor recipe changes only the argument of the same observation cut in addition to its previously approved scenario setup. |
| PostgreSQL server clock | Exact original coordinator read commit, no write, no retained active transaction; cancellation and closed native owner fail without a timestamp. |
| SQLite observation start | Different clock concept: focused boundary proof brackets the exact application-clock cut, with no selected PostgreSQL owner. |
| Both-backend fresh/reopened evidence append | Real catalog execution/replay/reopen root preserves exact findings and two result publications despite duplicate input. |
| Clock policy drift | Independent finite clock oracle rejects altered dialect, author context, native store, local-clock fallback, UTC/minimum rule and overlap. |
| Remaining catalog raw constructors/reads and hand-wired runtime graphs | Still explicit migration debt. Constructor migration must not introduce a new raw getter to retain these consumers. |

Qualification includes focused race proofs, complete finite recipes/type
overlays, all structural guards, decreasing census/registry, native unused,
planner/spec contracts and exact committed-head complexity. Receipts are under
/home/youmew/.cache/swarm-2542-local-20261008/catalog-clock-*.
No production clock/runtime semantics, spec rule, framework, compatibility or
watchlist mapping changes. Parent zero debt, strict guards, fork deadline and
integrated qualification remain open.

Initial complete sweep correctly rejected the three unclassified private
clock read occurrences and an older constructor oracle still expecting
per-handle selected.Close(). The three source-reviewed read/scan/coordinator
occurrences are now explicitly fixture-2151 within the unchanged private
boundary. The old oracle now compares the actual factory against its exact
shared-close-owner recipe, rather than weakening cleanup checks to a generic
substring. The G01 collector and permission model are unchanged. Injected
premature-dispatch executions fail as intended inside their passing negative
control; those subprocess failures are not hidden candidate failures.

Measured decreasing debt: 14,024 findings / 10,332 raw sites becomes
14,020 / 10,329: four identities removed, zero added. The refresh command
accepts SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT=downward, not =1; the two
incorrect-value invocations remain preserved as command-validation failures,
not qualification credits. The corrected final census, registry and native
adversarial controls pass with all 16 required family children present.
