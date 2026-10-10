# Native Lookup-Miss / Header-Count Cohort

Stacked on mutation seed `a79f3e4e5`; reviewed increment-2 base `13bffdb2e`.
One coherent #2542 family, no PR or additional semantic gate.

The original both-store root and every requested-key, typed-miss, callback-not-run
and before/after-cardinality assertion remain. The setup no longer returns SQL
or reconstructs workflow stores. Canonical native run/source setup precedes the
actual opaque workflow callback/read owner. Blank and absent UUID routes still
exercise the original normalized exact-key miss on both stores.

The count helper's complete caller set is this root's two before/after sites.
It now consumes a detached int64 through storetest. backend/pipelinepersistence
owns the exact original SELECT COUNT(*) FROM flow_instances, with no run, type,
status, eligibility, history or decode filter. The private adapter opens one
original selected native read transaction. Invalid/uninitialized/raw/closed or
cancelled ownership returns zero with an error, never zero-success evidence.
The four actual count reads per root require four native read commits, no writes
and zero active transactions. No generic count/table/query selector is exposed.

Original runtime root PASS under race (26.027s), both stores and both key cases,
no failure/skip. Private original-query equality and native-coordinator proof plus
invalid/closed/cancelled-owner proof PASS under race (24.386s), both stores; the
independent raw expected count is non-vacuous after lawful native construction.
The whole physical SQL scope is also pinned by the independent codemod oracle.

Two finite recipes cover the complete root and count helper, now 92 total.
Independent AST comparison preserves key generation, normalization, callback and
cardinality assertions. Weakened typed/scope/callback/count controls fail. Complete
snapshots, hostile controls and actual combined candidate-overlay type preflight
PASS (40.506s); read-only current-source application must remain inert.

All 78 structural guards PASS; partition/envelope PASS (50.389s), timing contract
PASS (2.132s). Fresh ratchet, 12,541-fact registry, shared fourteen-child census and
hostile raw-parameter/SQL-callback proof PASS (76.530s). All prior children and
all six venue/tier memberships remain. Five exact new private facts are explicitly
classified (three backend, two runtime adapter); no permission/collector change.
The initial unused SQL import and unclassified private findings were refused and
corrected, not granted waivers or execution credit.

Debt `14,790 -> 14,774`: sixteen removed, ZERO added, including multiplicity.
Confirmed raw-operation debt `10,975 -> 10,965`: ten removed. All 67 excluded
uncertainties and the collector remain unchanged. Receipts are retained at
`/home/youmew/.cache/swarm-2542-local-20261007/increment3-lookup-miss-*`.

Authoritative raw-SQL policy records the exact whole-header observation and native
miss obligations. No production behavior change, architecture issue, compatibility
path or framework. Existing parent/watchlist mapping remains sufficient. This
complete root/helper family is canonicalized; bookkeeping, scheduler, remaining
lifecycle/fixture families and global getters stay under open #2542/#2151. Final
zero debt, strict guards, SQLite fork deadline and integrated full remain owed.
