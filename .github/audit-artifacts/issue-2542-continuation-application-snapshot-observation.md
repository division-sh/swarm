# Canonical Whole-Application Snapshot

Routine #2151/#2542 migration with no new SQL owner, reader or private registry
allowance. snapshotCatalogApplication now consumes the existing closed
ReadSelectedForkApplicationStorageSnapshot operation through the original
native harness reader. Schemastore supplies identical SQLite/PostgreSQL base-
table inventories; one native read transaction retains repeatable/coherent
inspection, all columns and sorted rows. No table/query/callback selector or
raw getter escapes. Every unavailable/late-failing snapshot returns no data.

The old manual DB transaction/table scan and row serialization are deleted.
Canonical typed table receipts are returned verbatim, not converted back into
the old byte-to-string representation. Physical columns/value types, null,
bytes/text and numeric distinctions survive; timestamps normalize only location,
not instant. This strengthens the immutability witness and removes a second
physical renderer. No compatibility formatter, new framework or runtime policy.

Original connected-history test body is unchanged: join source runtime writers
while retaining process capability, capture before, execute two real selected
fork attempts, require exact typed route-history refusal/text, forbid every
fork work/result field, compare the complete after snapshot and report table
differences. Both future success artifact hashes remain untouched. Complete
consumer shape and hostile controls reject wrong owner/context, ignored errors
or filtering; separate structural proof pins join-before-read and all original
refusal/work/conservation assertions.

Named real connected-history refusal and future-artifact roots pass under race
on both stores. Five existing canonical owner roots pass under race: complete
physical table inventory, invalid/canceled/closed/partial refusal, byte/text/
null/numeric distinctions, equal-instant timezone normalization and incomplete
row refusal. Reuse those supported owner proofs rather than inventing a mock
snapshot or duplicating SQL. No workload/assertion/retry/skip/timeout weakening.

Finite inventory: 295. No private raw additions; classifier, policy, source
ownership and guard permissions unchanged. Existing watchlist mapping suffices.
Final static/type/hostile/78-guard/census/registry/all-family/contract/native
receipts, native unused exit zero and inert replay pass. Exact-head complexity
is checked before push. No aggregate core/full, hosted CI or Darwin-union claim.
Receipts: ~/.cache/swarm-2542-local-20261008/catalog-application-snapshot-*.
Remaining catalog construction/snapshots and parent zero debt/strict closure/
SQLite fork deadline/integrated qualification remain explicitly open.
