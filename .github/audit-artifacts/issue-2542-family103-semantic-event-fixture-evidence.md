# Cohort 103: complete semantic event fixture comparison

Approved #2542/#2151 selected construction, mutation and raw_sql_policy govern.
The existing ReadSemanticEventFixtureEvidence owner already composes complete
eventrecord, closed delivery physical projections, pipeline facts and the original
runforkrevision cardinality accessor in one original selected read snapshot.
This family migrates its missing canonical-mutation/unrevisioned comparison consumer;
no new owner, port, SQL, permission or observation framework is needed.

Both comparison stores are independently constructed native owners. The terminalization
sibling also uses native PostgreSQL construction. Delete all four raw helper definitions:
fixtureRevisionCount, fixtureDeliveryCount, fixtureLoadedEventRecord and
fixtureDeliveryProjection, including pool/dialect arguments and direct SQL loaders.
These names had no other callers after the complete family propagation.

The complete persisted eventrecord Equal assertion is retained, not replaced by a
decoded-event subset. All 18 original physical delivery projection fields are retained
in the existing map, with exact computed delivery IDs and explicit presence checks on
both sides. The unique delivery primary key makes map cardinality the exact physical
event delivery count. Revision conservation for unrevisioned insert/duplicate and
strict growth for the canonical mutation remain; initial absent-event snapshots are
supported by the existing owner. Both event presences are now explicitly required.

Race proofs: both affected roots on both stores, plus existing native semantic-event
evidence original-read, cancellation/wrong-run/no-partial/closed/refusal controls.
Immediate terminalization keeps exact dead-letter identity, count, database-clock
millisecond precision on SQLite and non-regressing updated_at assertions.
Two finite whole-root recipes/type overlay preserve the independent stores, UUIDs,
routes, source, clock, insert/duplicate dispositions and all original assertions.
Hostile controls reject missing record/projection evidence, weakened equality/counts,
wrong selected owner and retired helpers; the closed owner still supplies all18 fields.

Value: shared comparison now consumes canonical physical evidence, reduces cross-read
skew and completes native setup for both proof roots. Confinement/snapshot reliability,
not measured flake-rate reduction. Parent raw/getter/protocol debt, strict completion
guards and final qualification remain open; existing architecture/watchlist unchanged.
