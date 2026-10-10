# Cohort 109: native API replay construction

Same approved #2542/#2151 original construction/raw_sql_policy boundary as cohort108.
Four already owner-backed replay consumers now use native PostgreSQL construction
at the original opening/cleanup point: distinct replay/audit/idempotency, idempotency
before audit readiness, idempotency before direct fan-out failure and singleton agent
replay projection. No exposed pool, reconstructed admission or repeated pool cleanup.

Extend the four existing finite recipes and the same named constructor normalization
and actual-source completion control; do not add duplicate recipes, alternate owners,
new SQL or permissions. All event keys, source fixtures, recipient routes, actor
identity, idempotency, audit, failure, readback and no-duplicate assertions are retained.
The actual-source control prevents constructor regression despite before/after
workload normalization. Foreign backend/test/reconstruction controls remain.

Focused race proof runs all four original PostgreSQL replay roots, with complete type
overlay and the existing event/physical/replay owner oracles. This is construction
qualification, not newly claimed backend parity. Existing canonical read owners have
their own native both-store controls. Census must decrease with zero new identities.

Value: finite construction caller fan-out after earlier canonical read-owner migration;
confinement/resource ordering, no measured flake-rate claim. No production semantics/
spec amendment/framework/compatibility/architecture split. Parent completion open.
