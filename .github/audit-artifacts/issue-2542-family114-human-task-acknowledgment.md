# Cohort 114: native human-task acknowledgment-loss fixture

Binding: approved #2542/#2151 original selected construction/raw_sql_policy,
the existing human-task decision/idempotency domain, and events.event_id primary
identity (platform-spec.yaml:1726). Native construction remains backend-specific;
decision/card/API behavior is unchanged. No new observation or mutation owner.

Three complete functions retire the backend pool/getter return, the authority's
unused SQL parameter and two forwarding arguments, and raw decision-event count
SQL. StartPostgresRuntimeStore and the existing SQLite constructor supply the
original owner. ReadCanonicalEventRecord uses that original read transaction and
closed complete-record codec. Exact presence under the event-ID PRIMARY KEY is
equivalent to the former COUNT(*)=1 predicate, with exact event/run identity now
asserted too. This is not a generic count mapped onto a filtered semantic list.

Preserve real public mailbox.decide admission, injected post-commit lost ack,
decided card/continuation, original result replay, two successful API mutations,
unchanged event ID and final verdict. No retry, reconstructed coordinator or raw
seed. Finite recipes compare every other statement, actual source contains no raw
DB/getter/admission, hostile identity/absence/replay/owner controls remain closed.
Focused race executes both backends, plus the existing canonical-record read
owner's refusal/codec controls. Census decreases with zero added identities.

Value: one complete public acknowledgment/replay fixture now has one original
construction/read owner rather than a raw handle propagated across helpers.
No measured flake rate or production semantics/spec change claimed. Broader human
task/mailbox fixtures and parent zero-debt/final qualification remain tracked.

Adjacent tooling control makes the prior increment's exact two unrelated runtime
factory snapshot classification adversarial: a foreign third literal remains
visible and unclassified. No broad exemption or production permission.
