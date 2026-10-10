# #2542 continuation: pending post-commit fault evidence

The both-store pending publication fault root consumes the existing coherent
semantic event-fixture reader for exact event presence and platform/pipeline
receipt cardinality/outcome. It retains every real fault, typed refusal, committed
event, absent premature receipt, recovered claim identity, release, sweep and
success-receipt assertion. Receipt identity is the canonical event/subscriber
primary key, so success cardinality is the stored receipt count only when its
stored outcome equals success, otherwise zero. No outcome/recovery authority is
inferred from this physical witness.

The old raw handle/getter/dialect placeholder and three query/scan cuts are gone.
Fixture construction consumes existing StartSQLiteRuntimeStore and
StartPostgresRuntimeStore owners, never a live pool or reconstructed coordinator.
The canonical reader returns no partial evidence after an error. No new SQL,
reader/parser, compatibility, permission or raw classification is introduced.

The complete finite inverse replaces only construction and the two observation
cuts. Hostile controls preserve exact event, 1/0 before recovery, successful-only
receipt 1 afterward, held claim/release identity and original sweep bounds.
Actual race proof is TestEventBusPendingPostCommitFaultRetainsDurablePublicationOnBothStores,
all existing fault/backend cells. These execute actual bus publication, the
injected live post-commit refusal, claim release/reclaim and recovery sweep.

Governing raw_sql_policy and existing watchlist/gate suffice. This closes pending
post-commit observation/construction, not other bus faults, pool/context/setup
families or #2542/#2151. No new framework, tracker or runtime-spec behavior.
Final counts/proof receipts are on the issue; no core/full/server2/hosted,
fork-deadline or parent closure is claimed.
