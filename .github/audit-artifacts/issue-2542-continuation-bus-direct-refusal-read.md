# #2542 continuation: bus diagnostic-direct refusal observation

The PostgreSQL and SQLite diagnostic-direct bus refusal roots now observe the
exact event through the existing canonical event-record owner instead of local
COUNT SQL or DatabaseForTest. events.event_id is the canonical primary key, so
the original cardinality is exactly zero for absence and one for presence.
Decoder/owner/read errors are returned to the unchanged fatal error assertion,
never converted to absence or success. A corrupt unexpected row therefore still
fails the proof rather than disappearing behind the stronger canonical decoder.

No new reader, SQL, private-operation classification, authority wrapper or
production behavior is introduced. Each original selected store remains the
reader; the SQLite raw getter is gone from this family. Both original fixture
setups and assertEventBusDiagnosticDirectRefusal are otherwise untouched.

Actual race proof runs both backend roots, all three real public publish owners
(publish, publish_acknowledged, publish_direct) and every diagnostic-direct event
type. Every unchanged test requires the closed-event typed refusal and zero
persisted event evidence. The complete finite roots and hostile owner/event/
presence/error controls reject weakened observation. Existing canonical event
reader controls own present/missing/refused and complete decoding behavior.

Governing boundary is selected_contracts.selected_runtime_store_projection.
raw_sql_policy; existing event ownership/watchlist and #2151 broad gate suffice.
This closes the diagnostic-direct refusal observation family, not other bus raw
counts, post-commit scopes, faults, construction or parent #2542/#2151. No new
framework, compatibility, tracker or authoritative runtime-spec change.
Final counts and receipts are recorded on the increment issue comment; no
aggregate core/full, hosted CI, server2, fork-deadline or closure is claimed.
