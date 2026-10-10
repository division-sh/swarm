# Cohort 97: inbound agent delivery cardinality fan-out

All seven PostgreSQL provider journeys (GitHub, Slack, Stripe, Twilio, Shopify,
Telegram and the Typeform/Intercom matrix) now supply the original selected store
to their shared agent-delivery count. It reuses delivery's existing closed
ReadServedDeliveryStatusCount with exact event, agent role and subscriber and
no statuses. No new SQL, owner or interpretation is introduced.

The full-package consumption probe and ratchet identified an eighth consumer in
the both-store acknowledged cleanup/duplicate journey. Its PostgreSQL arm also
now supplies selected, preserving every after-commit error, hidden-read duplicate,
retry/no-redelivery and hostile-result assertion. The SQLite sibling helper and
all six existing callers (five provider roots and this acknowledgment root) now
consume the same canonical count port, with their signatures/callers unchanged.
No same-concept cardinality helper remains on a raw read. The initial ratchet red
was repaired by consumer propagation, not by a baseline increase or exemption.

The existing 19-recipe inbound propagation family remains the finite rewrite and
proof owner for caller bodies. Its oracle normalizes only this exact input
substitution alongside the previously approved seed/receipt changes; every
signature, payload, durable marker, paused/pending/resume, delivery channel,
receipt and no-duplicate assertion remains. Three new recipes pin both helpers
and the acknowledgment root. Each helper preserves its original
selected/context/event/role/subscriber input and unchanged failure/count output.

Focused actual provider and acknowledgment roots run under race on their
supported stores. The existing both-store physical
predicate and raw/cancelled/closed/unavailable controls separately prove this
canonical port, including exact sibling exclusion and original coordinator use.
No broad matrix or server2 tier run is required for iteration.

This closes this cardinality helper and every known caller. Other inbound raw
setup/status/marker reads and wider authority debt remain in #2542. Existing
raw_sql_policy and broad approval govern; no runtime/spec/framework/architecture
change or compatibility path, and no parent closure is claimed.
