# Progressive Entity Presence

Run `swarm verify tests/conformance/entity-progressive-presence` against a fresh
current store/source. The visible smoke file is a supported `swarm test` journey;
verification alone is not runtime or reconstruction proof.

This fixture distinguishes three concepts without fabricated domain values:

- `business_brief: text` is absent during `assess`, then assigned before every
  entry to `consume`. Its read in `consume` needs no optional unwrap.
- `review_note: text?` permits an explicit clear. `has(entity.review_note)` is an
  absence decision; neither creation nor readback fills in an empty note.
- `attempt_count` has the real domain initializer `0`, applied at creation only.

Before this change, required declarations fabricated values at creation and
could resurrect them on reads. Merely removing those defaults while retaining
the declaration-as-proof check would admit reads before assignment.

The naive alternative would make `business_brief` optional and require
`entity.?business_brief.orValue(...)` in the consumer. That describes a different
domain contract and can hide a missed assessment. Here the declared stage path
proves the value instead. Add a path from `assess` to `consume` that omits the
write, or move the read before the write, and verification must reject it.
