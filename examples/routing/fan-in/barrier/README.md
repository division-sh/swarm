# Portfolio/period report barrier

This recipe gives each keyed portfolio its own keyed period children. Ordinary connections select the portfolio by `portfolio_id`, then its period by `period_id`. The parent forwards setup and report messages through declared events; it does not accumulate or decide completion. Each period's explicit join owns its snapshotted membership, contributor identity, completion, deadline, persistence, and replay.

```sh
swarm verify examples/routing/fan-in/barrier
swarm serve examples/routing/fan-in/barrier
swarm event publish portfolio.setup --payload-json '{"portfolio_id":"portfolio","expected_operating_ids":["op-a","op-b"],"period_id":"2026-Q1"}'
```

Expected: setup creates the selected portfolio and period, then arms the period's join. It completes after exactly the declared operating identities arrive, preserving declared member order. Another portfolio's same-named period or another period of this portfolio has independent state. If a member never arrives, the five-minute stage-entry deadline advances only that period to `failed`; do not model the barrier with `accumulate` completion fields.

Proof boundary: strict load, verify, and readback consume this checked artifact. The runtime proof must preserve public `portfolio.setup` and `operating.report.triggered` ingress, then prove operating creation, event-ID carry projection, explicit report emission, ordinary portfolio and period routing, persistence, restart, and ordered barrier completion on SQLite and PostgreSQL. `join.members.by: payload.operating_id` is explicit; no input pin supplies a window, partition, deduplication policy, or coordinator owner.
