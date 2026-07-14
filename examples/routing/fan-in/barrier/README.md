# Fan-in barrier

This recipe prefigures an Empire portfolio waiting for an explicit ordered set of `operating[*]` reports. The receiver pin owns arrival identity and the join owns the finite membership snapshot, completion, timeout, persistence, and replay.

```sh
swarm verify --contracts examples/routing/fan-in/barrier
swarm serve --contracts examples/routing/fan-in/barrier
swarm event publish portfolio.setup --payload-json '{"portfolio_id":"portfolio","expected_operating_ids":["op-a","op-b"],"period_id":"2026-Q1"}'
```

Expected: setup arms the join; it completes after exactly the declared operating identities arrive, preserving declared member order. If a member never arrives, the mandatory join timeout advances to `failed`; do not model the barrier with `accumulate` completion fields.

Proof boundary: strict load, verify, and readback consume this checked artifact. The runtime proof is producer-driven: it preserves the public `portfolio.setup` ingress and enters each member at `operating.report.requested`, then proves create, event-ID carry projection, explicit `operating.reported` emission, EventBus delivery, persistence, restart, and ordered barrier completion on SQLite and PostgreSQL.
