# Fan-in barrier

This recipe prefigures an Empire portfolio waiting for an explicit ordered set of `operating[*]` reports. The receiver pin owns arrival identity and the join owns the finite membership snapshot, completion, timeout, persistence, and replay.

```sh
swarm verify --contracts examples/routing/fan-in/barrier
swarm serve --contracts examples/routing/fan-in/barrier
swarm event publish portfolio.setup --payload-json '{"portfolio_id":"portfolio","expected_operating_ids":["op-a","op-b"],"period_id":"2026-Q1"}'
```

Expected: setup arms the join; it completes after exactly the declared operating identities arrive, preserving declared member order. If a member never arrives, the mandatory join timeout advances to `failed`; do not model the barrier with `accumulate` completion fields.

Proof boundary: strict load, verify, and readback consume this checked artifact. Runtime conformance preserves the public `portfolio.setup` ingress and executes each canonical `operating.reported` producer output through EventBus, Pipeline, persistence, restart, and completion on SQLite and PostgreSQL. It deliberately starts report delivery at the producer-output boundary because the separate template `create` carry-to-handler gap remains tracked by #1835; full producer-driven execution is not claimed here.
