# Fan-in barrier

This recipe prefigures an Empire portfolio waiting for an explicit ordered set of `operating[*]` reports. The receiver pin owns arrival identity and the join owns the finite membership snapshot, completion, timeout, persistence, and replay.

```sh
swarm verify --contracts examples/routing/fan-in/barrier
swarm serve --contracts examples/routing/fan-in/barrier
swarm event publish portfolio.setup --payload '{"portfolio_id":"portfolio","expected_operating_ids":["op-a","op-b"],"period_id":"2026-Q1"}'
```

Expected: the join completes after exactly the declared operating identities arrive, preserving declared member order. If a member never arrives, the mandatory join timeout advances to `failed`; do not model the barrier with `accumulate` completion fields.
