# Portfolio/period report barrier

This recipe gives each keyed portfolio its own keyed period children. Ordinary connections select the portfolio by `portfolio_id`, then its period by `period_id`. The parent forwards setup and report messages through declared events; it does not accumulate or decide completion. Each period's explicit join owns its snapshotted membership, contributor identity, completion, deadline, persistence, and replay.

Report ingress carries an authored business `operating_id`, known when membership is declared. This is separate from `operating_instance_id`, the generated operating instance key: the ingress-to-operating `create` connection retains `key_from: event.id` and projects that event ID only into `operating_instance_id`. The operating handler emits both identities; the parent forwards only the business contributor ID to the period join. Reusing a business ID in another period therefore does not reuse the generated operating instance or its join membership.

For the report commands, set `RUN_ID` to the run ID returned by setup.

```sh
swarm verify examples/routing/fan-in/barrier
swarm serve examples/routing/fan-in/barrier
swarm event publish portfolio.setup --payload-json '{"portfolio_id":"portfolio","expected_operating_ids":["op-a","op-b"],"period_id":"2026-Q1"}'
swarm event publish operating.report.triggered --run-id "$RUN_ID" --payload-json '{"operating_id":"op-b","portfolio_id":"portfolio","period_id":"2026-Q1","revenue":22}'
swarm event publish operating.report.triggered --run-id "$RUN_ID" --payload-json '{"operating_id":"op-a","portfolio_id":"portfolio","period_id":"2026-Q1","revenue":11}'
```

Expected: setup creates the selected portfolio and period, then arms the period's join. It completes after exactly the declared operating identities arrive, preserving declared member order. Another portfolio's same-named period or another period of this portfolio has independent state. If a member never arrives, the five-minute stage-entry deadline advances only that period to `failed`; do not model the barrier with `accumulate` completion fields.

Proof boundary: strict load, verify, and readback consume this checked artifact. The runtime proof must preserve public `portfolio.setup` and `operating.report.triggered` ingress, then prove operating creation, event-ID carry projection, explicit report emission, ordinary portfolio and period routing, persistence, restart, and ordered barrier completion on SQLite and PostgreSQL. `join.members.by: payload.operating_id` is explicit; no input pin supplies a window, partition, deduplication policy, or coordinator owner.

`TestA2PortfolioSupportedFiniteJoinSurfaceBothStores` exercises that boundary through authenticated JSON-RPC and normal serve boot. It stops and reconstructs the actual serve runtime against the same store while two periods are each partially filled, verifies unchanged public event/entity readback, then publishes the remaining root triggers to complete both joins with typed ordered results. This is an orderly in-process serve restart, not a crash-recovery proof.
