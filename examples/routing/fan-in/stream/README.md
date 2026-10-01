# Period-scoped report stream

This recipe routes reports from independently created `operating` instances to a `portfolio` instance keyed by `period_id`. Ordinary `select-or-create` routing owns instance selection; `accumulate.key: payload.operating_id` owns business deduplication within that instance. The stream has no finite membership or completion condition.

```sh
swarm verify examples/routing/fan-in/stream
swarm serve examples/routing/fan-in/stream
swarm event publish operating.report.triggered --payload-json '{"period_id":"2026-Q1","revenue":120}'
```

Expected: every distinct `operating_id` is processed immediately in its period's instance. An identical contribution under the same key is idempotent; changed content under that key is a conflict, not a silently discarded update. A different period selects a different instance with independent reports, accumulator state, and `last_revenue`. If finite completion is required, use the barrier recipe instead of adding completion fields to `accumulate`.

Proof boundary: strict load, verify, and readback consume this checked artifact. The runtime proof must enter through `operating.report.triggered`, create the operating instance, project the event-ID-minted `operating_id` into its handler, emit `operating.reported`, and prove ordinary period-keyed EventBus routing followed by keyed accumulation and isolated downstream state writes. There is no pin-owned partition or deduplication policy.
