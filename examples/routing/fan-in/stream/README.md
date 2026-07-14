# Fan-in stream

This recipe routes reports from independently created `operating` instances to the one `portfolio/default` coordinator. The input pin owns the stream window and deduplication key; `accumulate` only collects each accepted arrival and never waits for finite membership.

```sh
swarm verify --contracts examples/routing/fan-in/stream
swarm serve --contracts examples/routing/fan-in/stream
swarm event publish operating.report.requested --payload-json '{"period_id":"2026-Q1","revenue":120}'
```

Expected: every distinct `operating_id` in a period is processed immediately and stored in that period's accumulator bucket. A duplicate identity in the same window is ignored. If finite completion is required, use the `fan-in/barrier` recipe instead of adding completion fields to `accumulate`.

Proof boundary: strict load, verify, and readback consume this checked artifact. The runtime proof is producer-driven: it enters at `operating.report.requested`, creates the operating instance, projects the event-ID-minted `operating_id` into its handler, emits `operating.reported`, and executes EventBus stream routing and windowed deduplication before downstream state writes.
