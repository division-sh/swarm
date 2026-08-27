# Harness Injection

Use this validation-only recipe when a test harness will provide a flow input and observe a flow output, but the authored bundle must be checked before behavioral execution exists. The declarations satisfy static input-producer and output-consumer proof only. They create no route, subscriber, standing target, provider ingress, recipient manifest, or runtime delivery.

```sh
swarm verify --contracts examples/routing/harness-injection
swarm serve --contracts examples/routing/harness-injection
swarm event publish work.requested --payload-json '{"work_id":"work-1"}'
```

Expected: verify succeeds with an explicit non-production label:

```text
verify ok: contracts=<repo>/examples/routing/harness-injection -- 1 harness-injected input at [worker.work.requested], 1 harness-observed output at [worker.work.completed]; not production-valid
```

Expected serve rejection:

```text
production validation rejects test-only input source: harness at worker.work.requested; replace it with a real producer before booting
```

If `source: harness` is removed without adding a real producer, verify reports:

```text
[BLOCKER] input_pin_wiring @ worker: Flow worker declares input pin event work.requested but no accepted producer source was found in the authored bundle. Expected a producer proof for input pin target worker.work.requested.
```

Add a parent `connect`, use `source: external` for true ingress, produce/consume the event inside the authored topology, or restore `source: harness` / `sink: harness` only for a validation fixture. Event publication is shown only to make the boundary explicit: this recipe cannot boot, and neither harness declaration delivers an event.
