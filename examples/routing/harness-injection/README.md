# Harness Injection

Use this validation-only recipe when a test harness will provide a flow input later, but the authored bundle must be checked before behavioral injection exists. The declaration satisfies static producer proof only. It creates no route, subscriber, standing target, provider ingress, or runtime delivery.

```sh
swarm verify --contracts examples/routing/harness-injection
swarm serve --contracts examples/routing/harness-injection
swarm event publish work.requested --payload-json '{"work_id":"work-1"}'
```

Expected: verify succeeds with an explicit non-production label:

```text
verify ok: contracts=<repo>/examples/routing/harness-injection -- 1 harness-injected input; not production-valid
```

Expected serve rejection:

```text
production validation rejects test-only input source: harness at worker.work_requested; replace it with a real producer before booting
```

If `source: harness` is removed without adding a real producer, verify reports:

```text
[BLOCKER] input_pin_wiring @ worker: Flow worker declares input pin event work.requested but no accepted producer source was found in the authored bundle. Expected a producer proof for input pin target worker.work_requested.
```

Add a parent `connect`, use `source: external` for true ingress, produce the event inside the authored topology, or restore `source: harness` only for a validation fixture. Event publication is shown only to make the boundary explicit: this recipe cannot boot, and the harness declaration itself never delivers that event.
