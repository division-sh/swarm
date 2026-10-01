# Map Scatter And Gather

Use one meaningful map for both lexical key fan-out and finite arrival membership.
There is no separately maintained key list or dummy membership map.

```sh
swarm verify examples/routing/map-scatter-gather
swarm serve examples/routing/map-scatter-gather
swarm event publish batch.ready --payload-json '{"items":{"z":[9],"A":[1,1]}}'
```

Publish the root `batch.ready` input through authenticated `event.publish`, with
the verified `bundle_hash`, a fresh `idempotency_key`, and this payload:

```json
{"items":{"z":[9],"A":[1,1]}}
```

The root starts in `waiting` with empty `items`. Its real creation handler writes
the payload map and enters `collecting`; that committed entry snapshots the map's
lexical keys. Deferred fan-out reads the retained post-write `entity.items` and
publishes each key with its integer-list value from the exact retained batch
payload, index and count. This is the same map written by the batch handler,
not a live entity reread or a separately authored membership list. Ordinary flat
connects select-or-create keyed workers. Each worker creates its business entity,
retains the requested values and reports them through the ordinary root connect.

The root collector joins from the same `state.items`, keyed by the explicit
`member_id`, with `[integer]` output. Its exact deferred continuation writes
`ordered_results` as `[[integer]]` and enters terminal `complete`. The example
above produces `[[1,1],[9]]`, in `A`, `z` order, preserving the repeated value.
Expected: one root, two distinct keyed workers, one ordered completion, and no
failed business delivery. For an empty map, expect one root, no workers and `[]`.
The deadline is five minutes from stage entry; an incomplete batch enters
terminal `failed`. An empty map completes with `[]` without creating workers.

If serving is stopped and restarted on the same store, the retained map, worker
entities, exact entry and settled results remain unchanged. Replaying the same
authenticated idempotent publication must not create another worker or completion.

`TestA2MapRecipeSupportedSurfaceBothStores` asserts load/verify, normal served
boot, root-only authenticated publication, actual worker creation and settlement,
public entity/event readback, same-store restart and idempotent replay on SQLite
and PostgreSQL. It reuses the existing in-process H serve restart harness, not a
standalone process-kill proof. Partial-chunk source mutation and later-entry
composites remain separately qualified by the pipeline proofs.

## Qualification Status

At `178d7c1d3`, the public meaningful-map and empty-map journeys pass on SQLite
and PostgreSQL at `-race -count=3` (71.966s), including normal same-store restart
and idempotent replay. This is independent proof before E's constructor merge,
not final composed qualification or a whole-process crash guarantee.

The earlier pre-boot verifier failure is retained in #1994's proof ledger.
Generic entity paths now consume the canonical catalog owner, without a local
map parser, reader bypass or alternate membership list. Malformed empty aliases
also fail closed through that catalog. The original business assertions remain.

```sh
go test ./internal/runtime/testfixtures/canonicalrouting -run '^TestCanonicalRoutingExamplesLoadAndVerify/map-scatter-gather$' -count=1 -v -timeout=2m
go test ./internal/serveapp -run '^TestA2MapRecipeSupportedSurfaceBothStores$' -count=1 -v -timeout=3m
```
