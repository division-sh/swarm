# Map Scatter And Gather

Use one meaningful map for both lexical key fan-out and finite arrival membership.
There is no separately maintained key list or dummy membership map.

```sh
swarm verify examples/routing/map-scatter-gather
swarm serve examples/routing/map-scatter-gather
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
The deadline is five minutes from stage entry; an incomplete batch enters
terminal `failed`. An empty map completes with `[]` without creating workers.

`TestA2MapRecipeSupportedSurfaceBothStores` asserts load/verify, normal served
boot, root-only authenticated publication, actual worker creation and settlement,
public entity/event readback, same-store restart and idempotent replay on SQLite
and PostgreSQL. It reuses the existing in-process H serve restart harness, not a
standalone process-kill proof. Partial-chunk source mutation and later-entry
composites remain separately qualified by the pipeline proofs.

## Qualification Status

At base `910c26d67`, the count-one load/verify and both-store served selectors
fail before boot. `expression_field_reference_validation` rejects both
`fan_out.items_from` and `join.members.from` with:

```text
entity path "items": type "map[text][integer]" is not declared in the resolved type catalog
```

The existing owner is `wave1ResolveNamedType` in
`internal/runtime/bootverify/wave1_entity_contracts.go`; its legacy field reader
handles lists but not maps. Bundle loading admits the declared map. The proof
preserves the full verification assertion without a reader bypass, alternate
membership list or production change. Runtime, restart and idempotency assertions
are not yet qualified; race-count-three is deferred until count-one is green.
The closed canonical owner, checked-YAML registry and construction API guards
pass at this base.

```sh
go test ./internal/runtime/testfixtures/canonicalrouting -run '^TestCanonicalRoutingExamplesLoadAndVerify/map-scatter-gather$' -count=1 -v -timeout=2m
go test ./internal/serveapp -run '^TestA2MapRecipeSupportedSurfaceBothStores$' -count=1 -v -timeout=3m
```
