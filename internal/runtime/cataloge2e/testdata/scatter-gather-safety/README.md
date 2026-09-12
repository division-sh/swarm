# Scatter/Gather Safety Corpus

Inspired by the handover scatter-gather workload: separate template instances
park with durable timers. This smaller fixture adds an actual gather barrier
and exact assertions, without an LLM, external service, or private credentials.

## Execution

1. Open the collector with ordered membership `[alpha, beta, gamma]`.
2. Submit three distinct text-valued items through root ingress and durable fanout.
3. Verify three separate worker entities, their admitted receiver routes, fields,
   and active stage timers. The collector must remain open at zero of three.
4. Finish workers through a second durable fanout. Each worker emits its own
   stored value, reaches its terminal stage, and cancels its own timer.
5. Verify each intermediate gather frontier and the final membership-ordered
   result `[red, green, blue]` through persisted join state and public delivery
   readback.

`TestScatterGatherSafetyBothStores` executes these variants on SQLite and
PostgreSQL using the existing real-runtime catalog harness:

| Variant | Additional safety assertion |
| --- | --- |
| `ordered` | Ordinary scatter, park, finish, and gather |
| `reversed` | Arrival order cannot change declared result order |
| `duplicate_publication` | Republish each exact completion input: no extra intent, event, state mutation, receiver claim, or gather contribution |
| `partial_restart` | Stop and reconstruct the runtime after one completion; preserve the partial barrier, worker IDs, and remaining timer IDs before finishing |
| `rejected_input` | A boolean in a text field is refused; all workflow states and domain row counts remain unchanged, then valid completion still succeeds |

The completion oracle follows the exact publication ID and its causal
descendants, requires every expected delivery to settle, and checks closed
fanout issuance with matching cardinality/cursor. A bounded readback poll only
observes this durable predicate; elapsed time, stable ticks, and global quietness
are not success criteria. Unexpected dead letters fail the test.

The malformed-input assertion permits the platform's rejection diagnostic.
It does not permit a domain event, fanout intent, timer, or workflow mutation.
Template instance IDs are opaque: the test reads their admitted routes, then
cross-checks payload keys, isolated stored fields, and all subsequent receivers.

## Running

Small targeted run:

```sh
go test ./internal/runtime/cataloge2e -run '^TestScatterGatherSafetyBothStores$' -count=1
```

Repeated race proof, with test capacity admission:

```sh
go run ./cmd/swarm-test -- -race ./internal/runtime/cataloge2e -run '^TestScatterGatherSafetyBothStores$' -count=3 -timeout=10m
```

The test is registered in the canonical routing artifact census and selected by
the required `catalog-runtime` CI partition.

## Scope

Part of #2407's regression corpus; useful before #2443's lifecycle decomposition.
This is not complete #2407 or #2443 closure. It does not prove a 100-item load
budget, numeric fidelity (#2402), paid-agent execution, timer firing, forced
process death, or selected-fork deferred replay (#642). The restart case is
ordinary graceful runtime reconstruction against the same durable store, not
historical non-agent replay. Duplicate publication means the same event ID and
payload, not a new business command targeting an already-terminal worker.

Existing governing contracts remain unchanged: `platform-spec.yaml`
`handler_specification.handler_fields.fan_out`, `handler_specification.handler_fields.join`,
and the `fan_out_intents` / `fan_out_outcomes` schema contracts. No production
runtime, storage, replay, or lifecycle semantics are modified.
