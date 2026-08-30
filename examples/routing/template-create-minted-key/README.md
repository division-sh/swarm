# Create With A Minted Key

Use this when every accepted event creates a template instance and the platform owns its identity. The receiver mints `validation_case_id` and immediately carries it into the delivered event.

```sh
swarm verify examples/routing/template-create-minted-key
swarm serve examples/routing/template-create-minted-key
swarm event publish producer/validation.triggered --payload-json '{"candidate":"candidate-1"}'
```

Expected: one validator instance is created with a UUID key, receives `validation.requested`, and explicitly emits the projected `validation_case_id` on `validation.started`. The journaled `validation.requested` payload remains unchanged. Retry/replay reuses the persisted route decision. To use the admitted event identity instead, declare `carries.validation_case_id.from: event.id`. If the admitted event id is missing, delivery fails closed rather than generating a fallback.
