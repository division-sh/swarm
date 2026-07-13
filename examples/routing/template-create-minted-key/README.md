# Create With A Minted Key

Use this when every accepted event creates a template instance and the platform owns its identity. The receiver mints `validation_case_id` and immediately carries it into the delivered event.

```sh
swarm verify --contracts examples/routing/template-create-minted-key
swarm serve --contracts examples/routing/template-create-minted-key
swarm event publish producer/validation.triggered --payload-json '{"candidate":"candidate-1"}'
```

Expected: one validator instance is created with a UUID key and receives `validation.requested`. Retry/replay reuses the persisted route decision. For `mint: event_id`, admission must provide a stable event id; a missing id fails closed rather than generating a fallback.
