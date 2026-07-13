# Root Ingress

Use this when an external caller starts work in the root flow. The root input pin declares ingress authority, and `item-handler` is the same-flow consumer. No child route or producer broadcast is involved.

```sh
swarm verify --contracts examples/routing/root-ingress
swarm serve --contracts examples/routing/root-ingress
swarm event publish item.received --payload-json '{"item_id":"item-1"}'
```

Expected: `item.received` is persisted with the root node delivery before dispatch, `item-handler` runs, and `item.processed` is visible through event readback. If the input is rejected, verify that `schema.yaml` declares `source: external` and that `nodes.yaml` has the exact local subscriber.
