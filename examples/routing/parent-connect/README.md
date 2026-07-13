# Parent Connect

Use this for static inter-flow delivery. The producer output pin and consumer input pin describe the interface; the parent package owns the edge.

```sh
swarm verify --contracts examples/routing/parent-connect
swarm serve --contracts examples/routing/parent-connect
swarm event publish producer/work.requested --payload-json '{"work_id":"work-1"}'
```

Expected: `producer-node` emits `work.ready`, one persisted route targets `consumer`, and `consumer-node` runs. If verify reports an unconnected pin, repair the parent `connect`; do not add an emit target.
