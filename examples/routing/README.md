# Routing Examples

These directories are the positive authoring owners for supported routing patterns. Run commands from the repository root after configuring the required first-party provider trigger pack directories in your elevated local operator `swarm.yaml`.

| Need | Example | Decision |
|---|---|---|
| Deliver an external event to a root handler | `root-ingress` | Declare one external root input and a same-flow subscriber. |
| Deliver between static child flows | `parent-connect` | Declare output/input pins and one parent `connect`. |
| Require an existing keyed child | `template-select-existing` | Use receiver `resolution.mode: select`; a miss never creates. |
| Reuse or create one keyed child | `template-select-or-create` | Use receiver `resolution.mode: select-or-create`. |
| Return to the exact requester | `template-reply` | Pair request/reply pins; correlation defaults to request event identity. |
| Create a child with a platform-minted key | `template-create-minted-key` | Use receiver `resolution.mode: create` with `mint: uuid` or `event_id`. |

Verify any example with:

```sh
swarm verify --contracts examples/routing/<example>
```

A successful command prints the verified bundle summary and exits zero. If it reports a missing provider-trigger platform inventory, configure `provider_triggers.packs.platform_dirs` in the elevated local operator config before retrying. If it reports routing invalidity, do not add producer `target` or `broadcast`; compare the bundle to the applicable directory above.
