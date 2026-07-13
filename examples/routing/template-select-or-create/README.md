# Select Or Create Template Instance

Use this when one carried key should reuse exactly one active instance or atomically create it when missing.

```sh
swarm verify --contracts examples/routing/template-select-or-create
swarm serve --contracts examples/routing/template-select-or-create
swarm event publish producer/account.requested --payload-json '{"account_id":"account-1"}'
```

Expected: the first event creates `account-1`; repeats and concurrent same-key requests reuse that identity and replay the committed route. On an ambiguous active match, remove the duplicate identity before retrying rather than adding conflict fallbacks.
