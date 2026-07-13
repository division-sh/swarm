# Select Existing Template Instance

Use this when delivery must target exactly one already-existing `account` instance. The carried `account_id` is the selection key. Missing and ambiguous matches fail closed and never create an instance.

```sh
swarm verify --contracts examples/routing/template-select-existing
swarm serve --contracts examples/routing/template-select-existing
swarm event publish producer/account.setup.requested --payload-json '{"account_id":"account-1"}'
swarm event publish producer/account.work.requested --payload-json '{"account_id":"account-1"}'
```

Expected: the setup event creates `account-1` through the same authored connect path used in production. The work event then selects that exact existing instance without creating another one. On `platform.target_unreachable`, publish the setup event or restore the intended instance; on ambiguity, repair duplicate active identity before retrying.
