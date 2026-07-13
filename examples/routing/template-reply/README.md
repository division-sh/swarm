# Template Reply

Use this when a provider response must return to the exact requester route. `replies_to` pairs the response input with the request output. With no `correlation_key`, the platform uses stable request event identity.

```sh
swarm verify --contracts examples/routing/template-reply
swarm serve --contracts examples/routing/template-reply
swarm event publish initiator/requester.setup.requested --payload-json '{"account_id":"account-1"}'
swarm event publish initiator/request.submitted --payload-json '{"account_id":"account-1"}'
```

The setup event creates the requester through an ordinary authored route. The request event selects that requester, whose handler emits the provider request. Expected: the provider receives one request and its reply returns only to that requester, including after restart/replay. If reply context is missing or terminal, inspect the dead letter; do not reconstruct identity from payload or broadcast the reply.
