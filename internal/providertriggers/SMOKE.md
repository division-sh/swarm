# Provider Trigger Smoke

Optional provider-trigger smoke profiles are local product-assurance checks. They are not part of standard CI and they do not replace the deterministic provider-trigger conformance suite.

## Shopify Local Provider-Tool Smoke

This profile starts a local Swarm API listener with a SQLite runtime store in the test process, points the official Shopify CLI at `/webhooks/customer-a/shopify`, and verifies the existing runtime gateway, provider-trigger pack, inbound marker, and event-store outcomes. It does not claim Postgres live-smoke coverage; deterministic Shopify gateway coverage still proves both SQLite and Postgres stores.

Prerequisites:

- Shopify CLI installed as `shopify`.
- Shopify CLI authenticated enough to run `shopify app webhook trigger`.
- A Shopify app client ID and client secret.
- `SHOPIFY_FLAG_PATH` only when you want to use an existing Shopify app directory. If omitted, the smoke creates a minimal throwaway `shopify.app.toml` that contains the supplied client ID and webhook API version.

Run:

```sh
SWARM_SHOPIFY_LOCAL_SMOKE=1 \
SHOPIFY_FLAG_CLIENT_ID='<client-id>' \
SHOPIFY_FLAG_CLIENT_SECRET='<client-secret>' \
go test ./cmd/swarm -run TestShopifyLocalProviderToolSmoke -count=1 -v
```

Optional overrides:

```sh
SHOPIFY_FLAG_TOPIC='orders/create'
SHOPIFY_FLAG_API_VERSION='2026-04'
SHOPIFY_FLAG_PATH='/path/to/shopify/app'
```

The smoke intentionally uses `SHOPIFY_FLAG_CLIENT_SECRET` as an environment variable instead of a CLI argument so the signing secret is not placed in the process argv. Missing prerequisites skip the test. Present prerequisites plus mismatched provider delivery fail the test.
