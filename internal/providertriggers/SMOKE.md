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
go test ./internal/serveapp -run TestShopifyLocalProviderToolSmoke -count=1 -v
```

Optional overrides:

```sh
SHOPIFY_FLAG_TOPIC='orders/create'
SHOPIFY_FLAG_API_VERSION='2026-04'
SHOPIFY_FLAG_PATH='/path/to/shopify/app'
```

The smoke intentionally uses `SHOPIFY_FLAG_CLIENT_SECRET` as an environment variable instead of a CLI argument so the signing secret is not placed in the process argv. Missing prerequisites skip the test. Present prerequisites plus mismatched provider delivery fail the test.

## Typeform Manual-Live HTTPS Smoke

This profile starts a local Swarm API listener with a SQLite runtime store in the test process, waits for a real Typeform webhook delivery at `/webhooks/customer-a/typeform`, and verifies the existing runtime gateway, Typeform provider-trigger pack, inbound marker, event-store, and delivery outcomes. It does not call the Typeform API, create or update webhooks, or claim Postgres live-smoke coverage; deterministic Typeform gateway coverage still proves both SQLite and Postgres stores.

Prerequisites:

- A Typeform form with a webhook configured manually in the Typeform UI.
- A public HTTPS tunnel that forwards to the local listener address.
- The Typeform webhook destination URL must be `https://<public-host>/webhooks/customer-a/typeform`.
- The Typeform webhook secret must match `TYPEFORM_WEBHOOK_SECRET`.
- Run with `go test -v` so the waiting instructions are visible while the smoke is running.

Run:

```sh
SWARM_TYPEFORM_LIVE_SMOKE=1 \
SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL='https://<public-host>/webhooks/customer-a/typeform' \
TYPEFORM_WEBHOOK_SECRET='<typeform-webhook-secret>' \
go test ./internal/serveapp -run TestTypeformManualLiveHTTPSWebhookSmoke -count=1 -v
```

By default the smoke listens on `127.0.0.1:17470`, so configure the HTTPS tunnel to forward to `http://127.0.0.1:17470`. Override the listener only when your tunnel uses a different local address:

```sh
SWARM_TYPEFORM_LIVE_SMOKE_LISTEN_ADDR='127.0.0.1:18470'
SWARM_TYPEFORM_LIVE_SMOKE_TIMEOUT='5m'
```

After the test logs that it is waiting, trigger Typeform delivery with either the Webhooks UI `Send test request` button or a real form submission. Missing prerequisites skip the test. Present prerequisites plus a Typeform delivery that no longer matches the Typeform pack contract fail the test. The smoke also sends a controlled bad-signature request through the same served route and verifies that it fails before marker/event persistence.
