# Replay Latest Physical Event Identity Continuation

Binding: approved #2151/#2542 selected-store fixture authority migration and
platform-spec.yaml selected_runtime_store_projection.raw_sql_policy.
This is physical identity readback, not canonical event decoding, routing,
execution eligibility or replay authority.

The entry point latestEventIDByName owns one exact observation. Its full
consumer set is two calls in two original audit-publication/fanout-loss replay
roots. All now use the original selected native read coordinator through the
existing bounded API physical-observation owner.

The fixed query preserves exact event_name, text exclusion of one event ID,
GLOBAL cross-run scope, created_at DESC and LIMIT1. It does not normalize
names/IDs, add run/status/actor/delivery filters, or invent a tie-break rule.
The public port returns only a detached string/error, no SQL, selector,
callback, dialect or reconstructed coordinator. Missing/unavailable reads
refuse with no partial identity, retaining sql.ErrNoRows.

| Manifestation/consumer | Disposition | Exact proof |
| --- | --- | --- |
| Failed audit publication after stored replay | moved to canonical owner | Original audit-loss root under race; unchanged actual subscription, typed failure, exact replay, stored completion and retry conservation. |
| Direct fanout failure after stored replay | moved to canonical owner | Original saturation/fanout-loss root under race; unchanged channel fill/drain, exact failure, completion and retry/no-duplicate assertions. |
| Exact identity/name/exclusion/order and global scope | moved to existing owner | Both-store native proof with two matching events in distinct runs, a newer differently named event, reverse insertion order, exact exclusion, blank/non-UUID text exclusions and case/whitespace absence. |
| Raw/cancelled/closed/unavailable/absent | moved to existing owner | Both-store native refusal root and identity root pin zero evidence and sql.ErrNoRows; original coordinator counts four read commits/three failed reads/no writes/zero active work. |
| Wrong owner/excluded ID/weak assertion/unknown caller | moved to canonical owner | Three whole-function transforms: one new finite recipe and two composed snapshots; independent full-workload/helper-shape proof, six hostile changes and exhaustive exact-call inventory. |
| Other event corruption/session/setup SQL in those roots | still bypasses and explicitly tracked | Remains exact-site decreasing debt under #2542, not a compatibility path or closure claim. |

Chosen closure is this physical reader and its COMPLETE finite callers, not
parent elimination. Existing watchlist/tracking remains sufficient; no new
framework or runtime semantics. Full78 guards, census/registry, complete
codemod/type/temporal controls, contracts, replay, exact-head complexity and
native unused receipts accompany the increment before pushing. Final zero
debt, strict completion, SQLite fork-deadline and integrated proof stay open.
