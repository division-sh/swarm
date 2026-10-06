# Batch 3 Local Family: Unused Inbound Setup Authority

Local stack: #2575 `a4bad4116` plus #2574's two observation/closed-owner repair
commits, reconciled at `bc5c454ab`. The optional CI split `085372ba3` is on a
separate branch and is NOT part of this migration. No push, new PR or tier
qualification is claimed.

## Ownership And Complete Family

Chosen family: unused raw-pool propagation through bounded inbound fixture
setup. The semantic writers remain the existing selected run, flow activation,
static-agent and standing-target owners consumed by
`seedPostgresInboundGatewayRuntime`. Its SQL pool parameter was not read, and
was not authority needed by any of those writers. This is not replacement of
the production ingress/router, admission, lifecycle or transaction protocol.

Removed that argument from its complete sixteen-call census across ten files:
the PostgreSQL provider matrix, acknowledged-ingress fixture, raw-provider
settlement wrapper, both GitHub application proofs, Microsoft Graph, Notion,
operator channel, Slack and Telegram consumers. There is one original helper,
not a new wrapper retaining the old signature.

Caller probing then identified the same-concept tail: the raw-provider wrapper
also accepted an unused pool, and its store opener returned that pool solely for
the wrapper. Removed both the unused argument and return, including the SQLite
getter, and retained all PostgreSQL constructor work/cleanup and all existing
public event/dead-letter/delivery/duplicate/replay assertions. No live read or
mutation statement was removed. The unused-propagation family now spans nineteen
function rewrites; fixing only the first helper would not have closed it.

Live raw observation queries in other inbound proofs are a DIFFERENT remaining
operation, not unused arguments to preserve defensively. Native PostgreSQL
construction/location primitives are already allocated to G/#2555 for first
landing (F01/F02); this family does not copy them or introduce a competing
constructor. The larger inbound/provider/activity/retained-fixture extraction
will consume those landed owners. The parent and other mapped families remain
open under #2542/#2151; this is one family inside the planned grouped batch,
not a new per-helper PR or a zero-debt claim.

## Committed Codemod And Guards

Reuse the existing `tools/fixture-codemod/pipeline-observations` executable:
its original twenty-eight recipes and dispatcher/preflight are unchanged;
nineteen finite `inbound-unused-pool` before/after snapshots are appended.
Exact snapshot matching rejects changed bindings/work or unknown source,
all edited files type-check together BEFORE writes, and repeated application
is empty. No generic search/replace or source-text matching inside literals.

The new pure recipe test reconstructs only the permitted AST parameter/call/
return edits and compares every remaining executable statement with the frozen
after snapshot, preserving all workloads and assertions. It rejects removing
a live parameter use or effectful argument. The new completed-family guard
uses the SAME exhaustive approved debt census, not the narrower legacy registry
loader: it detects raw authority at the original seed and propagated provider
wrapper/consumer, and raw result escape at the provider opener. The opener's
still-live constructor pool is not falsely claimed removed.

The incomplete seventeen-rewrite intermediate is retained as a deliberate
negative-control source. Its wrapper escape must fail the completed-family
guard, even though its code compiles and the first helper is corrected. Normal
debt and descriptive role inventories remain independently authoritative.

## Focused Evidence On Vemew

- Downward-only actual census: `15,065 -> 15,042`, twenty-three occurrences
  removed, ZERO additions. Confirmed raw-operation debt: `11,181 -> 11,162`.
  The sixty-seven excluded-source uncertainties remain explicit and unchanged.
  Collector identity remains
  `494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5`.
- Existing descriptive registry: 12,377 resolved findings verified; no role
  relabel, new ordinary-site permission, collector or guard-waiver change.
- `TestInboundAcknowledgedPublicationCleanupRespondsAndDoesNotRedeliverBothStores`,
  race: PASS on SQLite/PostgreSQL (10.30s root, 11.344s package). Original real
  cleanup, no-redelivery and acknowledgment/replay assertions preserved.
- GitHub paused-runtime persistence/released-dispatch (PostgreSQL contract) and
  Stripe configured-delivery (SQLite), race: PASS (3.71s / 4.27s).
- `TestInboundGatewayProviderRawSettlementSQLitePostgres`, race: PASS (46.27s),
  fifty-seven passing records, both backend cells, zero failures/skips. All nine
  providers retain zero-consumer/real-subscriber and duplicate readback checks.
- Existing executable-delivery owner, revision accessor and retirement
  inventories: focused PASS after the stacked repairs and initial caller edits.
  Recheck the exact final family head, along with complexity, before any push.
- Codemod snapshot/idempotence/unknown-source/atomic-preflight tests PASS.
  Applying the committed executable to a fresh `bc5c454ab` source produces all
  nineteen rewrites across ten files, type-checks the complete overlay and is
  byte-identical to the checked-in output. The previous twenty-eight recipe
  values remain identical. Repetition is empty.
- Complete-family guard: PASS31.881s. The compiled seventeen-rewrite
  intermediate deliberately FAILS30.871s, naming its actual raw call, returned
  pool and wrapper parameter. This is a real census negative control, not
  compiler rejection or a vacuous check against a narrower inventory.

Evidence lives under
`/home/youmew/.cache/swarm-2542-local-20261003/batch3-inbound-*` and
`batch3-structural-guards.log`; no server2 tests or hosted retries were started.
Earlier local pure-test failures were source-position formatting differences,
not a runtime regression; canonical AST formatting repaired the comparison
without relaxing executable-statement equality. Initial stacked registry merge
artifacts were corrected through the existing generated registry owner before
family qualification. No earlier failed/incomplete receipt is recast as a pass.

## Next Packing

Keep the ten-group native setup/inbound/provider/activity/reopen batch from
the approved packing map. This family is its dependency-free first committed
piece. Remaining native constructor consumers reuse G's allocated primitives;
the broad activity/journal and retained restart families keep their owning
lifecycle proofs. All three mandated store guard families plus independent
complexity are now mandatory pre-push checks for every later candidate. No
final integrated full or SQLite fork-deadline qualification is run mid-batch.
