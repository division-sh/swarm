# Family 140: Entire manager delivery persistence fixture

Binding: #2542 comments 6074823946 and 6075436915. One whole-family review
checkpoint, not a new PR. This closes the manager fixture family, not #2151.

## Owner And Consumer Map

- Normal setup retains the original storage artifact and exact manager authority.
  Explicit setup calls the original run/source, pipeline publication claim,
  publication, delivery claim/renew/settlement and continuation owners.
- Selected setup uses the existing compiled agent-emits-to-node source, original
  selected-store composition, process capability, recovery, preparation,
  materialization, declaration census, execution issuance and real claim. A
  channel gate observes the completed claim; it cannot supply an authority.
  The narrow selected test recipe lives beside its composition owner rather
  than in the lower-level storetest package, avoiding test import cycles.
- Source root construction and its publication commit atomically through the
  original publication owner. There are no copied fork binding/execution rows.
- All 25 original constructors in five test files are migrated. Three indirect
  roots cover projection dispositions, trace/reply lineage and descendant lanes.
  Startup-replay, tracking, settlement-error and fixed short-lease wrappers use
  these same owners. Pure disposition/error collaborators remain unit evidence.
- Handler/heartbeat completion and manager join precede releasing the selected
  claim gate. Normal failure retirement, context/projection disposal, process
  join, process-capability release and original store close follow in order.

The handwritten schema, adapter, fake transaction protocol, lazy claim-time
run/event/delivery creation, fallback run ID, cached continuation events and
manufactured selected execution are deleted, not moved behind another facade.
The explicit-only manager default introduced in family 135 is unchanged.

## Propagation And Incidental Repairs

The existing finite codemod has 28 added recipes: 18 mechanical caller rewrites
with AST equivalence and foreign-constructor controls; 10 separately pinned
semantic/setup repairs. No second codemod inventory or generic driver is added.

Incidental repairs preserve behavior rather than invalid fixture data: a second
event attaches to its existing run; publication authority matches completion
admission; the descendant is explicit before exercise; selected source, agent,
event, fork identity and generation derive from real preparation/issuance.
Source-set tests now prove an exact durable pending obligation has no accepted
claim before the fence opens, instead of relying on lazy creation's missing row.
The storage-only artifact's selected-compilation refusal remains covered.

Ordinary production claim/renew entries still use DefaultLeaseTTL. The fixed
1.5-second test cut shares their native admission/transaction/acknowledgment
implementation. Age changes are atomic and retain token/version/lease expiry;
the hostile foreign-token cut rolls back its first write. Retry eligibility is
a closed exact failed-delivery fault, not a free SQL callback or retry policy.

## Proof Disposition

Both-backend race proofs cover claim acknowledgment loss after real commit,
receipt status/error/continuation combinations, replay suppression, exact
renewal, blocked-output heartbeat, retries, cancellation/reset, source-set
fences, selected terminal failure and normal-authority exclusion, projection
carrier decisions and descendant serialization. Unchanged passing roots from
the initial affected matrix are retained; repaired roots are rerun explicitly.
No full/core, hosted CI or aggregate parent qualification is claimed here.

Earlier red receipts are retained: invalid selected source/preconditions,
native read-scope/authority/origin setup mismatches, the SQLite serialization
deadline under simultaneous selected preparation, and a new age-proof assertion
which initially used start time rather than the native immediate renewal time.
No existing timeout, workload, backend cell or behavioral assertion is removed.

The writer census also exposed 18 already-landed bounded fault writers omitted
from its explicit ledger. Their existing owners and named proofs are recorded;
their SQL and guard boundaries are unchanged. New manager faults are separately
enumerated. This is inventory repair, not a SQL allowance or a production fix.

## Completion Evidence

Source-pinned receipts use the local prefix
`~/.cache/swarm-2542-local-20261009-family140-`.
The final downward-only ratchet removed 74 findings with zero additions:
12,462 / 9,154 became 12,388 / 9,089; all 67 unresolved occurrences remain.
Registry and native authority controls passed against the same frozen source.
Native unused, exact-head complexity and timing are recorded in the whole-family
issue checkpoint after qualification. Existing watchlist
mapping remains #2151/#2542; no new issue or architecture framework is needed.
