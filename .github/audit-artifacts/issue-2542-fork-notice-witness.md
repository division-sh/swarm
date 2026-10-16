# #2542: entire fork-receiver notice witness dependency

Signed predecessor62f09eb5e15afb9a9b9ed9838336254944dd6be7;
bulk gate6134544118. Complete16-call/four-declaration/four-file dependency,
not parent completion or channel-intent disposition (separately authorized
6142069926). No production semantics or authoritative spec contract changed.
The binding context remains #2151/#2542 and the exact predecessor's independent
physical witness, adjacent to the mailbox source-event/fork-history contracts.

## Owner, rule and consumption

Existing mailboxpersistence owns the fixed source-run notice witness:
SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id
WHERE e.run_id=$1. No table-name/query callback or event-ID-only fallback.
The native adapter validates run and selected owner, enters that exact selected
coordinator's read boundary once, then delegates to the mailbox owner.
storetest exposes only this bounded witness; the served helper accepts its
already-installed receiverProofStateReader instead of the broad runtime.

All16 calls are moved: eight in runForkReceiverOwnershipJourney, four in
TestRunForkHistoricalIdentityPublicExecutionBothStores, four in
TestServedForkLoopGenerationStateEffectBothStores. The sole helper's SQL loop
is deleted. Complete original columns/order, NULLs, byte-to-text/value JSON,
row multiplicity, sorted non-nil empties, source/sibling/runless exclusion and
context.Background survive. Query/scan/encoding/iteration failures return
no partial evidence. All source/fork/frontier/generation/replay/notice/cleanup
assertions and surrounding native construction are unchanged. Raw runtime/DB
remain explicit debt only where other observations still consume them.
Private independent SQL and selected schema corruption are named owner-test
oracles, not exported fixture escape capabilities. Restoration is registered
before injecting the unencodable column and completed before store close.

Committed generator migrate-fork-notices pins the full signed Before and exact
After, rejects foreign carriers/aliases and validates all outputs before writing.
Manifest2c569e9fcb4d63d943f640c11245a4d3d00cdcf62653ec768ec70af713abb03e.
Four complete bodies/16 calls; hostile assertion/identity/release/reader changes
refuse. Exact full-body inversion composes earlier companion/receiver/public
endpoint/debug histories; original manifests and every historical expectation
remain immutable. Companion/debug generators retain the newer exact notice
successor instead of rewinding it. No generalized relocation or spelling rule.
Recipes1427/transitions729; transition digest
dee15a2dbd672025c6b8055a57c9314e34f0d3d2931564597d5392bcaefb4b27.

## Manifestation proof

Receipts /home/youmew/.cache/swarm-2542-family145/fork-notice-*.

| Manifestation/invariant | Status | Exact proof |
| --- | --- | --- |
| Complete original physical rows/NULL/text/time/order, source/sibling/runless/empty scopes, one read and no writers | reproduced and fixed | owner-race.jsonl:two roots/both stores PASS22.094s; exact independent original SQL comparison plus selected transaction probe |
| Invalid/raw/cancelled/closed owner, unencodable row without partial evidence, restored history | execution-proven through the same corrected path | final-owner-refusals-race.jsonl:both stores PASS14.260s; actual selected fault and idempotent restoration |
| Historical identity, loop generation state effect, canonical and optional receiver acquisition | execution-proven through the same corrected path | consumers-race.jsonl:four roots/14 passes/both stores PASS106.810s; complete original source/fork/replay/refusal/notice assertions |
| Complete finite transform and prior histories, foreign carrier/shadow/ellipsis/argument/assertion counterexamples | reproduced and fixed | finite-collection.jsonl:253 unchanged passing roots/280 passes; sole RED was stale expected total1426; final-recipe-inventory.jsonl fixes only that count, one root/1428 passes PASS4.299s |
| Four-package candidate type checking and exact generator replay | execution-proven through the same corrected path | candidate-overlay.jsonl PASS102.920s; generator-replay/companion-generator-replay/debug-generator-replay retain all exact hashes |
| Every existing structural/ownership/inventory guard | execution-proven through the same corrected path | complete-structural-guards.jsonl:81 package-root executions/458 passes, four packages, no test failures/skips |
| Required new sqlite/postgres proof cells, immutable old cells and whole-root partition | reproduced and fixed | plan-binding.jsonl:ten roots/74 passes PASS24.376s; omission/extra-backend controls, unchanged original evidence digest |
| Ordinary no-growth/source/collector/role accounting | execution-proven through the same corrected path | downward-census.jsonl PASS103.752s; final-readonly-policy.jsonl22 roots/1325 passes PASS31.324s |

The inventory RED is retained, with exact repair and unchanged-root receipts
carried rather than re-running the complete collection. No root skipped,
timeout inflated, retry-until-green, assertion weakened or fake authority added.
Affected pinned U1000 passes; signed complexity evidence is recorded separately
at the checkpoint. No local tier, server2, speculative integration qualification
or preflight claim on the stale held base.

Exact census7179/4920 ->7155/4898:24 findings/22 raw operations removed,
zero additions, all67 excluded occurrences and71 conserved debug occurrences
retained. Collector84f9fb91... unchanged, no new transition pair/permission.
Baseline SHA73f1b2790ca7be95edd750f1921ea4805c24f05ed38e54e8865c4b3e1894a0d1.

Closure is this whole notice-query dependency. The broader fixture-authority
class and all M01/Docker/D3/M07/static-fork/drain/final SQLite deadlines and
integrated completion holds remain open. Existing watchlist mapping suffices;
no new architecture/framework/issue. No claimed runtime performance/flake
improvement. Next approved channel-intent wave uses the existing exact observer;
it is deliberately not folded into this mechanical observation checkpoint.
No push/PR/rebase. Rebase only after #2617 and handoff; before any eventual
authorized push run actual preflight on the clean rebased tree.
