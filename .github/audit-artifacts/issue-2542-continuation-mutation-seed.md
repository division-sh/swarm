# Native Shared Mutation-Seed Cohort

Base reviewed increment 2 `13bffdb2e`; integration master `06018d2e1`.
One coherent #2542 family commit under the approved broad #2151 migration and
long-lived branch model. No PR, generic fixture permission or runtime rewrite.

## Complete Owner And Consumer Migration

The shared mutation seed's four original callers now receive one original native
selected fixture. The seed uses canonical run/source materialization followed by
acknowledged selected activation, not a raw PostgreSQL setup, upsert interpreter
or transaction-context protocol. Opaque workflow reads and the existing SQL-free
callback adapter delegate to the actual selected engine-mutation owner. All four
original names and callback/state/gate/journal/history assertions remain; SQLite
is added beside PostgreSQL. The seed has no remaining raw-bearing caller.

| Original manifestation | Execution proof / status |
| --- | --- |
| MutateE callback failure rolls back | both original backend cells, exact sentinel and unchanged stored state; reproduced and fixed |
| overlapping callback snapshots reject stale first writer | both original cells, both entry channels before release, second commit wins, typed stale conflict and exact gate/journal readback; reproduced and fixed |
| closed lifecycle transition rejects competing callback snapshot | both original cells, actual compiled transition and persisted/claimed handler delivery before contention, unchanged winner/stale/history assertions; reproduced and fixed |
| single callback writer persists | both original cells, exact processing state, gate and singleton journal; reproduced and fixed |

Native lifecycle proof reuses the original transition preparation/finalization
body once. Only its semantic admission collaborator is separated: native proof
uses the actual selected publication and delivery-claim owners; untouched legacy
consumers still call their existing admission and remain explicit parent debt.
No native consumer falls back to that raw path. The independent AST oracle
compares the shared preparation body verbatim against the reviewed predecessor,
and two unchanged legacy lifecycle/logging sibling roots execute after factoring.

The scenario run already exists, so the native event explicitly has existing-run
semantics. The former run-creating fixture contradicted stored scenario_setup
origin and was correctly refused by canonical admission; no origin override is
added. Exact event, target, compiled handler, opaque claim and context binding
are checked. Missing/foreign claims leave queued state/revision/history unchanged;
the genuine claim then commits successfully on both stores. This is real native
claim-authorized component storage proof, not full public handler execution,
delivery settlement, startup/readiness or restart proof.

Native stale writes return the canonical typed lifecycle conflict, not the fake
adapter's free text. Assertions now require workflow_engine_state_revision_conflict
and exact mutation-flow route, queued prior state and revision 1. Independent
negative controls reject untyped text, wrong class/code/scope/state/revision.
No compatibility classifier or accepted alternate error is retained.

Contender entry/release and winning-commit order remain exact. New release-once
and worker joins make assertion-failure cleanup safe before the native fixture's
probe/close cleanup. Exact write totals are 1/2/4/2 after source/run setup, with
0/1/1/1 selected workflow-mutation commits respectively and zero active work.

## Codemod, Guards And Proof

Six finite recipes cover all four callers, the shared seed and original lifecycle
wrapper; 90 recipes total. Owner extraction is bounded manual preparation, while
caller propagation is mechanical. Independent AST comparison retains every
callback, channel cut and assertion, permits only the explicit canonical error
and native admission changes, and requires both workers' joined cleanup.
Changed state/gates/history or contention ordering are rejected. The original
wrapper delegates its exact old admission rather than silently changing other
families. Full snapshots/controls and actual combined candidate-overlay type
preflight PASS (7.845s); current-source application remains inert.

Original four roots / eight backend cells under race PASS (19.374s).
Typed-conflict control and both-store missing/foreign/genuine claim proof PASS
under race (9.825s). Two unchanged preparation siblings PASS under race (10.434s).
All 78 structural owner guards PASS; partition/envelope PASS (33.966s), timing
contract PASS (1.293s). Fresh ratchet, unchanged 12,536-fact registry, shared
thirteen-child census and hostile raw-parameter/SQL-callback controls PASS
(76.409s), no skips. All earlier children and six venue/tier memberships remain.

Debt `14,802 -> 14,790`: twelve removed, ZERO added. Confirmed raw-operation debt
`10,983 -> 10,975`: eight removed. No new private SQL, registry permission,
collector change or excluded uncertainty. All 67 uncertainties stay tracked.
Early failed compile receipts and the correctly refused old text/origin probes
remain in `/home/youmew/.cache/swarm-2542-local-20261006/increment3-mutation-*`;
passing final-source receipts are in the corresponding 20261007 directory.
No failure received closure credit, retry, timeout increase or assertion waiver.

The authoritative raw-SQL fixture contract records this native component path,
exact typed conflict, existing-run claim and joined cleanup. Production semantics
are unchanged. Existing parent/watchlist tracking suffices, no new issue or
architecture framework. Closure is this shared seed and complete caller family;
bookkeeping, lookup-miss, scheduler and other lifecycle/fake-runner families remain
under open #2542/#2151. Zero debt, strict completion guards, capability deletion,
SQLite fork deadline and integrated full remain the final parent obligations.
