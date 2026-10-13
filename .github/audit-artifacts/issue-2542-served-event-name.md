# Served event-name whole dependency

Part of #2542 / #2151. Signed predecessor7c937953bb4bacfe1b0fdd275a533e739a7f8270;
immutable mechanical source predecessorcee2b46e824700b380339cba079cb5dfdd191aba
(the intervening repair did not change these sources). Standing bulk gate.
Local-only checkpoint; no push, rebase, PR, tier or server2.

## Class and owners

Chosen closure: the complete served singleton event-name counter and its
run-aware/polling helper chain, plus equivalent singleton query bridges.
Binding contract: platform-spec.yaml:platform_tables.tables.events, the
original physical predicates, and the existing supported fixture/native-reader
contracts. These are storage observations, not event execution or authority.
No platform/runtime semantics changed; no new override or compatibility seam.

Fourteen calls /six declarations /three helpers now consume the native reader:
servedEventNameCount, servedEventNameCountForRun, requireServedEventNameCount.
Every other argument, replay/absence assertion, polling/deadline, readiness,
teardown and identity cut is retained. Unsupported fixture backend labels fail
closed; no supported backend behavior changes. Three raw parameters and both
handwritten dialect counter bodies are removed.

| Canonical owner | Exhaustive touched singleton consumers | Old path |
| --- | --- | --- |
| eventrecord.CountGlobalNamedEventStorageTx | existing CountEventNameStorageForTest and storetest port; complete served global/name helper and replay/ingress callers | raw API-support SQL bridge moved unchanged behind the existing event-support boundary; served dialect SQL removed |
| unchanged eventrecord.CountRunNamedEventStorageTx | existing CountRunNamedEventStorageForTest and served run/name helper; CountRunEventNameStorageForTest and its unchanged causal/pipeline/catalog callers; ReadLifecycleEventCardinalityForTest and its unchanged lifecycle callers | duplicate causal query deleted; singleton lifecycle SQL deleted and bridge moved into existing event-support boundary |

The two prior run/name native bridge APIs retain their different validation
contracts; they are consumers of one query, not separate SQL interpreters.
Source controls compare original complete bodies with only the exact query
substitution. Original native transactions, validation and error behavior stay.
No wrapper layer, generic SQL getter or new transaction protocol.

Sibling census found the lifecycle singleton and absorbed it now. Compound
multi-field publication-refusal/replay and mutation snapshots remain separate
atomic-cut observations; standalone raw test oracles remain broader #2542
debt. This does not claim all event-name SQL or parent-class elimination.

## Source and accounting

Finite codemod plus full output: migrate-event-name and internal/eventname.
Manifest3ec05c09f5a792662c6623c10efac08ae2c897fd8308a4d9e62aa941d1f3b0b1.
Whole six-body proof rejects unknown carriers, shadowing, changed arguments,
assertions, owner, scope and poll cuts; owned-source inventory covers aliases
and initializer escapes. Newest exact inverse precedes delivery-status,
run-scalar/request-key/debug inverses; older generators preserve newer output.

Corpus1475 ->1479 recipes /779 ->783 transitions. Four new rows and two exact
existing After updates; no historical Before or rows removed. E's reserved
Manager eight and runLifecycleRootForkPolicy rows are unchanged. Transition
digest9d2ffe795edbb0d4eff4ed4957d0ccd81ab394d03c44e9d0ac78a2891b18c16d.
Exact source/collector chain composes the authorized seven-pair retirement;
no added relocation/signature allowance or excluded-source credit.
Collector88b0724a6221e16b93c92943cb9090ab9178318f906edb65cc1e02fc2dcd1b66.

Real normal downward ratchet PASS205.589s, trusted source26701e7b187579220dffe72e01debd2e89cd2675:
**6979 /4740 ->6958 findings /4722 raw sites**, **21 /18 removed, zero added**.
All67 excluded occurrences remain. No manual baseline edits or metadata-only
refresh. Parent remains open at6958 /4722; no invented finish-line credit.

## Proof

Go1.26.8/GOMAXPROCS=2/-p=1/-count=1; runtime/native use -race.
Evidence /home/youmew/.cache/swarm-2542-family145/event-name-*.

| Manifestation | Status | Actual proof |
| --- | --- | --- |
| Global exact-name counts, duplicate/class/lineage/snapshot behavior and refusal on raw, cancelled, closed or unavailable owner | execution-proven through the same corrected path | native-race.jsonl:5 roots/27 passes, both stores,58.639s |
| Served live replay, paused/resumed ingress and run-scoped mailbox supersession absence | execution-proven through the same corrected path | served-race.jsonl:3 roots/9 passes, both stores,51.317s |
| Shared causal run/name query, A2 lost acknowledgment and parked node eligibility | execution-proven through the same corrected path | sibling-race.jsonl:2 roots/6 passes, both stores,21.895s |
| Lifecycle query sibling, physical duplicates/class/name and validation/refusal | execution-proven through the same corrected path | lifecycle-owner-race.jsonl:2 roots/6 passes, both stores,20.962s |
| Whole 14-call/6-body rule, canonical query/bridge equivalence, hostile carriers and finite corpus | execution-proven through the same corrected path | corpus-final.jsonl:6 roots PASS39.111s; repaired-corpus-controls.jsonl: unchanged complete idempotence root/1479 rows PASS5.034s; entity-conflict-composition.jsonl PASS |
| Native source, old accounting/collector chain and local-only metadata | execution-proven through the same corrected path | accounting-controls.jsonl unaffected roots carry; accounting-repair-controls.jsonl: all10 affected/new/retirement/copied-source roots and448 passes PASS27.220s; refreshed-sidecar.jsonl PASS |
| Complete candidate typing and historical generator conservation | execution-proven through the same corrected path | composed-guards.jsonl: original four-package overlay PASS175.870s and seven historical generators PASS; exact snapshots/exports unchanged by the final private body-only lifecycle query move, whose current package is compiled/executed by lifecycle-owner-race |
| Persistence/source/constructor/retirement ownership | execution-proven through the same corrected path | final-structural-owner-guards.jsonl:80 roots/457 passes; indexed-deletion rerun final-tracked-deletion-guard.jsonl closes the remaining root, no exception or guard change |
| Physical raw-authority reduction | reproduced and fixed | real-ratchet-downward.jsonl, exact21 decrease and zero additions |
| Parent and existing runtime/history/landing holds | split / escalated as separate class | unchanged parent holdings, no aggregate closure or restart qualification claim |

U1000 passes. No new cognitive30 hotspot; exact signed complexity is posted
with the checkpoint. Planner/tier/workload policy is unchanged; earlier named
planner receipts carry. No duplicate tier or integrated matrix.

Development reds disclosed: initial finite-rule arity corrected from the exact
source; expected fingerprint probe; exact corpus count retained its old value
until reviewed generation; writer-stage synthetic source copy lacked the new
immutable manifest, repaired without changing assertions; source-ownership
inventory saw the intentionally deleted lifecycle file while still in the Git
index, resolved by staging its actual deletion, not ignoring read failures.
Full-root selection is recorded; a too-exact development regex ran only the
idempotence root, followed by the explicit six-root corpus proof above.

No deeper architecture or new tracking owner is introduced. Existing #2542 /
#2151 and watchlist remain sufficient. All SQLite public-fork, M01/Docker,
D3/M07, drain/static-fork/deadline, historical provisioning and integrated
holds remain. Add the immutable cee2b46e8 receipt to exact package-keyed hosted
history before landing. Preflight remains required before any eventual push
from a clean/rebased head; rebase once after #2617 and orchestration handoff.
Continue with the next whole ready observation dependency.
