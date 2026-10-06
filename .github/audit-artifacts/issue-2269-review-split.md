# #2269 Cancellation And Emit-Feedback Review Split

Review branch: `agent-e/2269-cancellation-emit-review`. No PR. No merge or
tier qualification claimed. Part of #2269, not closure of the entire issue.

```text
CI-Tier: full
Local-Tier: lifecycle
```

Local qualification remains core first, then lifecycle plus the 13 supplements
in Gate E (#2269 comment6000906792). The soaks run hosted; no local full.

## Boundary

The user-authorized split contains authored logical-turn cancellation,
phase-free provider/origin settlement and recovery, and immutable causal emit
feedback. Stage movement alone does not cancel accepted work. Deadlines begin
at the actual first provider launch; cancellation retains accepted effects and
exact delivery/directive settlement responsibility. Emit receipts distinguish
acceptance from committed handler advancement and replay their original stage.

Universal idle disposal and final-marker eligibility/readout remain in #2269's
remainder; they are not claimed closed here. The original full branch stays at
`f4beac69b`; the separately migrated remainder is `c19caf953`.

## A And G Reconciliation

- Rebased once onto `origin/master@f46503ee2`, including merged #2578 and B's
  observation migrations. G's callback/inbox fencing and native write outcome
  separation are retained; no older copy or compensation bypass is restored.
- A's posted D1 handoff is `b7f81ddf0a3875fc452389da8e41ef0a3b2ac62e`, composed
  at `da804ac066de3a69cc0841f876076f97b4edfc19` (#2269 comment6022956647).
  Ordinary handler/timer retirement producers, injections, dead helpers and
  the engine callback/result remain deleted. Explicit termination, active-run
  fencing, accepted-effect settlement and cleanup joins remain. E's committed
  stage/cancellation extensions are the intentional differences from that pin.
- Consumed only A's D1 resurrection guards and M29 acknowledgment control,
  plus the current constructor/header fixture migrations. Retired receiver
  configuration is not restored. A retains D1 and test-owner first landing;
  #2564's unmerged workstream is not wholesale imported.
- Test observations retain A's original selected owners and exact predicates:
  mutation attribution/NULL bytes, delivery/turn conservation, header-only
  contention and API/card cardinality. No competing generic reader is added.
- The seven-line `StartEmptyPostgresDSN` remains the explicitly pinned
  source-equivalent G prerequisite at `5e237cefa`. Master now has G's template
  DSN primitive, not this empty-store variant. The empty-store helper returns
  no pool and retains native sandbox cleanup. G remains sole first landing
  (#2555 comment6021956829); reconcile that identical dependency when it lands.

## Pre-Review Evidence

At Go source `8167eeb51` after reconciliation:

- All five affected packages compile. The first compile exposed removed
  configuration calls; those were migrated, not bypassed or restored.
- Unchanged authority registry/debt collector: PASS140.305s, ZERO added sites,
  downward-only removal36, snapshot15032 ->14996. The earlier38 removals were
  measured against e0448efca; two now overlap master's landed migrations.
- D1 transition/resurrection guards: contracts25.626s, pipeline0.125s.
- M29 committed acknowledgment/lock control: engine0.010s.
- Exact static/template timeout configuration and plan/recovery: manager0.146s.
- Both-store ordinary-stage retirement refusal: native store2.092s.
- Generated facade is unchanged; every surviving changed Go file is formatted.
- Reconciled-source cancellation startup/restart and public RPC readback:
  `TestServedCanceledTurnRecoveryBothStores` PASS10.469s. Native emit acceptance
  and handler-feedback receipts, selected canceled-origin startup reaction and
  constructed-root termination controls PASS8.845s together, both stores,
  count-one. These are focused receipts, not tier or paid-provider proof.

Earlier focused proofs at `52210d417` / `052ccaaf4` remain source-pinned,
not relabeled as post-rebase qualification: H3 race-three109.199s/132.601s;
M33 race-three155.118s; served canceled recovery race-three85.021s;
card contention/replay race-three49.703s; selected terminated-origin42 cells
race-three137.601s/141.896s/152.447s. Full receipts and historical reds are in
swarm-docs `docs/audits/2269/IMPLEMENTATION-PROGRESS.md`.

Served canceled recovery uses the compiled internal mock-lifecycle process
with public RPC readback, not paid-provider or public-launcher qualification.
No token, provider credential fixture, deadline inflation, new raw-SQL test
site, compatibility reader or qualification waiver is introduced.

## Still Required

Early reviewer-e review, composed-candidate core, final lifecycle plus the 13
named supplements, eventual hosted full and the actual PR proof audit. No
core/lifecycle/full pass or complete failure-class closure is claimed here.

## Early Review Retry Repair

Binding review: #2269 comment6024231565. Both P1s were reproduced red before
repair: the preserved eight normal-store cells failed at reviewed source
aaa32c0b0 in4.605s (stale first claim or incompatible authorization/launch
anchors). The regression is committed separately at dca363b68. Earlier
diagnostic compile/setup failures are not finding evidence.

Production repair and expanded normal/selected/directive regressions:
`b1b33c9ee`. Captured-tail oracle reconciliation: `4aee09f5a`, test-only.

- The canonical effects authorization mutation binds `current_attempt_id` to
  its actual successfully authorized attempt. `admitted_attempt_id`,
  `first_attempt_id`, first launch/deadline, bound and predecessor physical
  evidence stay immutable. No latest-attempt selection or compatibility reader.
- The guarded terminate writer, launch/timeout admission, retained completion
  intent, canceled physical settlement and both recovery scopes consume that
  binding. Historical anchors validate the logical delivery and constructed
  owner; execution and settlement still require the exact current claim.
- A reclaimed claim terminated before authorization records exact
  `origin_evidence` and no current physical attempt. Closed predecessor history
  does not suppress it. Origin-only settlement refuses a physical attempt on
  that claim and also waits for the entire logical physical set to close.
- Directive execution-owner changes remain separately fenced, not delivery
  claim renewal. Same-owner prelaunch retry is proven; a foreign owner is
  rejected without another provider attempt or fabricated delivery.
- The captured-tail oracle now checks current attempt/authority independently
  from the immutable first-launch clock. It still proves all physical joins,
  rollback, exact reaction retry and no adoption of the newer live generation.
- Authoritative `platform_tables.runtime_agent_turn_lifetimes` prose/DDL are
  reconciled in this repair. The timeout start, authored cancellation semantics,
  claim `Same` fencing, provider replay policy and routing rights are unchanged.

### Temporal Coverage

Every row below is part of the existing cancellation split, not universal idle
or graph eligibility. Exact tests run on both SQLite and PostgreSQL.

| Manifestation | Proof |
| --- | --- |
| No attempt / termination versus first authorization | `TestUnstartedClaimedTurnTerminationBothStores`, `TestUnstartedDirectiveTurnRecoveryBothStores` |
| First authorization / prelaunch termination | `TestAuthoredTurnTerminationCommitsWithExactStageBothStores` prelaunch and restart rows |
| Canonical prelaunch rejection / queued retry cancellation | `TestRetriedBusinessTurnTerminationAndTimeoutRecoveryBothStores/retry_queued` preserves version, retry count and immutable predecessor |
| Current reclaimed claim before authorization | Same root `unstarted`; selected retry root `unstarted`; real origin-only recovery/settlement and repeat, no fabricated attempt/clock |
| Current retry authorization and first actual launch | Normal/selected retry roots `authorized`, `launched`; exact current cancellation, premature-settlement refusal and immutable anchors |
| Timeout recovery / foreground and selected reaction settlement | Normal `timeout-recovery`, selected `timeout_recovery`; actual commit, unchanged deadline, exactly one reaction, repeated readback, no normal acquisition of selected work |
| Several physical tails / captured settlement | `TestCanceledOriginWaitsForEveryCapturedProviderTailBothStores`; separate current attempt and first clock; every physical tail remains required |
| Captured recovery / retained late response | `TestCanceledCapturedTurnRecoveryNeverReadmitsWorkBothStores`, `TestCompletionReportsCanceledOriginWithoutDroppingAcceptedResponseBothStores` |
| Stale predecessor / canceled claim subsequently reclaimed | New retry roots plus unchanged `TestCanceledTurnRecoveryPreservesExactClaimBoundaryBothStores` negatives |
| Directive retry versus foreign execution owner | `TestLogicalTurnDirectiveRetryKeepsExactExecutionOwnerBothStores` |
| Bound/clock immutability and corrupt history | `TestLogicalTurnClock*`, `TestLogicalTurnTimeout*`, `TestCanceledTurnRecoveryRejectsCorruptFirstOriginBothStores` |

### Repair Receipts

The b1b33c9ee production and new regression sources are unchanged in4aee09f5a.
Race/count-three at b1b33c9ee, with unchanged3m budgets:

- Normal retry root: PASS95.292s, all30 repeated backend/mode cells.
- Selected retry root: PASS130.412s, all24 repeated backend/mode cells.
- Directive retry and unchanged stale-claim negative roots: PASS43.641s,
  all18 repeated backend/mode cells.
- Combined four-root race command remains RED/incomplete at180.046s. The
  package budget expired while progressing through a PostgreSQL case that had
  run1s. Individual slices above retain the same race/count/timeout; no budget
  inflation, reduced assertions or retry-to-green.

Managed surrounding controls at b1b33c9ee remain RED43.522s: the two-tail
oracle still conflated first clock with current settlement authority, on both
stores. Repaired narrowly at4aee09f5a, preserving and strengthening exact clock
and authority checks. Its three captured-tail/recovery/retained-response roots
PASS71.550s at race/count-three and the original3m budget, both stores.

Managed authority ratchet/registry at4aee09f5a: PASS176.913s, debt14996,
ZERO added debt sites,13012 exact findings. Baseline and collector are unchanged
(collector494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5).
The35 newly classified findings are exact private effects-owner SQL/types,
not public/runtime escape sites. No raw-SQL test statement or new observation
port was added; regression observations consume the existing native owner.

The full focused surrounding native control group (clock, queue/unstarted,
authored terminate, captured tails, corrupt-history/atomic reaction,
selected startup and emit feedback) PASS40.601s through the managed runner at
4aee09f5a, count-one, unchanged3m budget, both stores.

`TestServedCanceledTurnRecoveryBothStores` PASS25.263s at4aee09f5a,
count-one,3m budget. It uses the compiled internal mock-lifecycle process and
public RPC readback, not paid-provider/public-launcher qualification. The
native retry matrices, rather than this generic served control, prove the
specific retry manifestations.

Active required-root/child binding for core/lifecycle/full proof plans:
`TestCurrentProofPlansBindActiveRequiredRoots` PASS76.829s at4aee09f5a.
This builds/checks the plans; it does not execute their tiers. Generated facade,
provider adapters, authority collector and debt baseline have no repair diff;
all changed surviving Go files are formatted and the diff is whitespace-clean.

No server2 window or tier run has started. Fresh core, lifecycle plus13,
hosted full and independent repair approval remain required. The existing
watchlist node was refined by reviewer-e at swarm-docs595a156 for both P1s;
no additional class, issue, collector exception or compatibility path is added.
