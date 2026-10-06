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
