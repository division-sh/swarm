# Pre-Implementation Coverage Audit: P16 Process-Transition Addendum

Superseded scope: independent gate5905422933 ruled `insufficient; widen class`.
This reset-focused request is historical evidence, not current permission.
`issue-2498-standing-operator-composition-preimplementation.md` replaces its
P16a-P16e boundary with the complete command/durable-state/child-presence matrix.

Agent-g, #2498 / #2407 R1.1, 2026-09-30.
Implementation baseline: origin/master@4fccc57ec73ce54827167dddc5bbafac41ae66d2.
Candidate audit head before this addendum: d75edc2a5, with uncommitted pause repair.
Binding prior approval: https://github.com/division-sh/swarm/issues/2498#issuecomment-5903816439.

## Disposition Requested

The approved P16 public path reveals an omitted earlier process-lifetime gate,
not another error in the generic-continue assertion. Further runtime changes are
paused pending independent disposition of this owner-map correction. The prior
approval and all original P/M/RB/U obligations remain evidence; G is not revoking
the lead's ruling or claiming any class closed.

Proposed action: absorb this bounded P16 reachability repair into #2498 through
the existing serve standing controller, RuntimeContextManager standing transition,
pipeline parent exclusion and standing desired-state writer. No new issue,
schema, occurrence registry, lease, compatibility path or framework is proposed.
The current issue needs this additive owner/earlier-gate correction before code
touches those process-transition semantics. No change to standing product policy
or generic-continue permission is proposed.

This is not a request to re-gate a named P-row merely because its test is red.
The new permission question is whether an operator transition may proceed without
an executable occurrence, and which existing durable owner proves that absence
lawful. The approved claim/continue predicate cannot answer that question. The
requested disposition is limited to that previously omitted process authority;
the approved corpus, readback and pause semantics are not resubmitted.

## Observation And Classification

The compiled retained H generation reaches readiness, public standing.suspend
succeeds, and both compiled `control continue` and authenticated `run.continue`
refuse with RUN_NOT_PAUSED and unchanged captured public run/event/delivery facts.
The next required public standing.reset returns JSON-RPC -32603 on both stores.
Two executions reached this same refusal on each store; the second also retained
the child output and response identity/hash. No reset, retained suspended N+1,
positive resume, public race or complete P16 closure credit follows.

The raw cause is independently reproduced on the audit-only baseline: inserting
reset directly after suspend in the existing selected-runtime standing handler
test fails 3/3 on SQLite and PostgreSQL with:

`standing service <exact service_id> has no process occurrence`

The context-manager control reproduces the same rejection 3/3 after a real
transition Wait/Retire, even though AcquireStandingService still selects the
loaded declaration's operator context. These are failing counterexamples, not
repair receipts. The production context-manager file is unchanged against master.

Reproducer: `issue-2498-standing-transition-probes.patch`, SHA-256
`b141e3a698b4fdb2e06530942d86bbf64a3e306ce2e01a0ba0d6dec2dba4cdce`.
Apply to the baseline in a disposable worktree. With host PostgreSQL DSN supplied:

```sh
git apply --unidiff-zero .github/audit-artifacts/issue-2498-standing-transition-probes.patch
go test ./internal/serveapp -run '^TestStandingServiceMutationsUseSelectedRuntimePipelineOnBothStores$' -count=3 -timeout=3m
go test ./internal/runtime -run '^TestAuditStandingOperatorTransitionAfterRetirement$' -count=3 -timeout=2m
```

The second probe demonstrates the existing drain entrance rejecting an absent
execution child, not a requirement that this low-level helper accept every
operator call. A correct caller-side repair may retain that negative control;
the public preserved-reset chain, not removal of the helper error, is the oracle.

Category: standing lifecycle / process-versus-durable authority composition.
Entry point: P16's supported suspend -> preserved reset, not the reset SQL helper.
The immediate parent remains generation-scoped executable lifecycle composition
(#2250); the coverage parent remains #2407. The chosen pause class already names
P16, but its census incorrectly treated the earlier drain as proven. Fixing the
selected-store continue predicate does not close that mandatory public path.

## Exact Path And Canonical Owners

1. Authenticated standing.suspend selects the loaded service runtime through
   RuntimeContextManager.AcquireStandingService. Operator context selection does
   not acquire a potentially fenced standing execution occurrence.
2. serveStandingServiceController.closeAndDrain takes the existing pipeline
   parent transition, then BeginStandingServiceTransition fences/drains the
   existing standing occurrence and withdraws executable targets.
3. The selected standing desired-state transaction commits suspended override,
   paused run/control, quiescence and journal. Its pointer/relation/source guards
   and parent-before-run lock order remain authoritative.
4. StandingServiceTransition.Retire joins and removes the exact execution child
   from the context manager. This is correct: suspended services must not retain
   executable standing leases.
5. Generic run.continue now consumes the canonical standing disposition and
   refuses the suspended generation; it cannot recreate that child.
6. Public standing.reset correctly selects the loaded operator context, but
   unconditionally repeats closeAndDrain. BeginStandingServiceTransition requires
   at least one execution occurrence and rejects before the reset transaction.
7. Consequently the existing atomic reset owner never gets to retire N/create
   suspended N+1. Resume is a separate producer using prepared publication; its
   existing publication-before-reopen ordering must remain intact.

The missed owner is RuntimeContextManager's process standing-occurrence
transition, consumed by the serve controller. It is a real lifetime owner, not
the first helper encountered. Durable desired-state authority and execution
occurrence presence are distinct; neither may substitute for the other.

## Systematic Consumer Correction

| Owner/consumer | Classification and required treatment |
|---|---|
| AcquireStandingService | Already selects one loaded declaration's operator context; all three production consumers are serve suspend/resume/reset |
| BeginStandingServiceTransition | Omitted earlier gate; sole production caller is serve closeAndDrain, reached by suspend/reset |
| StandingServiceTransition Wait/Retire/Restore | Existing exact child drain/join, rollback and schedule restoration; preserve active-child behavior, no arbitrary missing-child success |
| serve suspend/reset closeAndDrain | Proposed bounded correction: distinguish executable-child teardown from a valid non-executable operator transition using existing owners; exact admission must be gated before coding |
| Selected standing suspend/reset/reconcile/orphan writer | Already canonical durable permission/pointer/source/transaction owner; do not bypass it or create a synthetic execution child to reach it |
| serve resume/publishActiveService -> PrepareStandingServicePublication | Already creates/replaces the exact executable child after committed active result, publishes before gateway reopen; unchanged positive control |
| Startup non-executable target suppression and failClosedAfterReopen | Already consume SuppressStandingServiceTargets; classify absence after restart/failure as well as same-process suspension |
| Generic run.pause/continue and delivery/pipeline eligibility | Already in the approved repair; no auto-resume, override clearing, fake control or alternate release path |
| Global shutdown, bundle deletion/reset and generic schedules | Separate aggregate/lifetime owners; preserve existing joins and monotonic teardown, not a standing-reset workaround |

Repo-wide production callsite census found no other caller of the standing
transition entrance. Existing served tests exercise suspend -> resume -> reset,
which recreates a child before reset; selected-store preserved-reset tests omit
this process gate. Neither previously proved suspend -> reset reachability.

## Binding Context And Proof Plan

Exact governing sections: flow_model.flow_instance_authoring.standing_activation_model.lifecycle_authority;
platform_tables.tables.standing_services and standing_service_generations;
api_specification.method_catalog["standing.suspend"], ["standing.resume"],
["standing.reset"]; process_local_work_lifetime_authority standing children;
cli_specification.command_catalog.standing_reset. The existing contract explicitly
requires reset to preserve suspension and says non-executable standing states
must not own executable leases. Update authoritative spec in the final PR if the
process transition clarification is approved; this prose is not a merge artifact.

Additive proof rows, both stores where persistent state is involved:

| Row | Required discriminating proof |
|---|---|
| P16a retired suspended N | Real compiled CLI suspend -> authenticated reset -> paused N+1, terminal N; no executable child/work; generic continue refuses both exact generations |
| P16b retained suspended startup | Joined stop/restart -> reset while still suspended -> exact paused successor; only public standing.resume activates it |
| P16c active teardown | Existing active reset/suspend retains exact schedule withdrawal, drain/join, and rollback restoration; held work actually blocks mutation |
| P16d invalid absence | Unknown/unloaded/foreign service and inconsistent active child absence remain fail-closed without pointer/control/journal mutation or invented child |
| P16e repetition/compensation | Keyed replay, fresh repeated suspend, failed reset, cancellation and reopen failure retain exact durable result and joined outstanding resources |

No test may resume before reset to evade the failing path. No blanket empty
transition, fake occurrence, reason-string match or detached drain is authorized.
The smallest exact existing-owner admission rule needs independent approval.
Estimated bounded tail: one process-phase admission correction and its above
proofs, medium confidence; the original complete corpus/mutation/qualification
tail is still substantial and is not erased. Intended closure remains complete
chosen-class elimination, not a first slice.

## Progress And Proof Limits

- Current candidate passes both-store 100-row partial-feed death/restart: public
  32/100, 68 owed, 32 unsettled; restarted generation ready while paused with old
  facts intact; public continue closes feed and delivers all 100 intended rows.
  This earns that checkpoint/convergence proof, not all M or mutation coverage.
- Its 100-agent shutdown takes 3.18s SQLite / 2.28s PostgreSQL using the existing
  documented 30s product shutdown grace. The harness's prior 2s override failed.
  No issuance/settlement deadline, runtime shutdown policy or performance budget
  was raised; final selection/budget and full-suite qualification remain unrun.
- Real restored-agent election-before-pause/new-claim fence passes count3 on both
  stores. Future retry/live claim ownership controls also pass count3; accepted
  in-flight settlement survives pause. No public handed-only wake credit yet.
- Recovery-disabled startup refuses paused pending agent/node, future failed,
  busy and reclaimable debt in both policy modes on both stores; no authority
  activation, handler start or durable snapshot mutation.
- CLI standing connection uses supported explicit connection config. The leaf
  argv classifier omits standing explicit connection flags despite command
  binding; that separate command-classification gap is reported, not silently
  fixed or credited. It did not cause this reset refusal.

Tracking decision: repair #2498 and the existing delivery/invariant watchlist
nodes; keep #2407 and #2250 open, no new issue or POTENTIAL_ISSUES entry. Broader
phase-model work stays in #2250. Request narrow absorption/classification now;
all process-lifetime runtime edits remain frozen meanwhile. No PR, final proof
audit, mutation qualification or whole-suite success is claimed.

Watchlist refinement is landed at swarm-docs@192be8b in the existing
runtime-operations.delivery_and_replay_ownership and
maintenance-and-cleanup.invariant_suite_coverage nodes. Both YAML files parse and
diff checks pass. The older denied-gate entries remain historical evidence, not
current approval state. Parent action is bounded absorption requested now, with
the broader lifecycle/coverage parents left explicitly open; no new child slice
or estimated zero-tail parent closure is claimed.
