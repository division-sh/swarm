# Pre-Implementation Coverage Audit: Reporter Delay Window

Audit only. Base: merged master `b005cc6599500f5e2b159ecaa9524bee5beef5f2`.
No implementation, test-body, selector, workflow, budget or baseline change.
Agent G; parent #2535 remains open. Independent reviewer-g coding gate requested.

## Contract And Classification

Category: high-risk proof-maintenance / qualification-coverage change. Symptom:
the 500-row reporter's fault collector continues delaying unrelated commits,
descendant settlement, readback and the mixed-four phase after its measured
issuance has ended. The observed sleep/helper is an entry point, not the boundary.

Chosen class: **ordinary 500-row fan-out progress proof charges all-commit,
whole-journey fault injection instead of a bounded serving-write issuance window**.
This includes BOTH the served public reporter and compiled-runtime conformance
reporter, every backend, final acknowledgment, outstanding attempt, drain and
mixed-readback consumer. Fixing only the served endpoint would leave this class
live. The issue framing is broad enough for this coherent child; it is not a
claim that all injected delay or all reporter performance debt is invalid.

Immediate parent: fault-injection lifetime/operation scope diverges from its
proof claim. Broader parent: #2535 fleet/local qualification cost; #2394 genuine
fan-out performance and #2353 test health remain separate, open obligations.
The one-second preservation root is a DIFFERENT proof concept: successful
cap-32 preservation under all-commit latency, with no throughput target. Its
complete 64-row/two-chunk/full-drain/readback proof stays unchanged. No new
one-second fast variant or weakened preservation assertion is proposed.

Binding user approval: 2026-10-09 Phase0 item8 and Phase1 item4, reaffirmed by
the orchestrator on 2026-10-11: use existing `DelayServingWrites` during measured
issuance, detach subsequent delay, retain full delayed coverage. This overrides
the old requirement to run the entire all-commit drain on every ordinary PR,
not its correctness assertions or actual stress proof.

There is no exact platform-spec numeric section defining these fixture delay
regimes. Binding adjacent spec is
`test_specification.internal_catalog_conformance.qualification_tiers`:
`admission` (lines23977-24016), `membership` (24018-24024), and
`timing_publication_and_cadence` (24121-24179) at this base. #2394's current
decisions and the actual original roots govern 500/504/502/2 counts, exact20
claim/chunk commits, original objectives and merge ceilings. Preserve served
SQLite165s/PostgreSQL150s delayed issuance, conformance120s delayed issuance,
normal SQLite/PG allowances, all drain/poll/transport/Go deadlines, real leases
and chunk32. No 180s allowance from parked #2607 is admitted.

The authoritative spec must be updated in the PR to distinguish serving-window
progress from all-commit full stress and to name their actual retained owners.
Issue/audit prose is not a substitute for that merge artifact.

## Proposed One-PR Boundary

1. Preserve the two existing all-commit delayed500 roots and the one-second
   preservation root as complete, explicit full stress owners. Do not make an
   old root silently change meaning according to an environment variable.
2. Add explicitly named serving-window500 variants to the existing two fixture
   implementations: `TestIssue2394ServedOriginalReporterMeasuredIssuanceBothStores`
   and `TestIssue2394ReporterFiveHundredServingWindowBothStores`. Each uses
   300ms `DelayServingWrites`, the unchanged rows/batches and native claim/chunk
   acknowledgment oracle. No copied SQL or parallel fixture interpreter.
3. Detach through existing `Collector.Stop()` ONLY AFTER the exact final native
   receipt (20 claims/20 chunks/zero separate release), not cursor visibility,
   delivery quiescence or a stopwatch. Preserve the final acknowledgment time
   and immutable issuance receipt; retain all captured attempts until settled.
4. Observe the subsequent drain/mixed phase with a separate zero-delay collector
   through existing selected-store installation. Stop is detachment, not a join:
   a precaptured attempt keeps its original delay/ack/error receipt. Reconcile
   both collector lifetimes at final cleanup; never erase or relabel active work.
5. Partition the existing reporter territory by complete root into lifecycle
   serving-window proof and full all-commit stress. Undelayed500, the eight-row
   transaction census and their public consumer coverage remain available in
   their current ordinary tier; no accidental demotion. Existing full/nightly,
   manual full and local `--full` retain all original roots, both stores and
   every mixed/payload/history/prefix/refusal/readback assertion. Full can also
   run the window variants; no claim that full then omits the stress cost.
6. No new profile, scheduling service, dispatcher, generic delay setter, SQL
   callback/getter, mutable probe option, test skip or compatibility path.
   Reuse existing planner/deferral/supplement and native transaction owners.
   Changes to membership land only in an orchestrator-approved quiet window.

### Cutover Hold (Not An Implicit Waiver)

Spec admission requires original PR proof to stay until a complete healthy
master-nightly AND strict merge-time nightly blocking are live. Current observed
latest schedule is RED38042773650 at055bbbaba; later green master pushes are
not proof of a fresh complete `Master nightly proof`. Local marker paths cannot
establish the orchestrator's actual activation state. Before reducing ordinary
PR selection, require its explicit attestation plus the exact fresh successful
master schedule/manual run, same-run full summary/tier and original stress
receipts. No settings or gate-script changes by G.

Reviewer-g must rule this rollout condition. If it is not satisfied, preserve
existing ordinary selection: no quiet removal or claim of nightly acceptance.
The one PR can prepare/qualify both regimes, but reduced-frequency activation and
performance closure remain held until the condition is met. No new user value
decision is requested; missing operational evidence is not invented.

## Full Path And Owner-Consumption Census

Public path: verified serve boot -> admitted run.start -> portfolio construction
and initial publication completion -> install selected-backend probe ->20 public
25-row event.publish calls -> actual fan-out claim/evaluation/chunk transactions
-> final native acknowledgment -> diagnosis/delivery settlement -> history and
payload/operator readback -> mixed-four publication -> exact502 accepted/2 rejected
settlement -> HTTP/CLI readback and unchanged-store snapshot -> joined teardown.

Conformance substitutes its existing constructed runtime/manager and admitted
event producer for public boot/RPC; all subsequent serving, actual native
acknowledgment, descendant and mixed semantics are retained.

| Canonical owner / gate | Exhaustive consumers and disposition |
| --- | --- |
| `transactiontest.Slot.Begin`, `Attempt.BeforeCommit/Committed/Finish` | Native SQLite/Postgres runners and retained PostgreSQL sessions ALREADY consume it; only this owner selects which actual operation is delayed. Keep native source/semantics unchanged. `TestFanOutGrantedActualCommitCountBothStores` proves two serving writes plus undelayed evaluation read and sibling-backend isolation. |
| `Collector.Stop` and selected `storetest.CollectTransactions` | Both500 fixture families MOVED to explicit issuance detachment for their new variants; existing full variants keep whole-journey collection. Stop/captured-registration and reordered cleanup controls remain authoritative. No fixture-local delay engine. |
| Exact20 claim/chunk acknowledgment receipt | Served `runIssue2394OriginalReporterHTTP` and conformance `runNumericFanOutReporterShape` ALREADY consume native WriteCommits/LastCommitAt; MOVED to use that receipt as the window boundary. SQL cursor visibility remains wake-only, never acknowledgment. |
| Startup/admission/source/generation/grant/run construction | DIFFERENT execution-authority concept; no producer, query, cadence or validation change. #2610's native pending-loop, full inventory/public/selected integrity and action-time controls remain the baseline. |
| Claim, cap32, lease, chunk commit/revision/history owners | DIFFERENT runtime concept, retained unchanged. Both existing500 paths and the64-row preservation root execute these owners; the latter observes prefix32 and exact restored cap32 with all-commit stress. |
| Delivery/settlement, public fan-out/event reads and existing fixture readback | ALREADY existing consumers; unchanged exact settlement and immutable facts in BOTH regimes. New zero-delay receipt is observation, never permission to dispatch/retry. No raw SQL duplication or new port; unavailable typed observations require owner coordination/re-gate. |
| Semantic rejection and mixed input | DIFFERENT validation concept; same502 accepted/2 rejected, exact UUID/numeric failure evidence, original500 identity invariance and HTTP/CLI presentation. Real execution required, not 'same seam'. |
| Planner policy, native root/child binding, workload_profile, venue recipes | MOVED only by exact complete-root partition. Three existing stress units are `serveapp-i-reporter`, `conformance-2394-reporter`, `serveapp-delayed-commit-preservation`; ordinary window/normal/census ownership must be disjoint. Every original root and backend remains one full owner; supplementary full units keep full workload, even at aggregate lifecycle. |
| Nightly/summary/tier and orchestrator health admission | ALREADY existing owners, no new dispatcher or settings change. Full stress receipts and strict blocking evidence are required before omission from ordinary selection. Replayed push or mere green tier metadata is not full-nightly qualification. |
| Other native-probe test consumers | DIFFERENT control concepts: commit-loss/cancellation/cleanup/JSON-copy/active accounting and both-store exact transaction counts. Preserve them; no shortening their fault window or changing probe semantics globally. |

This is the real native probe and fixture/qualification ownership, not the first
sleep/helper found. Old whole-journey delay remains authoritative ONLY for its
explicit stress claim; using it as ordinary issuance cost becomes non-authoritative.
Neither retained stress nor separate zero-delay observation is legacy runtime
compatibility or dual ownership of COMMIT truth.

## Manifestations And Exact Planned Proof

All rows below are PLANNED, not closure credit. Complete root names from above
are abbreviated S500/C500; old served/conformance delayed roots are SStress/CStress.

| Row | Manifestation | Required execution / discriminator |
| --- | --- | --- |
| R01 | Served SQLite whole-journey delay outlives500 issuance | S500/sqlite vs SStress/sqlite; exact20+20 delayed serving commits, final ack within165s, all original downstream/mixed proofs |
| R02 | Served PG same class | S500/postgres vs SStress/postgres; same native counts,150s and real retained-session receipt |
| R03 | Conformance SQLite same class | C500/sqlite vs CStress/sqlite;120s, exact native timestamps and unchanged500/504 history/events |
| R04 | Conformance PG same class | C500/postgres vs CStress/postgres;120s and same settlement/rejection/readback |
| R05 | Non-serving bookkeeping/read commits are still slowed | Native `TestFanOutGrantedActualCommitCountBothStores`, scope-selection controls; new500 receipts require zero non-serving injected delay |
| R06 | One of20 claims/chunks escapes delay | Both500 roots require exact20 delays in EACH serving family and40*300ms total injection; omit-one/inappropriate-scope control must fail |
| R07 | Cursor visibility precedes final receipt | Existing `TestCommitAcknowledgementTimesPrecedeReorderedCleanup` plus held-final-receipt counterexample: window cannot detach before twentieth Finish receipt/native ack; no cursor/LastSeen surrogate |
| R08 | Stop loses precaptured or concurrently registering attempt | `TestCollectorStopJoinsCapturedAttemptRegistration`, captured-attempt completion/ack/error control; repeat under race; old collector settles after detachment |
| R09 | Stale old cleanup detaches successor zero-delay collection | Install/stop successor counterexample through existing Slot; successor remains selected and receives real new commits |
| R10 | Future drain/mixed transactions remain delayed | New500 old/new collector receipts: precaptured work remains disclosed, all successor attempts zero delayed commits; native both-store small continuation control |
| R11 | Stress accidentally becomes serving-only/windowed | SStress/CStress receipts retain300ms across every native read/write operation until original drain/mixed completion; mutation to stop/filter early fails |
| R12 | One-second preservation silently weakened | Existing `TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores`, BOTH leaves unchanged:64 payloads, history[0,32,64], restored cap32, all-commit1s including readback, no throughput target |
| R13 | Drain or mixed settlement lost when injector stops | Both regimes: exact fan-out summaries500 then504/502/2, zero owed/unsettled/dead letters, mixed failures at ordinals1/2 |
| R14 | Accepted payload/identity/revision/history or source safety differs | Both regimes use existing exact500 event map, UUID/numeric fields, preserved original IDs, single-chunk revision history, public first/last/mixed event readback |
| R15 | Public presentation/read mutates durable evidence | Existing HTTP and compiled CLI status/fan-out/event readback and complete before/after snapshot equality; no narrative substitution |
| R16 | Failed/uncertain/rollback attempt counted as success | `TestProbeCommitOutcomesAndDelaySelection` and original native outcome controls; zero unexpected Failed/CommitFailures/CleanupFailures in positive reporters |
| R17 | Collector/subprocess leaks across store or fixture | Native sibling-backend isolation and both served isolated test children; exact joined cleanup, no shared delay/global-hook changes |
| R18 | Lower-tier partition drops normal/census/500 backend | Current-plan binding plus finite whole-root/backend union controls: keep undelayed500 and eight-row census; remove-one-store/root must refuse |
| R19 | Full/nightly/manual/localfull loses original stress | Exact full plan selects SStress/CStress/preservation once with both children, original count1/environments/timeouts; omission/overlap/premature-window mutation refuses |
| R20 | Lifecycle extra falsely supplies only window credit | Existing supplemental full workload/root/child guards; full extras execute actual stress, not ambient aggregate-profile inference |
| R21 | Lower-tier reduced proof before healthy blocking cadence | Actual master schedule/manual full + Master nightly proof/tier/summary and orchestrator marker attestation; red/cancelled/stale/replay/missing negative controls remain |
| R22 | New expensive proof escapes time/placement admission | Exact-head timing/planner tests and actual receipts; >30s root requires finite reviewer-approved placement, not Test-Time prose, aliasing old identities or automatic anchor refresh |
| R23 | Deadline/ceiling inflation, scope-dependent allowance leak | Existing `TestNumericReporterIssuanceMergeCeilingIsolation`, reporter-envelope guard plus both-scope165/150/120 controls; every old numerical bound unchanged |
| R24 | #2610 optimization/correctness regresses or gets double-counted | Preserve pending-only source and replay current manager/native controls; paired comparison uses baseb005 including2610, not its pre-repair history |
| R25 | New raw-SQL/owner/complexity debt introduced by extraction | No SQL copying/new arbitrary getters; complete local guard/census/inventory/registry, source-debt ratchet and independent complexity; coordinate existing owner before any port |
| R26 | 'Saving' hides contract change or outside load | One predeclared same-host/base-head comparison after gate, both stores, same Go/PG/slots/cache; phase wall/injected wait/actual commit/load separately, old red retained, no retry-until-green |

No row earns credit from a shared helper or owner alone. In-scope chosen-class
closure requires all R01-R26, including actual supported public paths and both
regimes. Baseline controls and D's history are not implementation proof.

## Parent Probe, Tracking And Architecture

Parent action: retain #2535 as the umbrella, this reporter child in ONE PR;
do not absorb cache keys, plan slimming, rebalancing, walker/order, context caps,
runtime capacity or the unrelated retry/readiness class already closed by #2610.
The shared `transactiontest` owner has exactly one delay selector; current served,
conformance and one-second users were all inspected, as were native scope/stop
consumers. No second sleep engine or live ordinary500 interpreter was found.

Watchlist promotion check: `maintenance-and-cleanup.yaml` /
`harness_reliability_and_local_smoke` already maps #2535/#2394/#2353, qualification
inventory, complete artifact truth, native COMMIT evidence and retained-drain
risks. It argues for BOTH500 consumers and honest preservation coverage, not a
serve-only fix or dropping slow proofs. Refine this existing node with explicit
fault lifetime/scope and cadence-safe full retention; no new issue/node or
POTENTIAL_ISSUES entry. One-second stress is checked/retained, not absorbed into
a different throughput claim. Current issue needs a short status/body amendment
marking #2599 merged and this next gated child; historical failures stay history.

Remaining parent tail: approximately four grouped implementation slices after
this child (cache/version receipt identity; thin plan/measured balancing;
walker/order/expiry; cadence/fleet acceptance), medium confidence on grouping,
low on effort/performance until measured. No parent closure or fleet-p90 claim.

Architecture smell: fixture fault options lived for the harness rather than the
claimed phase, and qualification tiers did not expose the retained stress owner.
Long-run direction: explicit native-acknowledgment phase boundaries and existing
complete-root ownership, not a new scheduler, logging framework or mutable delay
model. Track in #2535/watchlist only; current bounded repair closes both ordinary
500 consumers. Estimated1-2 engineering days plus named slow proof windows;
report's15-30 runner-minute potential is historical/conditional, not promised
current-head saving. Full retaining the original stresses does not magically
save their compute; report ordinary selected-path saving separately from strict
full cadence cost, and never claim this is a runtime throughput improvement.

## Coordination, Feasibility And Gate Request

D coordination posted on #2610 as6105474561. That mergeb1598ba3f is included;
only `flow_runtime_readiness.go` was its runtime optimization, and reporter/probe
fixture bytes are unchanged since it. Retain its full inventory/action-time
admission and5s loop; no competing manager/runtime repair. E's #2607 stays parked
and is a separate165->180 user value question, not permission to alter ceilings.

One PR is feasible through existing owners. Local endpoint-only repair would
leave the conformance consumer live; arbitrary probe mutation, new dynamic
nightly dispatcher or source/guard exception is NOT needed. Chosen-class closure
commitment: eliminate the ordinary500 fault-lifetime class entirely; intended
closure level `touched proof family canonicalized`, parent explicitly open.

Required proposed hosted selection: lifecycle plus all retained full reporter
stress units reached by this patch, or full if reviewer-g requires it. Local:
focused native/phase/cadence controls, complete affected public/compiled both-store
variants and the local guard/census sweep; request reviewer-g's exact tier before
any expensive qualification. No shared-host slot ask or large run at pre-audit.
After approval, focused -> required tier -> one final hosted qualification;
batched push/normal PR against master with explicit reviewer-selected tier/body.

Stop/re-gate if: nightly cutover evidence cannot satisfy the binding condition;
existing Stop cannot preserve captured/final acknowledgment evidence; a required
observation needs an unallocated/new authority port; a scope label fails to reach
the actual native owner; business/shared transaction semantics must change; a
missing sibling ordinary500 interpreter appears; an expensive new root needs a
versioned placement decision; or the remaining cost is real performance rather
than removable injection. No deadline/baseline/guard weakening or blind reruns.

## Baseline Actually Executed

At unchanged code baseb005 on vemew, Go1.25.0/GOMAXPROCS2/PG16 override:

| Existing control | Actual result |
| --- | --- |
| `TestCollectorStopJoinsCapturedAttemptRegistration`, `TestProbeCommitOutcomesAndDelaySelection`, `TestCommitAcknowledgementTimesPrecedeReorderedCleanup` | All three roots PASS race/count3, nine root executions; native scope/registration/ack semantics unchanged |
| `TestFanOutGrantedActualCommitCountBothStores` | PASS race/count3, six backend leaves, no skips; root elapsed18.15s/3.30s/3.26s |
| `TestCurrentProofPlansBindActiveRequiredRoots`, `TestConformance2394PartitionPreservesCompleteRoots`, `TestServedReporterTimeoutEnvelopeIsExact` | Three roots PASS, no skips/failures; binding root11.44s |

Raw JSONL/stderr are retained at
`~/.local/state/agent-g-2535-reporter-preaudit/` on vemew. These are focused
baseline/selection controls, NOT implemented window proof, a fresh reporter
workload, a local tier, nightly acceptance or achieved savings. No large run or
server2 request. #2610's matched149.352s/151.164s issuance and409.561s/408.244s
drains remain historical source-bound evidence; they are not current-base/head
measurement or waived new qualification.

Reviewer-g: please record the bounded coding gate, exact root/tier/cadence
conditions and the operational cutover disposition. This audit does not grant
itself implementation approval.
