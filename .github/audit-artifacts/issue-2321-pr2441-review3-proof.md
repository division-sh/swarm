# Post-Implementation Proof Audit: PR #2441, Cycle 3

Agent-g. Integrated code/test head **e1b6ecd52**, base **b38b84045**.
Full-profile qualification **FAILED**: two native PostgreSQL cancellation tests
and one compiled lifecycle startup test. Not a re-review request or merge claim.
This supersedes the frozen/outside-PG-gate status in the cycle-2 checkpoint, not
its historical failures. Separate #2321, #2432, #2439 and #2442 proof tables and
commit attribution are preserved.

Binding ruling: PR issuecomment-5611197913, #2321 issuecomment-5611206250,
and #2442 body/thread. Pre-code mapping/spec: eb8113c2d. Runtime implementation:
3da58f72a (#2442), e1b6ecd52 (#2321 shutdown dependency and reviewed registry).
The original pre-audits, amendments and earlier implementation proof artifacts
remain governing for unchanged command-selection, live/mock composition,
provider state, tooling, channel authority and diagnostic contracts. No further
semantic permission, new live call or settled-delivery replay was required.

## Concepts And Systematic Consumption

The original command-selection class is not widened into a generic lifecycle
framework. This approved correction closes the specific continuation producer /
manager dependency edge within #2321, and the separately tracked two-lifetime
PostgreSQL transaction-exit class #2442. Parent runtime ownership/startup #2250
and transaction-envelope #2412 remain open. Original error-reporting and
ordinary-only transaction framings were symptom-shaped; full execution paths,
not helper names, define the corrected boundary.

| Canonical owner / all current sibling consumers | Disposition and execution path |
| --- | --- |
| Command selection, typed execution descriptors and shared runtime constructor | Unchanged from issue-2321-postimplementation.md's manifestation table. Public mock test is a fresh command-owned lifetime; live serve/dev and retained internal H retain distinct proof credit. Structural readers consume admitted source, not deployment readiness. |
| Coordinator.Start/Retire/finish and private ordinaryCoordinatorStop | Initial acquisition/scan, wake, timer, Synchronize, fatal dispatch, lease settlement and terminal reporting already consume the corrected cycle-2 owner. No new cancellation classifier in Runtime. |
| Runtime.stopWithOptions | Corrected canonical dependency-order owner. Shutdown admission and occurrence are fenced; lifecycle executor and admitted ingress drain; continuation production is retired AND joined before manager teardown. Shared grace failure is retained, then exact continuation completion is joined without detachment. |
| Runtime startup abort, ordinary shutdown, failure-triggered shutdown | All enter stopWithOptions; initial coordinator dispatch is explicitly held/canceled/joined in the deterministic test. The unchanged startup-manager observability conformance executes real persisted replay and manager readiness. |
| Coordinator AcceptCommitted/Retain/Acquire and carrier Resolve/Release | Existing owner fence remains authoritative. Accepted carriers retain their drain rights; stopping production is not abandoning accepted delivery. Prior complete coordinator controls and integrated suite cover handoff, return, repeated retirement and errors. |
| Manager lifecycle, route readiness and retained receivers | Existing manager owner remains available until dispatch joins. No lifecycle conflict suppression, shutdown flag workaround, or second manager owner. After that edge, existing manager drain and final bus route retirement/occurrence join remain. |
| Workflow timer lifecycle, generic schedules, scheduler, outbox, startup cancellation, bus reset and startup grant | Existing cleanup owners remain. No new producer can acquire the fenced runtime occurrence; startup context cancellation, stop/join, route retirement and final occurrence drain still precede grant retirement. No second lifetime framework. |
| Supported source-removal survivor transition | Existing source-set removal owner and predecessor/successor Retire paths remain separate from full Runtime shutdown; no retired live-reload operation is restored. |
| Selected fork and runlifecycle.Executor | Different authority/reservation concepts, not normal-generation continuation owners. Existing fork and bounded-grace/accepted-candidate tests are retained. |
| PostgreSQL ordinary and retained transaction exits | Separate corrected semantic owners, full consumer census and manifestation table in issue-2442-postimplementation.md. No same-concept helper left as compatibility. |
| Diagnostic durable projection, immutable provenance, both fork validators and discard cleanup | Unchanged #2432 production owners and proof in issue-2432-postimplementation.md. Every independent/reopened fixture handle consumes canonical typed payload admission. |
| SQLite transaction connection/rollback/disposal owner and six read callers | Unchanged #2439 production owner and proof in issue-2439-implementation-proof.md. No PostgreSQL change is retroactively attributed to SQLite. |
| Persistence authority registry | Sanctioned generator ran, exact delta reviewed. Coordinator.finish cancel is typed-process-local; new private SQL references/signature are private-backend. Unused RunSessionTransaction and duplicate rollback-call rows deleted. Zero unclassified findings. |
| Internal H evidence capture | Existing test-only signal, child ordinal/PID, phase/cause and bounded stack evidence retained. No production signal handler; no readiness timeout relaxation. Historical J5 cause is not reconstructed from a later pass. |

Old invalid paths: manager-first continuation teardown; panic-skipped PostgreSQL
rollback/endTx; cancellation replacing independent retained errors; unsafe
possession remaining unfenced between settlement and operation release; unused
RunSessionTransaction. The registry deletion census is specific, not accepting
every unknown finding. The canonical spec includes both the PostgreSQL exit
rules and executable_event_delivery_obligation_rows.process_local_coordinator_lifetime
dependency order.

## #2321 Manifestation And CI Proof

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Continuation dispatch finalizes readiness after manager shutdown | reproduced and fixed | TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress, race count3 PASS 1.452s: actual coordinator initial dispatch is blocked, canceled by shutdown, manager's accepted OnEvent remains uncanceled until dispatcher is released/joined, then manager cancels/drains before runtime returns. |
| Supported persisted startup/replay/observability path conflicts on start during teardown, LSF-039 | reproduced and fixed | Unchanged TestStartupManagerReplayAftermathSurface_RoundTripsThroughObservabilityReader count3 PASS 2.252s. Lead baseline failed10/10; no manager conflict suppression. |
| Coordinator startup/retirement/independent failure and accepted carrier drainage | execution-proven through the same corrected path | Prior complete coordinator race count3 14.706s in cycle-2 audit; unchanged production coordinator in this pass. Integrated qualification below includes whole package and runtime drain tests. |
| Health/entity evidence abort or missing J5 child ownership evidence | execution-proven through the same corrected path | Previously executed TestOwnedLifecycleStartupEvidence count3 and TestCompiledProcessLifecycleStartupEvidence, plus unchanged evidence paths in full profile. Signal capture is not a J5 causal reproduction. |
| Historical J5 LSF-032, LSF-028/029 | split / escalated as separate class | Explicitly retained in #2353 as observed/unclassified under cycle-3 ruling. Passing qualification cannot establish a fix; any new recurrence must be investigated using retained evidence. |

The exact current CI failure surfaces were run independently of the large suite:

| Historical row | Exact executed test | Result / attribution limit |
| --- | --- | --- |
| LSF-033 | TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores | API race count3 PASS; own-transaction cancellation/disposal has deterministic #2442 proof. This pass does not erase the old failed trace. |
| LSF-034 | TestRunCreationReceiptRejectsTypedColumnCorruptionAcrossSelectedStores | API race count3 PASS; historical server57014 is not relabeled from this sample. |
| LSF-035 | TestOperatorEventPublishRootEventTemplateInputNameCollisionPayloadEntityIDDoesNotSelectTarget | API race count3 PASS; no entity assertion changes or server error filter. |
| LSF-036 | TestOperatorEventReplaySubsetAndFailClosedCases | API race count3 PASS; original subset target and fail-closed assertions retained. |
| LSF-037 | TestOperatorEventPublishResolvesFlowScopedContractEventName | API race count3 PASS; original event name and cleanup contract retained. |
| LSF-038 | TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity | Count3 PASS 17.678s; both stores and empty-control startup postures unchanged. |
| LSF-039 | TestStartupManagerReplayAftermathSurface_RoundTripsThroughObservabilityReader | Count3 PASS 2.252s, actual dependency-order repair above. |
| Store-full registry | TestPersistenceAuthorityFindingRegistry | PASS 4.831s after sanctioned regeneration; no unclassified rows. |

The five API test selections ran together in one explicitly enumerated focused
command, race count3, PASS 62.998s. No claim that all seven CI signatures were
caused by a single helper. API-spec PASS 1.649s and worklifetime async/owner guards
PASS 0.991s. git diff --check PASS.

## Separate #2432 / #2439 / #2442 Proof

| Issue / manifestation | Status | Exact proof |
| --- | --- | --- |
| #2432 duplicate projection/acknowledgement, immutable attribution, fork authorization and destructive cleanup | reproduced and fixed | Existing issue-2432-postimplementation.md and cycle-2 dual-store identity/mode, fork lifetime and forced independent/reopened-handle race count3 PASS 137.599s. No #2432 code changes this pass. Integrated suite reruns selected-store consumers. |
| #2439 SQLite failed commit/busy/panic/read/write cancellation and independent failures | reproduced and fixed | Existing issue-2439-implementation-proof.md; cycle-2 TestTransaction*/TestReviewer*/TestCommitFailure* race count3 PASS 3.720s and deterministic cancellation rollback-order controls. No SQLite code changes this pass. |
| #2442 ordinary/retained transaction cancellation/panic/cleanup/possession | reproduced and fixed | Separate issue-2442-postimplementation.md enumerates every terminal branch and public retained consumer. Full PostgreSQL backend race count3 PASS 39.565s; public retained path selection count3 PASS 3.523s. |

## Qualification And Closure

One authorized original-load qualification completed exit1 at e1b6ecd52:
SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -count=1 -timeout=30m ./...
Log: /tmp/agent-g-2441-r3-full.log, SHA256
82ff13118d8dd43c4abe82358bac97c71c4e5bc2027148d4e98f6e5370600cd3.
It waited51s for the shared slot. No reduced
parallelism, skipped failure assertions, live messages, or replay of settled
deliveries. Failures: TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate,
TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError and
TestCompiledProcessLifecycleStartupEvidence. API failed184.688s; releasee2e
failed813.731s. Runtime passed116.100s, conformance178.834s and the full selected
runtimepersistence package507.803s. No second full run.

The evidence capture successfully retained the real failed child's startup
phase/cause and stack. It exposed a registered-standing-context / inner-startup-
abort cleanup cycle; exact source analysis and remaining classification limits
are in issue-2321-startup-abort-cycle.md. That separate compiled test failed
before its post-readiness observer exercise. The full J1-J5 matrix had no failed
row, which does not explain the historic LSF-032 failure.

Current closure: bounded owners repaired and focused proof green; integrated
proof failed on native query/rollback cancellation provenance and compiled
startup admission/abort. See
issue-2442-query-cancellation-boundary.md and #2442 issuecomment-5611438051;
the server-notice read/write probe fails3/3 each at this head. No independent
merge approval. Each chosen class is intended to
close entirely in the combined PR, but parent architecture remains open and
historical unclassified anomalies retain their explicit disposition. Prior
public live/restart receipts retain separate credit from public mock test and
internal retained H; this run is not a new live-provider proof.

Watchlist decision: existing lead refinement68dd4a1 is sufficient; retain the
atomic_runtime_state_mutation and shutdown_and_runtime_lifecycle mappings.
#2442 is the already-created concrete tracker, not another new issue.
No POTENTIAL_ISSUES entry. Broader phase/lifetime and transaction-envelope debt
stays in #2250/#2412; the F-owned #2319 split is unchanged. Complete parent-tail
effort remains unknown and should not be inferred from this bounded repair.
There is no demonstrated need for a new coordinator, registry, retry policy or
generic framework. The small dependency-order repair has high ROI; broader
decomposition is multi-PR work requiring its own audit, not closure-bearing here.
