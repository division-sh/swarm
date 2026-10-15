# Selected run-fork execution: checkpoint proof audit

Track: FULL. Predecessor: bfb2ceb887f97d66f30d1507707cabbef3c6be49.
Upfront: #2542 comment6187321703. Binding approval: comment6187479154.
This is a local stacking checkpoint, not a PR, integration qualification or
parent-closure claim. No push is included in this checkpoint.

## Class and governing context

The changed concepts are test-selected persistence authority, exact physical
fork observations, and finite source/history correspondence. The working class
is the separable raw construction/readback chain of the thirteen enumerated
execution declarations, including its connected historical session, timer and
Claude helper consumers. Its parent remains the repository-wide fixture and
raw-authority migration in #2542/#2151. The symptom was raw SQL in tests; the
repair removes duplicated construction and interpretation rather than merely
moving individual assertions. Closure level: migrated dependency canonicalized;
the retained routing-dependent root is explicitly not raw-free.

The approved upfront enumerates the complete path: original native store,
compiled source, selected fork admission/materialization, real delivery/provider
execution, persisted readback, and joined cleanup. Runtime contracts, startup
authority, execution gates and shutdown behavior are unchanged. No exact spec
section prescribes this test-only projection API. The binding issue ruling and
the adjacent platform-spec.yaml table contracts for event_receipts.processed_at
and event_delivery_attempts.claim_version govern the two diagnostic corrections.
No platform architecture or runtime semantic change requires a spec amendment.

## Canonical owners and systematic consumption

The full consumer inventory is the thirteen changed declarations in
internal/runtime/runforkexecution/execution_test.go, plus the removed incidental
selectedExecutionTestAgentFields function. The new manifest authenticates all
fourteen complete declarations against the signed predecessor. The two
already-native competing-container constructions and all five family144
declarations are unchanged. No reserved A source-event/claim/settlement hunk,
D module fixture, E S6 port, or node-source generator is changed.

| Concept / canonical owner | Exhaustive touched consumer set and consumption proof |
| --- | --- |
| Original selected construction and work lifetime: storetest native factory, runtimepersistence mutation coordinator | All twelve separable supplied-pool setups use the native factory. WritesForkLocalExecutionAndLineage retains its original raw setup only because the approved obsolete routing observation needs it. Claude helper no longer accepts a pool. Complete source pins and the real ratchet verify the boundary. |
| Event identity, bytes, causal counts and ordered diagnostics: eventrecord | Execution lineage, agent execution, provider frames, activation, and branch roots use named projections through existing test_event_support.go and storetest. New bridges introduce no eventrecord import exemption. Event identity/byte tests and the 38-port refusal table execute the readers. |
| Delivery/attempt identity, subscriber/agent predicates and ordered diagnostics: closed delivery owner | Execution lineage, native agent, frames, activation and branch roots consume fixed projections in the existing read_projections.go. Settled-delivered attempts remain distinct from broader settled counts. Closed-owner guard and native failure/absence controls pass. |
| All-subscriber receipts and flow headers: pipelinepersistence | Lineage/activation/branch receipt counts preserve the all-subscriber scope; agent-construction flow-header count uses the flow owner. No platform-only count substitution. Both-store native executions and read-port table pass. |
| Root entity storage: entityruntime; named agent storage: agentpersistence | Agent-construction/execution roots read their separate domain owners; no flow-owner shortcut for entities. Both-store empty, absent, cancelled and closed-owner controls pass. |
| Exact four-key event mapping and cancelled-branch evidence: runforkpersistence | Lineage and branch roots retain every source/fork/event/policy key and branch-fact predicate. Native lineage/branch executions and invalid-owner read-port controls pass. |
| One joined attempt/operation/turn/execution failure observation: effectpersistence | Provider-failure root uses one query in one original selected read transaction, preserving all six fields and the zero-active-attempt check. Native provider-failure execution and both-store transaction counters pass. |
| Historical source session/audit/turn evidence: llmpersistence | Active-source emission and both conversation-history roots use typed identity, exact original IDs/clocks, separate native session/audit commits and OR-identity copy counts. Historical-row controls prove no fabricated lease, duplicate/cancel refusal and exact row/clock preservation. |
| Unproven source timer fixture and exact status: genericschedule | Deferred-work rejection keeps the exact timer/run/task/route/mode/clock cut through the selected mutation coordinator. The positive both-store timer control proves one write plus one read without revision effects. No live timer authority is manufactured. |
| Source/fork status and entity fields: existing runlifecycle/entity observation owners | Existing named projections are reused by all changed execution declarations. Original final-status/entity assertions remain in the finite source correspondence and real execution proofs. |
| Historical source correspondence: existing selectedreadrole and finite family145 reconciler | All fourteen current endpoints are authenticated, stale/foreign/missing/duplicate evidence is rejected, and old complete source/history is recovered once. Twelve existing recipes gain Successors; Claude and the retired helper append. Existing transition records/order/Before/After are immutable; fourteen new transitions append. |

Every new projection is a fixed domain query. Thin storetest bridges consume the
same original selected coordinator and do not infer authority from a database,
accept generic SQL/selectors, reconstruct transactions or create a new role.
Package-level owner guards cover the new files, not a filename exception.

## Manifestation coverage

Prefixes below: E = TestExecuteSelectedContractRunFork; A =
TestActivateSelectedContractRunFork; S = TestSelectedContract. N is the complete
native race receipt; R is the final read-port receipt; H is the historical-row
receipt; T is the positive timer/boundary receipt; C is finite composition.
Each row has exactly one coverage status. Retained routing debt is listed below
separately and is not represented as fixed.

| Known manifestation | Status | Exact proof |
| --- | --- | --- |
| ERejectsDeferredWorkBeforeMutation | reproduced and fixed | N: same root, all six refusal children; T: TestSelectedForkUnprovenSourceTimerFixtureKeepsExactOwnerAndClockBothStores |
| EWritesForkLocalExecutionAndLineage, separable live-owner chain | reproduced and fixed | N: same root; R: exact event, mapping, receipt, follow-up and subscriber read ports; retained routing/setup explicitly uncredited |
| EConstructsAgentRecipientWithoutHandlerMaterializer | reproduced and fixed | N: same root; R: distinct flow/entity projections |
| EMaterializesAndExecutesForkLocalAgentRuntime | reproduced and fixed | N: same root; R: agent, source exclusion, deliveries, attempts and typed diagnostic projections |
| SForkProviderTurnsUseCanonicalExecutionFrames | reproduced and fixed | N: same root, all three provider adapters; R: all failure-only diagnostic readers execute even on empty native storage |
| EClaudeOAuthPersistsStartupAndTurnCapabilityAuthority and proveSelectedForkClaudeOAuth | reproduced and fixed | N: same top-level root, default_sqlite and postgres children; C authenticates both complete declarations and removed pool parameter |
| EProviderFailurePreservesEvidenceThroughCleanup | reproduced and fixed | N: same root; R: joined failure observation and zero-active-attempt predicates |
| AExecutesReplayReadyContractSwapThroughSelectedRecipients | reproduced and fixed | N: same root; R: exact receipt/follow-up/delivery projections |
| ETreatsSourceConversationHistoryAsLineage | reproduced and fixed | N: same root; H: exact original clocks, separate commits and OR-identity counts |
| EAdmitsSameSourceActiveDeliveryForkPointEmission | reproduced and fixed | N: same root; H: historical source evidence and no copied source work |
| ETreatsPostTSourceConversationHistoryAsBranchDivergence | reproduced and fixed | N: same root; H: unchanged post-T historical clock semantics |
| EBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint | reproduced and fixed | N: same root; R: exact cancelled branch-fact predicate; A's reserved advancement hunk untouched |
| selectedExecutionTestAgentFields, unused incidental field-format helper | execution-proven through the same corrected path | C: signed removed-declaration correspondence and hostile resurrection control; U1000 after deletion; all N agent/history cases pass |
| Obsolete routing_rules observation and required raw setup | split / escalated as separate class | Exact unchanged query/source checked by C and the real ratchet; retained for the authorized integration of merged #2617, no new port or removal credit |
| Two competing-container concrete native constructions | split / escalated as separate class | C: five family144 bodies and all five ordinary-text tuples unchanged; ratchet retains multiplicity two |

## Receipts and corrections

Evidence prefix on vemew:
/home/youmew/.cache/swarm-2542-family145/selected-execution-.
All execution receipts use go1.26.8, GOMAXPROCS=2, count=1 and race. Native
services use the existing swarm-test admission owner; there is no tier run.

- N: whole-native-race.jsonl, runtime/runforkexecution PASS 61.675s, all twelve changed top-level execution roots. Supported mixed-backend roots retain both stores. The runtimepersistence package in that same aggregate was RED, not claimed as a package pass: it exposed the diagnostic-column issues below.
- R: all-diagnostic-owner-controls.jsonl, runtimepersistence PASS 11.274s. All 38 fixed read ports execute in independent transactions, including failure-only diagnostics; raw/typed-nil/uninitialized/invalid/cancelled/closed owners are refused. PostgreSQL-only aggregate diagnostics explicitly refuse SQLite rather than returning invented evidence.
- H: whole-native-race.jsonl, TestSelectedForkHistoricalSourceRowsKeepSeparateNativeCommitsAndClocksBothStores PASS 4.96s; event identity/payload root PASS 5.77s. These unchanged per-root receipts carry despite the separately disclosed aggregate diagnostic failures.
- T: final-timer-boundary-race.jsonl, runtimepersistence PASS 25.131s: positive timer transaction/clock control plus closed delivery SQL and event-admission boundary guards. TestSemanticRunFixturesUseLifecycleOwner carries its unchanged PASS 17.75s from N.
- C: final-source-history.jsonl PASS 18.291s; final-codemod-rule.log PASS 0.236s. Three new finite correspondence/history roots, old complete durable-resource history and all 1818 recipes preserve exact forward/inverse/idempotent behavior. Missing, duplicate, stale, foreign and warm-cache actual-source controls fail closed.
- Integrated history: integrated-history.jsonl carries nine independently passing roots, including complete workload preservation, fork caller/inventory, replay scopes, diagnostic retirement, native binding/competition and historical source pins. Its old durable-resource comparison was RED until composed through the new exact inverse; C proves that repair. No repeated 153-second preservation matrix was used.
- Indirect source controls: source-fork complete-file composition and standing29/five inverses pass in indirect-source-guards.jsonl. Final ordinary-text positive/negative controls pass in staged-text-guards.log, 7.195s; channel anchor/settlement/process hostile controls carry their unchanged individual passes.
- Quality: final-vet.log passes go vet -composites=false for all fixture-codemod packages and internal/store; the pre-existing five unkeyed literals are not represented as a plain-vet pass. final-unused.log passes the pinned U1000 sweep of changed scopes. Exact-snapshot complexity is recorded with the signed checkpoint receipt; no waiver. git diff --check passes.

Two failure-only SQL expressions had invalid old columns hidden by ignored
diagnostic errors: receipt ordering used updated_at instead of processed_at,
and attempt ordering used attempt_number instead of claim_version. Only those
schema-column references changed; diagnostic predicates, payloads and scope did
not. R reproduces execution of every diagnostic on both native adapters and
proves the repaired paths. SQLite-only refusals are distinct from sql.ErrNoRows.

The inherited ordinary-text hostile test expected 18 references although the
accepted predecessor and current corpus both contain 19. Its negative count and
mutant index now match that settled 19/20 inventory (comment6186734979); every
classified tuple and negative case is unchanged. No classifier extension.

The first new hostile source probe did not reach parsed source; AST-positioned
mutation now demonstrably changes the actual declaration and is rejected. An
earlier census process was stopped before any baseline write when integration
repairs remained; it supplies no credit. The qualified census below ran once
after compilation, focused proofs and quality checks passed.

## Actual accounting, residuals and architecture disposition

Real downward ratchet: qualified-ratchet-downward.log PASS 182.663s,
176 findings / 150 raw sites removed, zero additions. Physical baseline:
4487/2646 -> 4311/2496. The caller's original 184/154 leaves 8/4: one raw
carrier, the retained QueryRowContext and Scan, StartPostgres and supplied-pool
admission, one associated construction, plus two competing-container native
constructions. These are seven identities with multiplicities totaling eight.
The bootstrap source, collector policy and 67 unresolved excluded occurrences
are unchanged. The final read-only ratchet receipt is included in the checkpoint.

Held 163/135 is unchanged and uncredited: channel 76/65 (including abrupt-input
F2 6/5), old mock shutdown 78/62, numeric fork 9/8. Qualification-adjusted
remaining: 4650/2781 -> 4474/2631. No held proof is waived by these native
execution passes. Approval of the 176/150 is requested, not assumed.

The upfront 184/154 was an opportunity census, not a forced target. The exact
8/4 residual explains the difference. Parent #2542/#2151 remains open with
4474 qualification-adjusted findings if accepted; no unnamed zero-debt closure
or invented child-count estimate is asserted. The remaining tail estimate is
that exact census, to be grouped by its existing owner/dependency inventory.

Watchlist decision: retain the existing #2542/#2151 raw-authority and native
fixture-lifetime nodes, no new issue or framework. Architecture feedback is the
already-tracked dependence on broad raw carriers and pinned historical text;
the long-run direction remains typed domain owners, original selected lifetime
and finite mechanical propagation. This wave validates that direction without
adding a compatibility seam. Estimated remaining effort cannot honestly be
derived from this single file; this wave's measured throughput is reported in
the checkpoint with editing, proof and history-composition time separated.

No rebase, hosted CI, PR or duplicate local tier is claimed. The authorized
A+D integration release still gates the single rebase. This retained base
predates preflight; a fresh exact-head preflight/local receipt is required on
the integrated candidate before any eventual push, not retroactively claimed.
