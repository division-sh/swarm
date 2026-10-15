# Selected run-fork execution: upfront integration record

Track: FULL. Signed predecessor: bfb2ceb887f97d66f30d1507707cabbef3c6be49.
Accepted ledger: 4650 qualification-adjusted findings; physical baseline
4487/2646; held 163/135 receives no credit in this wave. Backup:
agent-b/backup-2542-20261015-bfb2ceb887 (signed commit, non-PR ref).

## Complete caller and output boundary

One caller file: internal/runtime/runforkexecution/execution_test.go. The real
baseline contains 184 findings / 154 raw operations: 14 construction, 15 local
raw types, one raw parameter, and 154 operations. Counts are opportunity, not
accepted removals. In the following table prefixes are abbreviated only for
legibility: E = TestExecuteSelectedContractRunFork; A =
TestActivateSelectedContractRunFork; S = TestSelectedContract.

| Declaration | Findings/raw | Required preserved outputs and temporal cuts |
| --- | ---: | --- |
| ERejectsDeferredWorkBeforeMutation | 11/9 | Six refusal cases; complete global snapshot; unchanged source status and active source timer |
| EWritesForkLocalExecutionAndLineage | 28/26 | Exact event/run/name/source/mode; four-key lineage; recovered routes; source-ID exclusion; target/source deliveries; mock follow-ups; failure diagnostics; source/fork/entity status |
| EConstructsAgentRecipientWithoutHandlerMaterializer | 8/6 | Persisted exact flow header and entity fields |
| EMaterializesAndExecutesForkLocalAgentRuntime | 26/24 | Fork versus source agents, event-copy exclusion, selected delivery/outcome/follow-up/finalization, ordered diagnostics and routes |
| EAdmitsSameSourceActiveDeliveryForkPointEmission | 14/12 | Real claimed source delivery, original session/audit/turn history, exact OR-identity copy counts and no copied in-progress source subscriber |
| SForkProviderTurnsUseCanonicalExecutionFrames | 16/14 | Three provider adapters; every ordered frame and causal identity; exact receipts/dead letters/delivery/runtime diagnostics on failure |
| EClaudeOAuthPersistsStartupAndTurnCapabilityAuthority | 8/4 | Both backends and native startup capability, credentials and authority |
| proveSelectedForkClaudeOAuth | 7/5 | Complete persisted session/turn/startup/effect evidence; remove raw pool parameter, preserve original selected owner |
| EProviderFailurePreservesEvidenceThroughCleanup | 8/6 | Exact attempt/operation/turn/runtime join, failure text, equal surfaces, closed execution and zero active attempts |
| AExecutesReplayReadyContractSwapThroughSelectedRecipients | 14/12 | Source/fork delivery exclusion, ordered replay rows, receipts and follow-ups |
| ETreatsSourceConversationHistoryAsLineage | 12/10 | Actual session and conversation owner, exact historical clocks and no copied sessions/audits/turns |
| ETreatsPostTSourceConversationHistoryAsBranchDivergence | 12/10 | Same history at post-T cut, exact divergence and no copies |
| EBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint | 18/16 | Exact source/fork status, ordered branch rows, post-T exclusions, receipts/follow-ups |
| SServedAndStandaloneContainersCompeteForOnePostgresAuthority | 2/0 | Already-native concrete adapter constructions; remain debt unless genuinely removed, no classification credit |

The two already-native constructions are not raw getter migrations. The target
is the complete remaining raw execution family, not artificial zero debt in
the file. Original case cardinality and failure-only diagnostic reads stay.

## Existing owners and finite transform

- Native factories: storetest.StartPostgresRuntimeStore, independent native peer
  and reopen recipes. Delete the twelve supplied-pool admissions; never recreate
  a selected coordinator over a pool. Keep original live work/lifecycle owners.
- Event identities, payloads and physical counts: eventrecord and its existing
  named storetest read bridges; no event-ID-only semantic reconstruction.
- Run/fork lineage, copied-row and branch evidence: runforkpersistence, through
  runtimepersistence's original selected coordinator and storetest. Reuse exact
  existing projections first; add only missing finite named projections.
- Deliveries and receipts: closed delivery/eventreceipt owners, not arbitrary
  SQL or a new delegate. Reuse existing node/count and selected-outcome readers.
- Source/fork statuses: runlifecycle; entity/flow fields: existing entity and
  flow-instance owners. Keep missing/NULL/text distinctions and exact keys.
- Session/audit setup: existing session/conversation fixture owners; no fabricated
  grants or lease authority. Timer setup/read: original timer owner, retaining
  the source timer's exact identity, initial cause, task ID and clocks.
- Startup/effect/turn joined evidence: original selected persistence coordinator
  and existing effect/LLM projection owners, with the full original predicates.

Use the existing finite family145 reconciler/recipe mechanism. Candidate
preimages come from this signed predecessor; mechanical propagation follows
owner repair. No new driver, router, SQL callback, compatibility path or guard
waiver. No production-contract change is proposed. Any unavoidable new semantic
decision is separated rather than silently absorbed.

## Complete source/history intersection census before coding

The exact-file scan of every top-level JSON corpus finds only these three
inventories: recipes.json (47 declarations), family145-transitions.json (47),
and family144-transitions.json (five). No selectedreadrole preimage manifest,
standing29 manifest, rewrite-stages-2566 entry/intent, collector source manifest
or source-fork whole-file pin contains this caller file. The file is also a
historical input of family145ForkCallerSources at immutable digest
44996b89ca731dc2c067e6bac1be0dfbbe9cf061b47fea8c66d6da7d9ab919ec;
preserve that input and replay the exact finite successor rather than repin it.

Twelve raw-bearing declarations already have recipe/transition rows: all table
rows except EClaudeOAuthPersistsStartupAndTurnCapabilityAuthority and the
already-native competing-container root; proveSelectedForkClaudeOAuth does
have a row. The Claude top-level root is a new append. Existing rows advance
only through exact finite Successors; old Before/After/order remain immutable.
No changes are planned to the other 35 declarations in the 47-row family145
inventory. The five family144 declarations are synthetic-carry refusal,
competing containers, buildSelectedForkProofContainer,
seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource and
seedSelectedExecutionStateOnlySourceRun; all five stay unchanged.

Earlier consumers to revalidate together: family145_fork_callers_test.go,
family145_native_binding_construction_test.go, family145_replay_scope_history_test.go,
family145_diagnostic_history_test.go, complete current-source/recipe endpoint
checks and historical finite source checks. If a shared helper must change,
include its entire consumer set before changing it, not an ad hoc pin update.
The exact four-key readers should not require changing source seeding helpers.

Standing29: zero direct declaration intersections, preserve all 29 actual
siblings and five previously authorized inverses. Source-fork native-file
inverse: preserve the just-accepted exact addition and original whole-file pin;
do not add projections to test_selected_runtime_outcome_observation.go. Use a
disjoint named file in the same existing owner where a port is missing.

Ordinary-text intersection: the competing-container root has five exact tuples
already classified in the retired-constructor transport reference guard: recipes Before
0095c88157da39be35aba627ff3991757f724e15a0d0c95b3843670d92b05c1f,
After 8bd9808550c6259b25b91fa5ac5fa8f102ea2706ed1dc538a7c73d580258368f,
Successor 91095150b46600d260565161d127d1d47bf017e10379e79e960fc6428ab01662;
transitions Before 247707110fb19b1ee87f200e2c16fa574479f71dbb46d0f82105cc71b4c731fb,
After 91095150b46600d260565161d127d1d47bf017e10379e79e960fc6428ab01662.
Leave that root and all five tuples unchanged. No new ordinary-text tuple is
anticipated; no general classification or baseline increase is authorized.

## Cross-lane reservations and integration

Correction to the prospective note in 6186521864: complete paginated PR-file
enumeration DOES show execution_test.go in merged #2617; the earlier negative
overlap claim was incomplete. Preserve its route/receiver/pin assertions at the
single authorized later rebase; do not migrate the removed routing_rules owner.
Current branch still contains that retired routing count; retain it untouched
until the rebase, with no deletion credit claimed here.

A's #2672 changes EBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint:
source advancement uses a genuine business event and pipeline settlement,
not a diagnostic dead letter. B reserves only that root's raw construction and
readback blocks, not A's source-event/claim/settlement hunk. Preserve A's
assertions on integration. D's #2674 touches module_fork_w5_test.go, not this
caller file; no nodes.yaml or grammar fixture changes planned. E's S6 ports,
ActiveSnapshots, ReadResetInventory and seedDeliveryRecoveryClaim stay untouched.
No rebase until the orchestrator's A+D integration release.

## One integration / qualification pass

During edits: compile, affected roots only; both-store race cells for the mixed
backend roots and representative exact read/absence/cancellation/foreign-owner
controls. Keep selected execution, live delivery and joined teardown real.
At the one checkpoint: compose source/history once, authenticate every changed
endpoint against the signed predecessor, preserve unchanged receipts; run the
family/history hostile controls and affected closed-owner guards, compile/vet,
then one global census and real ratchet, complexity and U1000. Report actual
accepted delta separately from held work and the two native construction sites.
No broad local tier. New-master preflight runs after the authorized integration
rebase and before an eventual non-backup push; this retained base predates it.
