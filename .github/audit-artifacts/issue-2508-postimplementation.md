# Post-Implementation Proof Audit: #2508

Agent-g. Local implementation proof is complete. The first submitted head passed
the default qualification but failed hosted release-package boundary admission.
The test-only relocation below repairs that failure; repeat qualification and
exact-head CI receipts are bound in the PR comment before review is requested.
Independent merge review remains external, not self-granted approval.

## Boundary And Governing Context

The approved container has three separately owned complete maintenance classes,
plus the explicitly approved A14 exact-run contract repair. It is not one shared
semantic class. Original independent gate: issuecomment-5927450842; C11 supported
mode correction: issuecomment-5927851651; A14 bounded absorption:
issuecomment-5928433996. The four A14 audit rows, issue-body repair and watchlist
disposition were recorded before semantic edits. No new owner, issue, framework,
dependency, compatibility path or vendor code was introduced.

Binding spec sections: `cli_specification.command_catalog.agent_view`,
`agent_diagnose`, `agent_deliveries`, `conversations_list`;
`cli_specification.foundations.output_contract.identifier_resolution.input_rows`;
`api_specification.method_catalog.agent.get`, `agent.diagnose`,
`agent.delivery_lifecycle` and conditional `conversation.list`;
`flow_model.flow_instance_authoring.analyzer_obligations.expand_minimize_tooling`;
`test_specification.deterministic_scenario_runner` mailbox selector and action
contracts. `platform-spec.yaml`
and generated `openrpc.json` are updated together. The strict operational API
and selected-store identity contract is unchanged.

Semantic concepts: A preserves operator-read capability mounting, exact identity,
validation/read ordering, wire shape, typed errors and bounded cursors; A14
repairs explicit CLI carriage of required run authority. D preserves structural
authoring-view presentation and separates live readiness from structural evidence.
C preserves anchor-typed mailbox selection, complete pagination, uniqueness,
detail admission, immutable content hashes and single action dispatch.

Chosen class commitment: all three named monolithic orchestration families are
fully decomposed and independently exercised; all four unscoped operational CLI
read producers are removed. Immediate parents are #2447's boundary-owned
maintenance class and #2407's invariant coverage; A14's immediate semantic parent
is exact CLI/API operational identity authority. Broader model/lifecycle debt
remains #2250. The issue is now broad enough and not merely symptom-shaped; the
four failing commands were entry points, not independent patch boundaries.

## Owners And Exhaustive Consumption

| Canonical owner after change | Exhaustive relevant consumers and disposition |
| --- | --- |
| Existing typed operator-read store contracts plus `OperatorAgentConversationHandlers` | Nine mounted methods: agent.list/get/diagnose/delivery_diagnostics/delivery_lifecycle/usage, conversation.list/list_turns/get_turn. Each delegates to its own local factory, preserving existing capability requirements and validator order. Public HTTP/WebSocket mounting uses this same existing registration; no second registration or store interpretation was added. Capability Cartesian product, exact owner-return wire, HTTP admission, typed errors, snapshots, both-store reads and compiled consumers execute these paths. |
| `resolveOperatorAgentIdentityParam` + selected-store `ResolveOperatorAgentIdentity` | Operational get/diagnose/rich-delivery/lifecycle/usage and agent-filtered conversation.list already consume exact run/optional route authority. CLI view/diagnose/deliveries and filtered conversation list now supply it explicitly. API remains strict. Effective frame and mutating restart/replay/directive are already explicit-run consumers, unchanged. Declared agent.list (#1562) is a different inventory concept and never selects a run. Session-ID conversation view/turn are a different exact authority, proven by compiled controls. No additional production CLI caller of the four repaired methods survives. |
| Existing CLI identifier registry/resolver | Agent prefixes remain exact-first, declaration-backed presentation. View/diagnose/deliveries retry only agent_id; supplied run_id and optional flow_instance stay byte-equivalent after existing normalization. New run registry entries are full_only, never prefix/latest/ambient resolution. Conversation agent filter remains exact-only. |
| Existing authoring-view/source-admission and routing-topology owners | Full describe text consumes local flow/ingress/graph/node/edge/timer/join/fanout/gate/diagnostic sections. Full, graph and routes JSON/quiet/text still consume the same admitted projection and existing topology renderer. Precise flow.Mode branch remains in writeDescribeText for the source guard. Verify, dashboard/projection readers and topology writers are sibling concepts, unchanged and regression-tested, not folded into the renderer. |
| Existing scenario mailbox selector + public mailbox/card owners | Both mailbox.decide and mailbox.defer call findDecisionCard. All stage_gate/human_task/proposed_effect selectors use the same admitted-field evaluator, exhaustive page collector and exact predicate matcher; detail is read only after one full-set match. All registered fields and forbidden cross-anchor fields are enumerated by tests. Public mailbox/list/get/decide/defer owners remain authoritative; no test-local cursor codec, dedup, retry, polling or lifecycle owner was added. |
| Existing command-owned MockOnly session/runtime composition | Compiled public swarm test executes three fresh authored scenarios. Text is supported; JSON/quiet are rejected before source/session/RPC. Retained lifecycle harness and golden restart/kill are separate regression evidence, not substituted for fresh public test, live provider or signed ingress proof. |

Old paths invalid/non-authoritative: API/describe/scenario monolithic bodies are
replaced, not retained as alternative interpreters. CLI view/diagnose without
run, optional-run deliveries and agent-filtered conversation list without run
are invalid and fail locally. No inference or compatibility branch survives.
Retired output-pin key/carries wording is removed, not restored in output.
Unsupported swarm test JSON/quiet remains unsupported (#1712).

Systematic census: production calls to the nine read methods and four repaired
CLI producers, identifier registrations, describe rendering helpers/routes,
findDecisionCard callers, and selector field switches were checked. Existing
typed owner/store implementations are consumers beneath the unchanged contract,
not new same-concept bypasses. Canonical read ownership alone is not proof;
the following execution rows provide the closure evidence.

## Proof Inventory

Before extraction, characterization was committed as `9745a0e96` (historical
pre-rebase hash `8d2ad0f2f`), preceding `36f476c1f`. Untouched-master A14 probe
at `1c4cce209` and candidate `806d49f0e` each had 24 compiled CLI failures and 26
successful cells plus 18 successful exact-authority RPC reads. These are the
generic failing proof, not manufactured mocked answers. Post-repair compiled
journey has 68 supported CLI cells, including same-run prefixes, session readers
and unfiltered/run-only controls; all nine explicit-authority RPC reads run on
both stores. Separate compiled selected-store identity proof has 120 refusal
and 48 exact-route success cells, all modes, both stores. Local 60-case scope
admission proves zero RPC and refusal before missing-token-file access.

Proof names below are executable tests, not planned aliases. A13 is discharged
by the named real HTTP/store/cursor/snapshot proofs, not an invented aggregate
test called SelectedStoreMatrix. Store-seeded read proof is not credited as
authored source creation or live lifecycle execution.

Hosted-boundary correction: `TestReleaseE2EPackageStaysAtPublicProcessBoundary`
rejected implementation imports in the newly added release-package tests. The
168-cell store-seeded compiled read proof and three fresh compiled public
scenarios now live in CLI integration tests, where their canonical fixture
construction belongs. Both still execute the real compiled CLI, not in-process
Execute or fake successful RPC; the retained process journey and describe
baseline stay in releasee2e with standard-library imports only. The unchanged
boundary guard passes. No existing test or production behavior was changed to
silence this failure, and no new fixture/runtime/registry owner was added.
The relocated compiled proofs pass together in 102.269s.

| Manifestation | Classification | Exact execution proof |
| --- | --- | --- |
| A01 capability Cartesian product | execution-proven through the same corrected path | TestOperatorReadFactoringCapabilityMatrix: all 16 combinations; independent conversation capability test. |
| A02 list/detail wire and refs | execution-proven through the same corrected path | TestOperatorReadFactoringWireCharacterization; TestOperatorAgentConversationHandlersExposeReadOwner; compiled owner/CLI reads. |
| A03 exact identity and ordering | execution-proven through the same corrected path | TestOperatorReadFactoringOrderCharacterization; TestOperatorAgentConversationHandlersTypedErrors; both-store compiled scoped/ambiguous/foreign identity proof. |
| A04 diagnosis queue/active/watchdog/tool evidence | execution-proven through the same corrected path | TestAgentDiagnoseExactDeliveryPaginationParity; TestOperatorAgentDiagnoseFailsClosedOnMalformedOwnerData, TestOperatorAgentDiagnoseFailsClosedOnMalformedActiveOwnerData, TestOperatorAgentDiagnoseFailsClosedOnMalformedWatchdogOwnerData, TestOperatorAgentDiagnoseFailsClosedOnMalformedLastToolOutcomeOwnerData; TestAgentOperatorDiagnosisSnapshotAggregateToPageIsAtomicAcrossBackends. |
| A05 usage capability/window | execution-proven through the same corrected path | Capability/wire/order tests; TestOperatorAgentUsageRejectsInvalidWindow; TestOperatorAgentUsageFailsClosedOnMalformedOwnerData; actual agent.usage both-store RPC. |
| A06 richer delivery diagnostics | execution-proven through the same corrected path | TestOperatorReadFactoringWireCharacterization and TestOperatorReadFactoringOrderCharacterization; TestOperatorAgentDeliveryDiagnosticsRejectsLimits, TestOperatorAgentDeliveryDiagnosticsRejectsBadCursor, TestOperatorAgentDeliveryDiagnosticsFailsClosedOnMalformedOwnerData; TestOperatorAgentDeliveryPagesBoundHydrationParity. |
| A07 lifecycle statuses/page | execution-proven through the same corrected path | TestOperatorReadFactoringWireCharacterization, TestOperatorReadFactoringOrderCharacterization and TestOperatorReadFactoringCapabilityMatrix; TestOperatorAgentDeliveryLifecycleRejectsBadCursorAndStatuses, TestOperatorAgentDeliveryLifecycleFailsClosedOnMalformedOwnerData; TestOperatorAgentReadSurfaceLoadAgentDeliveryLifecyclePostgres, TestSQLiteRuntimeStoreLoadAgentDeliveryLifecycle and TestReadProofFactoringCompiledSurfaces. |
| A08 conversation filtering | execution-proven through the same corrected path | Independent conversation capability, wire/order tests; compiled unfiltered/run-only/exact-agent/flow-instance and negative identity cells on both stores. |
| A09 safe turn page/cursors | execution-proven through the same corrected path | TestOperatorConversationProjectionBackendParity; public list_turns RPC, compiled conversation view; keyset insertion proof. |
| A10 exact safe turn detail | execution-proven through the same corrected path | TestOperatorAgentConversationHandlersTypedErrors and TestOperatorConversationProjectionBackendParity; TestReadProofFactoringCompiledSurfaces actual get_turn RPC/compiled conversation turn; TestConversationTurnOutputsOnlyAuthorSafeFrameFacts. |
| A11 HTTP/auth/schema/envelope | execution-proven through the same corrected path | TestOperatorReadFactoringHTTPCharacterization all nine methods, zero owner hits; existing HTTP auth/envelope, OpenRPC compliance/runtime probes. |
| A12 corrupt/internal failures | execution-proven through the same corrected path | TestOperatorReadFactoringOrderCharacterization and unchanged hostile owner tests; TestAgentOperatorSnapshotRejectsMalformedBaseAuthorityAcrossBackends and TestAgentOperatorSnapshotRejectsMalformedRelatedAuthorityAcrossBackends. |
| A13 bounded selected-store parity | execution-proven through the same corrected path | Actual nine-method HTTP journey; diagnosis page parity, selected snapshot handlers, delivery bounded hydration, conversation projection and TestOperatorConversationKeysetInsertionParity on both stores. |
| A14 compiled read family | reproduced and fixed | TestReadProofFactoringCompiledSurfaces and TestReadProofFactoringCompiledAgentScopeBothStores; pre-fix 24 failures on unchanged master and candidate. |
| A14-V view run scope | reproduced and fixed | Both compiled tests, text/JSON/quiet; TestAgentReadScopeRejectsBeforeClientConstruction; TestAgentReadScopePreservesExactRunAndFlowOnPrefixRetry; refs-only output controls. |
| A14-D diagnose run scope | reproduced and fixed | Same both-store compiled/refusal/prefix proof; unchanged queue bounds/cursor and diagnosis output/error tests with explicit valid scope. |
| A14-L deliveries run scope | reproduced and fixed | Same compiled proof; existing lifecycle status/limit/cursor/JSON/quiet/error assertions retained with explicit run; foreign and ambiguous refusal. |
| A14-C filtered conversation scope | reproduced and fixed | Same both-store filtered/flow/foreign/ambiguous compiled proof; local zero-RPC refusal; unfiltered/run-only and exact session controls retained. |
| D01 nil/empty/root | execution-proven through the same corrected path | TestDescribeTextFactoringCharacterization exact complete and empty transcript, nil writer. |
| D02 all flow detail branches | execution-proven through the same corrected path | Same characterization plus compiled merged grammar/ingress/coordinator fixtures. |
| D03 graph nodes/edges | execution-proven through the same corrected path | Same exact transcript covers root/path/initial/terminal/from/node/handler/event/after/timer/loop/escape/decision/verdict. |
| D04 timers/joins | execution-proven through the same corrected path | Same transcript including optional timer and fan-in/window fields; compiled fan-in fixture. |
| D05 fanouts/gates | execution-proven through the same corrected path | Same transcript plus compiled golden graph and gate scenarios. |
| D06 diagnostics/remediation | execution-proven through the same corrected path | Same transcript authored/fallback locations and multiline evidence; unchanged diagnostic tests. |
| D07 topology sibling | execution-proven through the same corrected path | TestDescribeRoutesUsesVersionedTopologyAndMatchesFullDescribe and TestDescribeRoutesHumanAndJSONAreDeterministic; TestReadProofFactoringCompiledDescribe full/graph/routes fixtures. |
| D08 JSON/quiet/no-color | execution-proven through the same corrected path | TestReadProofFactoringCompiledDescribe: 45 baseline cells, 90 commands; only repository absolute path normalized. |
| D09 source/admission precedence | execution-proven through the same corrected path | TestDescribeMissingContractsIsValidationExit, TestDescribeCommandIgnoresMalformedRepoDotEnv, TestDescribeCommandDiagnosticsCarryRemediationAndEvidence, TestDescribeRoutesCarriesExistingDanglingEventDiagnostic and TestReadProofFactoringCompiledDescribe; unchanged source guards in default suite; no new source/runtime owner. |
| D10 merged shape/projection | execution-proven through the same corrected path | Compiled static/template/key/standing/fan-in/golden fixtures match pre-extraction SHA256 output hashes; authoring/topology controls. |
| D11 retired output-pin spec wording | reproduced and fixed | TestDescribeFactoringRetiredOutputPinFieldsRemainAbsent plus exact compiled baseline; authoritative contradictory key/carries prose corrected without output change. |
| C01 action preconditions | execution-proven through the same corrected path | TestScenarioCardFactoringAdmissionCharacterization; missing verdict/cross-anchor/public mailbox action controls. |
| C02 every anchor selector set | execution-proven through the same corrected path | Same admission matrix, each allowed/forbidden field across all three anchors; no RPC before admission. |
| C03 exact predicate comparisons | execution-proven through the same corrected path | TestScenarioCardFactoringMatchCharacterization one-field match/mismatch, notices/other anchors, no fuzzy equality. |
| C04 unique first/later match | execution-proven through the same corrected path | TestScenarioMailboxConsumesCompleteMatchSet: all three anchors x both actions, 200 first-page rows; no early detail/action. |
| C05 zero/ambiguous full set | execution-proven through the same corrected path | Same unchanged 66-case matrix; real human/proposed concurrent earlier insertion creates two matches and is refused. |
| C06 repeated/cyclic continuation | execution-proven through the same corrected path | Same unchanged matrix, exact retained params/cursor bytes over later pages, repeated and cyclic cursor refusal. |
| C07 malformed page/cursor/later error | execution-proven through the same corrected path | Same malformed/later-error matrix; TestScenarioCardFactoringDetailAndMutationFailures non-string cursor; actual foreign cursor codec refusal. |
| C08 detail/hash/stale fence | execution-proven through the same corrected path | New detail failure matrix RPC/hash/snapshot/tag failures; unchanged stale-content matrix, immutable hash forwarded only after full selection. |
| C09 mutation response/error | execution-proven through the same corrected path | New all-anchor/both-action not-ok/wrong-card/missing-change matrix; exactly one mutation, no retry; existing decide/defer controls. |
| C10 real mailbox pagination | execution-proven through the same corrected path | TestScenarioMailboxActualServerContinuationBothStores and TestScenarioCardFactoringActualContinuationBothStores, all anchors, >200, exact late match/ambiguity/foreign codec. |
| C11 compiled fresh public test | execution-proven through the same corrected path | TestReadProofFactoringCompiledScenario: three authored MockOnly text scenarios; JSON/quiet exit2 before nonexistent source/config/session, zero RPC and unchanged file census. |
| C12 cancellation and failure | execution-proven through the same corrected path | TestScenarioCardFactoringCancelledContinuationNeverReadsDetailOrMutates and TestScenarioCardFactoringDetailAndMutationFailures/cancel_get, joined HTTP cancellation, no action. |
| Q01 structural integrity | execution-proven through the same corrected path | API-spec/OpenRPC generation and admission tests, source/capability/identifier/userfacing guards; TestReleaseE2EPackageStaysAtPublicProcessBoundary; authoritative generated artifact match. |
| Q02 stores and siblings | execution-proven through the same corrected path | Real selected-store projection/pagination/snapshot proof; focused API/CLI race, authoring/topology/dashboard and CLI readers in default suite. |
| Q03 process/golden | execution-proven through the same corrected path | Unchanged SQLiteSmoke, RestartAndForcedKillOnBothBackends, both BurstConcurrency iterations; exact hosted heavy-test unit retained, no deadline/workload change. Managed combined supported run passed in 207.663s. |
| Q04 final qualification | execution-proven through the same corrected path | Default go run ./cmd/swarm-test passed all 14 planned required units, without --full, at 2026-10-01 10:02:29 UTC. Canonical independent base/head complexity and diff census pass. Exact-head hosted CI is a separate merge gate whose receipt is appended to the PR comment, never inferred from local success. |

Required supported-surface proof actually run:

```sh
go run ./cmd/swarm-test -- ./internal/releasee2e -run '^(TestReadProofFactoringCompiled(Describe|Surfaces)|TestGoldenAgentWorkload(SQLiteSmoke|RestartAndForcedKillOnBothBackends|BurstConcurrencyOnBothBackendsIteration[12]))$' -count=1 -timeout=15m
go test ./internal/cliapp -run '^TestReadProofFactoringCompiled(AgentScopeBothStores|Scenario)$' -count=1 -timeout=3m
go test ./internal/releasee2e -run '^TestReleaseE2EPackageStaysAtPublicProcessBoundary$' -count=1 -timeout=1m
go test ./internal/apiv1 -run '^(TestAgentOperator.*Snapshot.*|TestSelectedStoreAgentSnapshotHandlers.*|TestAgentDiagnoseExactDeliveryPaginationParity)$' -count=1 -timeout=3m
go test ./internal/store/internal/runtimepersistence -run '^(TestOperatorConversation(KeysetInsertionParity|ProjectionBackendParity|ReadSurfaceListUsesCanonicalProjection)|TestOperatorAgentDeliveryPagesBoundHydrationParity|TestOperatorAgentReadSurfaceLoadAgentDeliveryLifecyclePostgres|TestSQLiteRuntimeStoreLoadAgentDeliveryLifecycle)$' -count=1 -timeout=3m
go test -race ./internal/cliapp ./internal/apiv1 -run 'Test(OperatorReadFactoring|OperatorAgentConversation|AgentReadScope|Agent(View|Diagnose|Deliveries|ReadCommands|Output)|Conversation|Conversations|Describe|Scenario(CardFactoring|Mailbox))' -count=1 -timeout=5m
go run ./cmd/swarm-test
go run ./cmd/swarm-complexity -base origin/master -head HEAD
git diff --check origin/master...HEAD
```

Supported combined process proof passed in 207.663s; exact compiled scope proof
in 10.750s; selected snapshots in 19.700s; pagination/keyset/lifecycle store
proof in 6.612s; focused API/CLI race in 4.596s/56.551s. Final all-anchor detail
failure/cancellation race passed in 1.284s; the final combined named API/CLI
race command also passed in 38.858s/77.866s. New keyset insertion proof is
separately selected, not assumed to be in the default broad unit. During the
default run, the only additional committed source was two test-only files;
the default log explicitly includes the new detail failure matrix. Production
code and spec were unchanged throughout qualification. No backend skip or
isolated heavy-test pass substitutes for the combined process proof.

## Accounting, Watchlist And Residuals

Exact measured comparison: base `36997a6a0` to production head `7b595029c`.
Entry cyclomatic scores: API 82 -> 6, describe 65 -> 6, selector 56 -> 6.
All new extracted callables <25 (API max14, describe max12, selector max22).
Cyclo >=30 277 -> 274 / >=50 57 -> 54; cognitive >=30 588 -> 586 /
>=50 196 -> 193. No hotspot-count increase; checked baseline refreshed through
the canonical pinned analyzer, independent base/head check passes.
Production Go diff: 596 additions, 490 deletions, gross1086, net+106 across six
files. Spec/generated OpenRPC adds 56 gross lines; conservative combined1142
is below #2407's 3000 cap. Waived historical 1500/net-neutral rules are not
represented as still binding. No schema, ledger or persistence inventory change.
The exact final baseline refresh adds two explicit test-file classifications
for the new keyset and detail-failure proofs; production scores are unchanged.

Authorized existing-test edit inventory: agents_test.go, agent_diagnose_test.go,
agent_deliveries_test.go, agent_output_modes_test.go,
cli_identifier_registry_test.go, api_consumption_boundary_test.go. Only run
input/expected scope additions and the directly contradictory blank-run message
changed. All old output, option, privacy, prefix, error and zero-RPC assertions
remain. Other existing tests, including goldens, are byte-unchanged.

Watchlist decision: existing boundary_owned_decomposition and
invariant_suite_coverage nodes, approval at docs `3c6a941` and final local proof
refinement `946ac34`, map approved A14 absorption and the three separate families.
The refinement records earned execution, not parent death certificates.
#2447/#2407/#2250 remain open. Remaining named
maintenance tail: #2509 (tool/capability admission), #2510 (Claude continuation
and run-pinned data reads), #2511 (durable-data validation): three containers /
five families, low confidence until each child census. Implementation did not
discover a broader owner or change this conditional estimate.

Architecture feedback: explicit authority carriage and boundary-local
decomposition are implemented now. Further shared facades would blur different
owners. Broader model/phase debt stays in #2250; coverage/ratchet debt in
#2447/#2407, watchlist only for these closed child rows. No new issue or
POTENTIAL_ISSUES entry. This batch is about one engineering day plus qualification;
the four-entrance scope correction is hours, high ROI through fail-closed public
proof. No additional architecture rewrite is needed to close this chosen class.

Achieved closure claimed: failure class eliminated for the three separately
bounded maintenance families and the complete four-entrance A14 authority gap.
No same-concept old interpreter remains inside those boundaries. This is not
merge approval. Exact-head review and CI remain required.
No live-provider, signed-webhook, retained public-test, unrelated fork/lifecycle,
parent corpus or broader architecture closure is credited to this PR.
