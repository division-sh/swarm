# Pre-Implementation Coverage Audit: #2510

## Implementation Status Addendum

Reviewer-g approved the bounded first slice in5933895918; classification
correction1d58069d3 preceded characterization8e2fe986b and extractionbc8516dc7.
The original intake below remains historical, not the current coding status.
**Implementation is now frozen on a newly reproduced shared-origin contract
gap**, recorded in [the origin escalation](issue-2510-origin-escalation.md).
Candidate and untouched master both refuse the memory-enabled public directive
after successful initial HTTP MCP reads, settlement and hash restart on both
stores. BoardStep/directive must be added to the consumer classification; all
five provider continuations share the session binding helper. Neither a
delivery-claim bypass nor a Claude-only fix is authorized by the maintenance
gate. Reviewer-g's bounded split-or-absorb disposition is requested. Q01 and
final qualification remain incomplete; no closure or merge readiness claim.

R05 census precision: no supported source/store setter was found. These ports
are constructor-captured under the existing lock; the originally proposed
setter-race probe has no production entrance and earns no credit. Real
Executor/store execution and race controls qualify the actual capture/read
path, not an invented setter. This does not create a new writer or owner.

Agent-g, 2026-10-01. **Independent reviewer-g gate requested; no coding approval.**
Source baseline: merged `origin/master@e110bfb368369d701d3e7784f418addf02d2f4ec`.
Branch: `agent-g/2510-preaudit`. This artifact changes no production, tests,
authoritative spec, runtime ownership, or selected-store schema.

## Recorded Gate Addendum

The [independent ruling](https://github.com/division-sh/swarm/issues/2510#issuecomment-5933895918)
approves coding as a first slice of #2447 after this classification correction.
Peer provider continuations share the broader provider-turn lifecycle, but use
separate provider-specific orchestration and already consume the common owners.
Their surviving seams are tracked in #2447 comment5933882419, not falsely
classified as a different lifecycle concept or closed by this Claude slice.
G1 spec and G2 live usage corrections are approved, including normally derived
description/definition/capability identity changes with no access/compatibility
change. Q02 is optional without explicit provisioning and authorization: record
not-run/residual risk, never a pass. All mandatory C/R/Q01/Q03-Q08 proof, unchanged
tests, outer lifetimes, characterization-first, complexity and qualification
conditions remain binding. The original pending statements below are historical
submission context, superseded only by this explicit ruling.

## Binding Context, Category And Class Chain

Read the full #2510 issue (no prior comments), #2447's lead-ratified current
keep-zone allocation and #2407's current R1 ledger, plus #2509's design-only
ruling, #2482's current diff and #2514's current diff. Re-read
`swarm-docs/docs/IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md`.

Category: high-risk behavior-preserving maintenance. Observed symptoms are two
oversized real production paths, not a demonstrated new runtime defect:

| Existing callable | Cyclo | Cognitive | Real entrances |
| --- | ---: | ---: | --- |
| `(*ClaudeCLIRuntime).continueSession`, `internal/runtime/llm/cli_runtime.go` | 68 | 101 | Managed and fork-chat continuation; normal and selected-contract execution consume the managed entrance |
| `(*Executor).execReadResourceData`, `internal/runtime/tools/executor_flow_data.go` | 60 | 87 | Both `resource_row` and `resource_rows` arms of `read_flow_data` |

Measured by the unmodified pinned `cmd/swarm-complexity` on independent equal
base/head snapshots. Whole census: 19,083 callables; cyclo >=30:274, >=50:54,
max184; cognitive >=30:586, >=50:193, max302. This is eligibility evidence,
not proof that extraction preserves behavior.

There are **two distinct complete working maintenance classes**, not one
agent-turn framework:

- C: mixed phase orchestration across every branch of Claude turn continuation,
  including exact adopted-session authority, provider-state selection, launch,
  native/MCP evidence, settlement, mutable projection and outer cleanup.
- R: mixed authorization/read/integrity/selection/presentation across both
  run-pinned resource tool arms, including exact errors and both byte budgets.

Immediate parent for C: oversized provider-adapter orchestration under #2447.
Immediate parent for R: oversized authorized tool-read orchestration under
#2447. Broader parent: regression-prone keep-zone complexity in #2407; broader
lifetime/model redesign remains open #2250, not part of this maintenance batch.
The issue framing is broad enough for both chosen classes, and narrower than
the parent by an explicitly lead-ratified allocation. A named function was an
entry point, not the audit boundary: all entrances, underlying owners, sibling
adapters, catalog/MCP consumers, projection readers and store adapters were
searched before choosing the boundary.

Exact governing spec paths at this baseline:

- `engine.agent_session_management.agent_memory_contract` and session lifecycle:
  exact run/agent/flow memory identity and acknowledged session mutations.
- `engine.agent_session_management.native_tools` and
  `managed_agent_capability_surface`: exact positively selected bindings,
  valid-empty versus missing/invalid inventory, managed versus fork-chat
  authority, and no reconstruction from display names or calls.
- `managed_external_effect_authority.logical_operation`, `launch`, `settlement`,
  `settlement.current_delivery_response_continuation` and `recovery`: launch
  admission precedes process invocation, acknowledged phase evidence survives
  cleanup errors, started errors are nonretryable uncertainty, exact root-owned
  continuation recovery precedes frame/provider execution, and drained evidence
  cannot project successor state.
- `workspace_model.deployment_mapping.claude_provider_state`: exact lifetime
  tuple, disposable container/persistent private backing separation, confirmed
  head validation, and no fresh-session or old-home fallback.
- `durable_data_resources.declaration_projection.access`, `version_identity`,
  `static_data`, `workspace_projection` and `legacy_policy`: source-owned grants,
  exact run pins, canonical stored codec, bounded target-bound resource cursor,
  distinct static identities, and no latest/head/path/old-cursor fallback.
- `engine.agent_session_management.flow_data_access.generated_tool` and
  `tool_model.platform_builtin_tools.tools.read_flow_data` contain the stale
  claims explicitly escalated below. They cannot be silently used to restore
  filename authority or to remove the admitted resource arm.

## Full User-Visible Paths And Reachability Gates

### C: Serve/Run/Fork To An Agent Response

| Ordered gate | Classification | Owner and proof boundary |
| --- | --- | --- |
| Strict source admission, command selection and process/store possession | Different semantic concept, with proof | Existing contract/composition/worklifetime owners; compiled verify/serve controls, Q01/Q02; not a startup rewrite |
| Normal startup or selected-contract fork preparation/admission | Different semantic concept, with proof | Existing managed admission and selected-fork owners; `TestCompletionAuthorityReviewFindingParity`, Q03; fork-chat has its separate sandbox authority |
| Run/delivery admission, concrete actor, acquired memory and root response recovery | Different semantic concept, with proof | Conversation, live-session, delivery and effects owners; C02/C03/C19; recovered response must be checked at conversation root before creating a new frame |
| Validated managed/fork-chat call enters Claude adapter | Same chosen class C | Both typed wrappers lead to one continuation path, C01/C22 |
| Resolve current model, source tools, exact workspace/state, prompt and prior head | Same chosen class C | Adapter consumes existing model, capability and workspace owners; C04-C07 |
| Register MCP turn, fingerprint, admit completion, construct first/resumed launch | Same chosen class C | C08-C10; outer token registration survives all tool calls and is unregistered on every exit |
| Process invocation, tool relay and joined heartbeat/transport lifetime | Different semantic concept, with proof | `cli_runtime_process.go`, `cli_runtime_transport.go`, completion heartbeat, executor and gateway retain ownership; C11-C14 and Q03 |
| Validate response/inventory/calls, candidate head and projection authority | Same chosen class C | C12-C16; backing check and current grant precede mutable promotion |
| Settle, respect committed/drained outcome, project exact continuation/turn | Same chosen class C | C17-C20; shared selected-store/effects/session owners stay unchanged |
| Release lease, heartbeat, MCP token and invocation container | Same chosen class C for orchestration; underlying cleanup is a different owner | C21/C23/C24, conversation outer release; no shortened defer or detached join |

A successful turn requires all earlier gates to succeed; a later outcome is
not license to bypass an earlier failure. Cleanup may report an acknowledged
commit alongside an error; neither acknowledgment nor independent error may be
discarded. No new delivery/provider redispatch or retry policy is proposed.

### R: Authored Grant And Run Pin To Delivered Resource Rows

| Ordered gate | Classification | Owner and proof boundary |
| --- | --- | --- |
| Compile exact data_access declaration and immutable resource version | Different semantic concept, with proof | Admitted semantic source and durable-data codec/source owner; selected-store baseline and Q04 |
| Explicit run creation/pin and concrete actor dispatch | Different semantic concept, with proof | Existing run/pin and actor owners, Q01/Q04; data.show's explicit selector cannot substitute |
| Actor catalog, exact managed surface, MCP/API binding and tool-call authorization | Different semantic concept, with proof | Executor catalog, toolcapabilities, gateway/frame; Q01/Q03/Q04 and unchanged static invocation controls |
| Closed input decode and selected resource arm | Same chosen class R | `execReadFlowData`, exact arm fields, R01; static_file is separately classified |
| Run, declaration, source-owned actor grant and captured selected-store port | Same chosen class R | R02-R05; denial precedes store access; capture source/store together under existing read lock |
| Load exact run pin, metadata and immutable payload | Different semantic concept, with proof | Common durable-data store owner, both adapters; R06/R19/R20, Q04 |
| Verify returned declaration/cardinality, schema, canonical bytes and version | Same chosen class R | R07-R09; preserve defensive validation even though store also verifies |
| Select keyed/positional row or bounded page/cursor | Same chosen class R | R10-R16; resource cursor v2 is not static cursor v1 or operator keyset |
| Serialize candidate/final result with array and full envelope byte ceilings | Same chosen class R | R17/R18; preserve whole-envelope checks and exact canonical output |
| Return result through managed tool transcript/continuation | Different semantic concept, with proof | Existing tool-result/frame/effects owners; Q01/Q04; no new logging or paging framework |

## Canonical Owners And Systematic Consumption Census

The owners below are real semantic owners, not the first local files found.
Proposed helpers are private phase extractions under these owners. No semantic
authority moves to a helper, and no exported adapter, registry, store port,
state machine, framework or parallel owner is introduced.

Classification vocabulary: **already consumes the canonical owner**;
**moved to the canonical owner in this work**; **different semantic concept,
with proof**; **still bypasses and explicitly split/escalated**.

### C Owners And Every Known Consumer Family

| Owner | Consumer/seam | Classification and exact evidence |
| --- | --- | --- |
| ClaudeCLIRuntime adapter orchestration | `ContinueManagedSession` and `ContinueForkChatSession`; Conversation managed/fork-chat call sites | Already consumes; both wrappers delegate to continueSession. Whole C01-C24 path is factored, neither wrapper is a second implementation |
| Conversation root and immutable completion continuation | `stepManaged`, adapter recovery port and effects continuation projection | Already consumes; `TestAllManagedAdaptersDelegateRecoveryOnlyToConversationRoot`, C19; no recovery added inside per-frame Claude helpers |
| `agentmemory`, live-session acquisition, sessions Registry and shared release/increment outcomes | Claude, Anthropic API, OpenAI compatible, OpenAI responses and Mock Start/Prepare/Continue; Conversation persistence/rotation | Already consumes; same acquire/require-base/release helpers and typed outcomes; adoption, acknowledged mutation and cleanup tests C02/C03/C20/C21 |
| ManagedCall/ForkChatCall, managedcapabilities and provider contract | Claude managed/selected-fork turns, startup/prepared probes, gateway/MCP discovery and actual tools/call | Already consumes; managed surface is not reconstructed from actor tools or provider display lists; C08/C12-C14 and Q03 |
| Separate fork-chat sandbox policy | Conversation.RunForkChat, Claude fork-chat branch and forkchat completion owner | Different semantic concept, with proof; no managed surface is acquired. `TestForkChatRetainsCommittedAssistantThroughCleanupError`, C22 |
| Effects Controller/Handle, shared completion authority/outcome and common effect-persistence owner | Every provider adapter, startup probe, selected-fork execution, normal turn, relay and continuation recovery | Already consumes; `TestAllNormalProviderSuccessPathsConsumeCanonicalDrainedDisposition`, committed-phase and dual-store acknowledgment controls C17-C19/Q05. Transport response parsing remains provider-specific |
| CLI process and prompt transport | Nonstream/stream launch, watchdog monitor, MCP HTTP bridge and tool-result continuation | Already consumes; C10/C11 and existing transport/started-failure tests. No move of durable-launch-before-exec or heartbeat ownership |
| Workspace ClaudeState resolver and inspected container cleanup | Reusable memory, stateless delivery, non-delivery invocation, startup tmpfs and fork-chat tmpfs | Already consumes; exact state tuple, `CheckHead`, outer Conversation release; C07/C15/C23 and Q02. State lifetime is not provider resume-ID authority |
| Shared provider response/tool-output/usage records | Claude parser/capability observation, shared settlement and transcript; Anthropic/OpenAI/Mock response adapters | Already consumes shared records; native inventory/transport details are different concepts proven by C12-C14/Q03; no cross-provider response parser consolidation |
| Anthropic/OpenAI compatible/OpenAI responses/Mock/Noop provider orchestrators | Their own Continue implementations | Same broader provider-turn lifecycle, separate provider-specific orchestration, explicitly retained/tracked in #2447 comment5933882419; shared session/settlement owners already consumed. HTTP/Mock/Noop primitives are not a second Claude continuation. Sibling committed-phase tests Q03; existing scores 37/42/42/29 are not claimed eliminated |
| Selected-contract fork materialization and startup admission | Upstream preparation, actor census, lifecycle and session setup | Different semantic concept, with proof; same managed continuation after admission, `TestCompletionAuthorityReviewFindingParity`; no fork/runtime reconstruction rewrite |

No row is marked moved-to-owner: this is existing-owner maintenance, not an
ownership migration. No live duplicate Claude continuation or shared completion
authority bypass was found by caller/interface/assignment and lifecycle census.
If implementation finds one, this audit cannot authorize preserving it.

### R Owners And Every Known Consumer Family

| Owner | Consumer/seam | Classification and exact evidence |
| --- | --- | --- |
| Executor read_flow_data dispatch and resource projection | Handler registry -> execReadFlowData -> both resource arms -> execReadResourceData | Already consumes; one live resource read projection, R01-R18 |
| `semanticview.ResolveAgentDeclaration` and `flowdata.AllowedResourceData` | Executor authorization; actor catalog/schema generation; tool visibility; dataaccess.Build workspace projection | Already consumes; exact resolved owning declaration, not mutable actor slice or independent flow-name inference; R03/R04/Q04 |
| `ResourceAccessStore.LoadRunResourceAccess` and common `store/internal/durabledata.Owner` | Executor single-declaration read and dataaccess.Build multi-declaration projection; SQLiteRuntimeStore/PostgresStore thin adapters | Already consumes; repo-wide search finds only these two production read consumers and one store implementation; R05/R06/R19/R20 and Q04. Preserve store transaction and calls unchanged |
| `durabledata.CompileStoredJSONL`, manifest/VersionID and canonical business key | Common store integrity, Executor defensive integrity/row projection; imports and sinks use corresponding admitted codec entry | Already consumes; stored codec is distinct from import wire-size admission. Expanded-payload and corruption tests R08/R09; no import-limit substitution |
| `resourceDataCursor` encode/decode/target fingerprint | Only Executor resource page creation/readback | Already consumes; exact v2 proof domain, run/actor/version target and offset, strict unknown-field/EOF checks; R13-R16 |
| `durabledata.PageRequest/PageResult` and `toolresultpolicy` | Resource page defaults, array-byte ceiling and full inline-result ceiling; relay/tool result consumers | Already consumes; R12/R17/R18; do not replace selected bytes with row-count approximation |
| Static-data source/identity and static_file arm | `flowdata.Resolve`, catalog static IDs, workspace projection, static cursor v1 | Different semantic concept, with proof; source-owned immutable bytes versus selected-store version pins. Unchanged static invocation/foreign-ID and cursor controls Q04/Q06 |
| Materializer/workspace projection | Host and Docker /data mounts built from admitted static records and run pins | Different presentation/lifetime concept, with proof; already shares grants/read port, but is not a paging implementation; materializer integrity tests Q04 |
| Global operator data.show and CLI data show | Explicit declaration plus head/version/alias selector, bounded store pages and operator cursor | Different semantic concept, with proof; no actor data_access or tool v2 cursor. Closed #2506/PR2507 owns that maintenance; unchanged supported API controls Q06 |
| Provider/generated JSON-schema validation | Actor-scoped generated read_flow_data schema, MCP delivery, OpenAI/Mock provider schema admission | Different representation owner, with proof; generated schema does not replace execution exact-pair authorization. Q03/R03; #2509 post-merge census must separately classify these validators |
| `tools/usage.go` -> registry usage -> provider descriptions | read_flow_data filename-only guidance | Still bypasses the canonical current wire description; explicitly escalated as G2 below. It is live presentation, not another access evaluator; no compatibility input is added |

### Old Paths And Lifetime Boundaries

Remove the two monolithic mixed-phase bodies by complete private extraction, not
by duplicating them behind a facade. Existing wrappers, store reads, release
owners and cursor codecs remain authoritative. There is no dead resource pager
to claim removed and no provider owner to replace. Retired filename-only tool
inputs, ambient file roots, latest-version selection, old resource cursors,
display-name inventory fallback and per-frame response recovery stay invalid.

Claude's outer named-return defer must still join cleanup into the returned
error. It must close over the *current* lease after rotation, retain the session
heartbeat across the complete turn, and unregister MCP only after tool activity
ends. Private launch/result helpers may not release these resources early.
Conversation still owns invocation release. Resource extraction must keep the
source/store capture, validation/read order, canonical byte checks, paging loop
and final whole-result validation; no read/query/streaming optimization is part
of this issue.

## Spec Contradictions And Requested Bounded Dispositions

**G1 (blocking until gate disposition):** the flow_data_access generated-tool
section says input is filename, output size_bytes, generation is static-only,
and omitted static grants imply no tool. The builtin list repeats static-only
generation. Those claims contradict the canonical durable_data_resources
structured-access/legacy-policy sections and live closed kind/static_id or
declaration API. Proposed authoritative prose correction, in the eventual PR:

- flow_data_access alone grants only exact admitted static bytes; absence denies
  that arm, not a separately admitted data_access resource arm.
- Tool exists if static or resource grants exist. Static uses
  `{kind:static_file, static_id, cursor?}` and the existing chunk/continuation
  result. Resource row uses structured declaration plus exactly canonical key
  or 1-based position. Resource pages use declaration/page and existing v2
  opaque continuation with array and inline limits.
- Keep static path admission, package/flow grants and resource pin admission
  separate under their real owners; do not restore filename/size_bytes aliases,
  add a field, alter wire bytes or introduce a second grant checker.

**G2 (explicit live-presentation disposition required):**
`builtinToolUsageHints["read_flow_data"]` still tells providers to supply a
filename, via registry usage and delivered descriptions. This is not harmless
test-only text. Request absorption of a bounded description-only correction to
the *existing* usage owner, stating the actual three structured arms and exact
actor/run restrictions. It changes provider-facing prompt bytes, so it is an
explicit exception to byte-for-byte description preservation, NOT permission
to change authority, schema, selection, errors or settlement. If refused, lead
must explicitly track/split this live contradiction; it cannot be silently
claimed fixed. No new issue is proposed before that disposition.

G2 is the only proposed provider-description byte change. Existing definition
and capability owners must derive the corresponding description identity and
plan fingerprint normally; no old-description alias, retained-surface rewrite
or compatibility acceptance is proposed. Characterization records the old and
approved corrected description explicitly instead of claiming they are equal.

Both points are raised before coding. No spec/prompt edit has been made. Add
exact stale-claim and delivered-catalog assertions in Q07 once approved.
Other #2509 operational-verify spec corrections remain parked in #2509.

## Manifestation Matrix: Exact Planned Proof

All **52 rows below are planned**, not earned refactor/public closure.
New characterization is committed on the approved baseline **before**
extraction, then rerun unchanged after it. Existing tests stay intact.

`CChar` = planned `TestClaudeContinuationCharacterization` in llm, through the
real typed continuation and existing fake-Docker subprocess/turn gateway.
Record exact argv/request fingerprint, response bytes, typed failure reasons
and retryability, ordered launch/settlement/projection facts, and cleanup.
`RChar` = planned `TestResourceReadCharacterization` in tools, through
`Executor.Execute` with recording selected-read port, exact outputs/errors and
store-call trace. `RStore` = planned `TestResourceReadSelectedStoreParity`
with real SQLite/PostgreSQL run/pin/version storage and the Executor, not mocks.

| Row | Manifestation/invariant | Exact planned execution proof |
| --- | --- | --- |
| C01 | Managed and fork-chat typed inputs; nil session and invalid authority | CChar/typed_inputs; existing typed-call admission tests, zero process/settlement calls on refusal |
| C02 | Fresh versus acquired/adopted memory snapshot; mismatched plan/identity/base | CChar/acquired_base; `TestManagedSessionAdoptionFailsClosedBeforeProviderDispatch`, `TestAllManagedAdaptersGateAdoptedSessionIdentityBeforeProviderLaunch`; exact release and zero launch |
| C03 | Partial acquisition, unacknowledged release and heartbeat loss | CChar/acquisition_cleanup; shared release controls and exact joined typed cause/cleanup trace |
| C04 | Inactive inbound delivery refuses before provider launch | CChar/inactive_delivery, zero launch, no mutable projection |
| C05 | Model/credentials/current capability errors preserve precedence | CChar/admission_precedence, paired invalid input cases and exact reason plus calls |
| C06 | Empty prompt and missing confirmed prior head | CChar/prompt_head, exact ParseFailures delta and zero launch |
| C07 | Memory/delivery/non-delivery/fork state selection; missing/foreign backing | CChar/state_namespace and `TestClaudeInvocationReleasePreservesSessionReadbackAndError`; exact resolver request and no fallback |
| C08 | Native-only, MCP-only, mixed and valid-empty selected surface | CChar/native_only,mcp_only,mixed,empty; existing CLI tool argument/managed frame controls; exact argv and native/MCP inventories separately |
| C09 | First versus resumed turn; deterministic child/fingerprint | CChar/first,resumed; --session-id exact attempt child, --resume/--fork-session prior head, system prompt placement and no random ID |
| C10 | Durable launch acknowledgment/cleanup fault versus no process invocation | `TestClaudeCLICommittedPhasesPreserveProcessAndResponse` plus CChar/launch_evidence; exact no-dispatch/started distinction |
| C11 | Prelaunch cancellation, started timeout/cancel/process failure | CChar/cancel_before,cancel_started,process_failure; `TestClaudeStartedTimeoutIsTerminal`, `TestClaudeCLIUncertainFailureDisposition`; no retryable started error |
| C12 | Missing/null/malformed/unexpected-native inventory, fresh/resumed | `TestClaudeCLIManagedInventoryFailureSettlesWithoutRetry` retained; CChar/inventory, exact uncertain settlement/no redispatch |
| C13 | MCP occurrence/token authority and provider tool-call mismatch | CChar/mcp_authority; existing turn-context and observed-call validation tests; catalog/occurrence tied to exact turn |
| C14 | Fork builtin projection and managed tool accounting/emit output | CChar/tool_accounting; existing fork projection and managed request controls, native calls not MCP counts |
| C15 | Returned child-session mismatch or unusable transcript | CChar/candidate_head; CheckHead refusal before confirmed head or conversation mutation |
| C16 | Projection grant lost after response/candidate backing | CChar/stale_projection; outcome evidence retained, no mutable head/turn update |
| C17 | Usage available/unavailable, valid/invalid cache subtotals | Existing `TestClaudeCompletionUsageFromRawDirectAndStream` and invalid-cache control; CChar/usage preserves settlement record |
| C18 | Current versus drained settlement; commit acknowledged with later error | `TestClaudeMemoryDrainedCompletionStopsBeforeProviderHeadProjection`, committed-phase controls, CChar/drained; exact no-current-projection outcome |
| C19 | Immutable root recovery, already-projected response and tool continuation | `TestSettledCompletionRecoveryUsesImmutableProjectedTurn`, root-only guard plus CChar/recovery; provider invocation count unchanged |
| C20 | Conversation/turn increment acknowledgment and persistence errors | CChar/turn_projection; `TestCompletedSessionTurnIncrementRejectsUnacknowledgedMutation`; acknowledged response retained once |
| C21 | Outer release and cleanup failure preserve response/error combination | CChar/outer_cleanup; `TestCompletedSessionReleaseRespectsAcknowledgementAndPriorError`; full-turn lease lifetime and joined error |
| C22 | Real fork-chat success/error acknowledgment under separate sandbox owner | `TestForkChatRetainsCommittedAssistantThroughCleanupError`, `TestForkChatRetainsExactResponseAfterAcknowledgedTurnIncrementError`; CChar/fork_chat |
| C23 | Restart/adoption of exact confirmed session and private backing | Q02 retained serve/restart plus CChar/restart_adoption; exact run/actor/session/provider head, no stale-head fallback |
| C24 | MCP unregister, heartbeat join, rotated lease and invocation release on every exit | CChar/cleanup_matrix with barriers/recorded trace; race controls; no shortened resource lifetime |
| R01 | Closed input union, unknown kind/field, retired filename and crossed arm | RChar/input_union; exact handler refusal and zero selected-read calls |
| R02 | Missing run, missing/malformed declaration precedence | RChar/request_precedence, exact error and zero read |
| R03 | Source-owned resource grant, exact pair versus independently allowed flow/event | RChar/grant_pair; generated schema cannot authorize crossed pair; zero read on denial |
| R04 | Mutable actor grants, sibling owning flow/run, source absence | RChar/actor_source; existing actor/flow tests and RStore/foreign_actor |
| R05 | Missing selected store and consistent source/store capture | RChar/store_capture, injected port error and exact call cardinality; race with supported setter, no source inference |
| R06 | Missing run/pin, foreign declaration, pin schema mismatch | RStore/pin_admission/sqlite,postgres; RChar/read_errors; typed selected-store reasons preserved |
| R07 | Empty/multiple returned records and wrong returned declaration | RChar/returned_identity, typed integrity failure before row presentation |
| R08 | Malformed schema/rows, wrong VersionID, noncanonical-but-equivalent payload | RChar/canonical_integrity plus `TestExpandedCanonicalPayloadIsReadableAndIntegrityCheckedBothStores`; no row leaked |
| R09 | Legal canonical payload expanded above import-wire limit; empty version | `TestExecutorReadResourceDataAcceptsExpandedCanonicalPayload`, RChar/empty_version, RStore/canonical_payload; stored codec not import admission |
| R10 | Keyless positive position, duplicate payload multiplicity, missing/out-of-range | RChar/keyless; RStore/keyless; exact VersionMissing versus input error |
| R11 | Keyed scalar types, canonical key matching, missing key, invalid key/position mix | RChar/keyed; RStore/keyed; use BusinessKeyFromValue and exact keyed row order |
| R12 | Page absent, default/item/byte limits, invalid page before cursor identity | RChar/page_defaults_precedence, exact errors/defaults and end/more values |
| R13 | Malformed/base64/oversized/unknown-field/trailing JSON/checksum cursor | RChar/cursor_decode; existing bounded cursor test, exact rejection |
| R14 | Foreign run, concrete actor, declaration or selected version cursor | RChar/cursor_binding; `TestExecutorReadResourceDataCursorIsBoundedAndTargetBound`; no latest or sibling acceptance |
| R15 | Static v1/old resource version and zero/end/negative/out-of-range offset | RChar/cursor_coordinate; exact fail-closed, no compatibility reader |
| R16 | Long declaration names, bounded target fingerprint and exact multi-page traversal | RChar/cursor_roundtrip; RStore/pages; no embedded declaration growth, omissions or duplicates |
| R17 | Array byte ceiling versus full envelope ceiling; oversized first/later row | RChar/page_budgets; `TestExecutorReadResourceDataFitsInlineEnvelopeAndExactArrayBudget`, `TestExecutorReadResourceDataRejectsIntrinsicallyOversizedRowsBeforeProjection` |
| R18 | Single-row and final page envelope serialization, canonical row/value/key output | RChar/inline_and_final, exact serialized output/continuation/item counts/byte counts |
| R19 | Advancing global head does not change existing run pin across restart | RStore/pinned_restart/sqlite,postgres plus Q04; same version/row output and cursor target after reopen |
| R20 | Pruned/missing/corrupt selected payload and failed store read | RStore/payload_refusal/sqlite,postgres and RChar/store_error; no mutable projection/head fallback |
| Q01 | Compiled public run/serve -> Claude native/MCP resource tool -> emitted event -> settled delivery | Planned `TestClaudeResourceReadSupportedServeRestart`, fake-provider transport mode on both stores; public process and HTTP/MCP evidence, exact typed tool result/event/receipt; not real-provider credit |
| Q02 | Same-store public serve restart with real Claude/private backing | Existing opt-in `TestCommandLiveServeAndRestartParity` on both stores, after prerequisites and dedicated-chat authorization. Optional per recorded gate when unavailable/not authorized: not run and residual risk, never a pass. Real provider/Telegram proof distinct from mandatory Q01; do not replay sent deliveries |
| Q03 | HTTP/Mock sibling completion, startup/prepared fork probes, provider/generated schema boundaries | Existing committed-phase tests, selected-fork prepared-probe, provider schema and generated-schema closure controls; no Claude-only shared-owner claim |
| Q04 | Actual selected-store Executor and dataaccess projection consumers | RStore all backend leaves plus materializer integrity controls; retained compiled mock lifecycle gives internal-harness credit only, not public live serve |
| Q05 | Settlement/current/drained/fork authority and cancellation/commit evidence parity | `TestCompletionAuthorityReviewFindingParity`, `TestCompletionTransactionAcknowledgementBoundaryBothStores`; backend leaves must execute, not skip |
| Q06 | Unchanged static tool, operator data.show, CLI and golden/restart siblings | Existing six durable-data invocation shards, closed-family API/CLI controls and golden dual-store restart through managed selectors; preserve their original assertions and proof scope |
| Q07 | G1 spec alignment and G2 exact delivered usage disposition | Planned `TestFlowDataToolSpecMatchesStructuredArms` and `TestFlowDataUsageMatchesDeliveredSchema`; assert all stale claims/filename guidance gone if approved and schemas/results unchanged |
| Q08 | Exact pinned complexity, change accounting, proof metadata and managed qualification | `go run ./cmd/swarm-complexity -base origin/master -head HEAD`; every factored callable <25, unchanged pinned analyzers/guards; actual production diff <=3000; `go run ./cmd/swarm-test`, no --full or capacity bypass, final-head CI and proof audit |

Q01's fixture must compile the public binary, author a resource-access agent,
admit data and run pins through public commands, and launch ordinary live-posture
serve with a deterministic provider/Docker transport double, not MockOnly
execution selection or an internal runtime entrance. The provider subprocess
must consume its supplied MCP turn token and perform actual HTTP tools/list
and tools/call for resource rows plus the admitted emit; synthesized tool logs
or direct store writes cannot substitute. Stop and restart the same served
project/store, then prove a later turn consumes the same pinned version and
proper resumed head. Q04 separately earns real selected-store Executor and
internal retained-lifecycle credit. Q02 alone earns real Claude/Docker/Telegram
credit, with fresh isolated stores and explicit prerequisites/authorization.
These three surfaces cannot be substituted for one another.

Generic regression proof: CChar and RChar are table-driven invariant suites
through the real affected entrances, not tests only of new helpers. Before
extraction they pin branch outputs, reasons, acknowledgment versus error,
ordered authority calls and resource lifetimes. Disposable negative mutations
(early token release, skip current-projection disposition, use current head,
drop actor/version cursor binding, omit envelope budget) must make the relevant
rows fail; mutation candidates never ship. A nondiscriminating or unreachable
checkpoint stops coding for gate repair rather than receiving same-seam credit.

## Baseline Execution Actually Run

At `e110bfb36`, unchanged focused controls pass:

- `go test -race ./internal/runtime/llm -run` ten exact continuation/managed
  frame/inventory/committed-phase/adoption/drained/release/started-error tests:
  PASS, 16.047s. This includes fake subprocess evidence, not a real-provider turn.
- `go test -race ./internal/runtime/tools ./internal/runtime/flowdata
  ./internal/runtime/dataaccess -run` existing resource/static/actor/source and
  materializer controls: PASS, 8.627s / 2.357s / 1.020s respectively.
- With the canonical host PostgreSQL test DSN, focused tests in
  `store/internal/durabledata`, `store/internal/runtimepersistence`, and `store`
  execute actual SQLite and PostgreSQL leaves: expanded payload/corruption,
  authority-review matrix, acknowledged transaction boundaries, selected-store
  lifecycle/replay/prune, keyless order/multiplicity/schema and payload
  corruption: PASS, 1.839s / 7.853s / 2.390s.
- Separate focused CLI native/MCP/fork projection, HTTP/Mock committed-phase,
  prepared selected-fork and provider-schema controls: PASS, llm 0.008s.
  Generated emit/role schema closure and exact canonical tool projection
  controls: PASS, tools 1.126s. These qualify sibling boundaries, not a new
  provider/resource supported journey.
- Exact equal-snapshot complexity measurement: PASS. No production diff exists.

No new characterization, live call, serve/restart, complete golden workload,
full qualification or post-extraction proof has run. Existing green baseline
tests do not establish extraction or G1/G2 closure.

## Parent Probe, Watchlist Promotion And Tracker Decision

Consulted maintenance `boundary_owned_decomposition` and
`invariant_suite_coverage`, plus semantic
`durable_resource_version_ingestion_and_run_snapshot_ownership`. Their live
evidence requires full authority, both-store, lifetime and executable-assertion
coverage; a smaller happy-path/helper extraction would be dishonest. It does
not justify merging provider transports, tool/output schema dialects, operator
pagination or R5 readiness reconstruction into a new framework.

Parent action: keep the lead-ratified two-family maintenance child, closing
both C and R entirely, not a smaller Claude/resource first branch. Keep #2447
and #2407 explicitly open. Other named child containers: #2509 two admission
families (design-approved/coding hold on #2482) and #2511 durable-data aggregate
validation. After eventual #2510 closure, estimated named tail is **two
containers / three semantic families**, medium confidence in enumeration, low
confidence in effort until stable #2509/#2511 censuses; approximately 4-8
engineering days plus dependency/review time. No parent closure is inferred.

No target-file overlap with open #2482 or #2514 was found in their exact diffs;
adjacent effects/contracts/generated schema remain integration controls.
#2482 stays OPEN `0f27aa11041e573fa05a2128cdccba96b1a5e564` and #2514 stays OPEN
`afa1644642769a95e36e06fe92df9ec41b4a418f` at census time. Their candidate
behavior is not treated as merged. Consume actual master before implementation
and repeat changed-owner controls; a changed class requires repair/re-gate.
Other agents' work and #2008 WIP remain untouched.

Tracker-state decision: update #2510's body to this measured complete C/R scope,
52-row plan and explicit G1/G2 disposition requests before coding; refine the
existing mapped watchlist nodes and record the #2509 parking decision. No new
child, umbrella, node or POTENTIAL_ISSUES entry is warranted. No older active
stream is superseded. #2506/#2508 remain closed, not reopened for this work.

Architecture feedback: long mutable continuation routines make lease/defer
lifetimes and commit-versus-error distinctions hard to inspect; authorized
resource reads mix access admission with codec, cursor and presentation logic.
Long-run better direction here is **private bounded phase helpers within the
existing owners, with caller-level characterization and visible outer
lifetimes**, not a general provider/transaction/pager framework. Track this
feedback in #2510/#2447 and existing nodes; #2250 remains broader debt. Estimated
2-4 engineering days for this complete batch after the gate, medium confidence;
high regression/review ROI, with provisioning time separately uncertain.

Intended closure: **the two chosen working maintenance classes eliminated within
the approved first-slice boundaries**, not the broader provider-turn lifecycle,
after all rows and final-head audit/qualification, while shared owners and
parent architecture classes remain unchanged/open. This PR commits to complete
C and R closure. It is feasible in one bounded PR; fixing only a wrapper or
one resource arm would not close it. No second live same-concept authority
interpreter is intentionally retained. G2 is a disclosed presentation defect
whose disposition must be recorded, not silently treated as a different owner.

Binding retained rules: all existing tests unchanged; characterize before
factoring; every factored callable <25; exact independent Git snapshot ratchet;
#2407's 3000-production-line cap; actual additions/deletions/duplication reported.
The lead waived one-family-per-PR, 1500-gross and net-neutral/negative rules for
these batches only. No vendor or third-party source edit is authorized.

**Stop conditions:** unresolved G1/G2 ruling; new same-concept authority bypass;
need to change provider admission/retry/settlement, run pin or cursor semantics;
shortened cleanup/join lifetime; required startup/general framework change;
unable to construct a discriminating supported checkpoint; contradictory
merged dependency; unreachable required proof or unavailable Q02 without an
explicit reviewed disposition. Repair the issue/gate before widening. Merely
ordinary in-scope test failures are repaired, not suppressed or retried green.

**Independent gate is pending.** Reviewer-g must explicitly record its scope,
spec/presentation dispositions, proof conditions and coding decision on #2510.
This artifact is not self-approval, completed proof, or merge readiness.
