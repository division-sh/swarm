# #2700 Focused HTTP Effect Retirement PLAN Amendment

Track: FULL. ONE #2700 landing, per reviewer-a6220928385. This is the required
C1'' PLAN amendment before the new handler semantic cutover, not a request to
ship the now-moot notice. Input is signeddd39a85e4aa0a5b15dc7c6a0d4848cc4dd8f5ef8
on master0b7f1188a63cf375f4da11d03d1699edfbf5faf1. No production changes in
this amendment, no push before B #2698 lands; actual post-B #2592 comes first.

Binding USER ruling6220888538 deletes authored effect_class. It supersedes the
declaration/notice part of6218884753 and acceptance6220601595. Verify promise
8(i),6217749380, and the O04 correction6219466832 still bind. Every HTTP tool
call, handler or agent, uses conservative interruption behavior: after launch,
timeout/transport/body interruption or crash leaves outcome uncertain and never
authorizes automatic redispatch. GET does not imply safety. Preparation or
admission refusal before launch remains distinct. Acknowledged success and exact
retained result reuse remain supported. No new effect-policy framework.

## Finite Owner And Consumer Cut

Paths below are relative to internal/ unless prefixed platform-spec or .github.
They distinguish the authored selector to remove from internal accounting to keep.

| Boundary / owners | Bounded disposition |
| --- | --- |
| runtime/contracts/workflow_contract_tools_value.go: projectToolDeclarationsValue/projectToolValue, closed names, branch-presence validation | Remove effect_class from legal tool keys and scalar/branch projections. Its presence, independent of value, gets the exact ruled teaching refusal at its authored file/line/column through this shared admission owner, including agent-only tools. No compatibility translation. |
| runtime/contracts/tool_schema_entry_value.go: draft effect, WithToolEffect, WithEffect, Effect, EffectClassProvenance, declaration serialization/effective projection | Remove authored storage, setters/getter, serialization and provenance output for this key. Internal HTTP execution planning obtains the fixed existing conservative activity policy, not a synthetic authored value or inference from HTTP method. Keep other provenance, category, permission, input/output schema, rate/credential and handler admission. |
| runtime/contracts/workflow_contract_activity.go; runtime/activity_validation.go; runtime/engine/executor.go: stepActivity | Stop demanding/selecting an authored class. For authored HTTP activity create the existing non-idempotent journal policy, attempt1/no-backoff and its current conservative fork policy. Keep exact handler/rule site, activity/result IDs, input, source/contract pin, loop generation, approval, result schema and posture checks. Remove reserved authored-value branches, not unrelated activity validation. |
| runtime/pipeline/activity_engine.go: executeActivityIntent, executeNonIdempotentActivityIntent, reuseExistingNonIdempotentActivityAttempt | All authored HTTP handler work enters the existing durable attempt path; remove the separate read-only retry interpreter. Reuse exact terminal journal/result evidence before any fresh provider dispatch. Keep claim acknowledgment-loss reconciliation, native identity validation, loop-generation fencing and result publication ownership. Merely deleting the loop or setting maxAttempts1 is not the repair. |
| runtime/pipeline/activity_engine.go: prepareActivityHTTPTool, executePreparedActivityHTTPTool | Keep request preparation, static/managed credentials, private-target admission, output validation and redaction. After the existing launch boundary all timeout/transport/response-read interruptions map to the existing typed uncertainty family with original cause preserved, not a retryable read failure. Before launch retain exact typed preparation/admission failures. The prepared authored duration covers headers and body; use a per-call client copy with only competing Timeout removed, preserving Transport, Jar and CheckRedirect. Earlier caller cancellation wins. No new timeout allowance or earlier budget start. |
| runtime/pipeline/activity_lineage.go: FreshActivityRequestLineage, SelectedActivityRequestLineage, ValidateResult; activity_publication.go; activity_attempt_record.go; activity_journal.go; author_activity.go | Fresh HTTP lineage is checked against the fixed policy through the existing planning owner. Keep exact request/result bytes, source/run/route/mode/input hash/generation and terminal timestamps. Retained payload/journal effect_class fields are evidence, not a request to choose a retry mode. Preserve canonical result and story envelopes. |
| store/internal/backend/activityjournal/{journal,owner}.go; runtime/pipeline/persistence_ports.go; selected/generated forwarding ports | Reuse ClaimActivityAttemptForLoopGeneration, LoadActivityAttempt, CompleteActivityAttempt and MarkActivityAttemptUncertain. Preserve native source admission, transaction identity, attempt1 CAS, acknowledgment semantics and fences. No new table, SQL authority hook, schema relaxation, or migration. New native SQL is not planned. |
| runtime/tools/{executor,executor_http,effect_launch}.go, agent_http_effect_class_test.go; runtime/effects and store/internal/backend/effectpersistence | Agent HTTP already uses ClassWriteOrUnknown through existing managed launch/settlement owners. Keep it, with exact operation/attempt and physical dispatch counts, including timeout. Replace the declaration-positive class matrix with key-free method/interruption/reopen controls. Do not delete unrelated internal classifications or durable managed-effect columns. |
| runtime/registration/provider_http.go; providerconnectors/{providerconnectors,catalog_generator,generator_source}.go and current generator profiles/output | Remove authored-tool class projection and obsolete validation/WithToolEffect calls. Current HTTP profile fields that exist solely to emit this retired tool key also go; regenerated current connector tool output is key-free. Preserve registration discovery/write roles through their existing protocol/category/schema owner, not an authored class or inferred GET safety. Preserve connector category/operation IDs, schemas, normalization, credential owner, response mapping/success and mock response admission. No new connector interpretation. |
| runtime/tools/channel_runtime.go; runtime/contracts/platform_interface_value.go; packs/{channel,channel_registration}.go; serveapp/channel_native.go | Inspect projection callers of removed ToolSchemaEntry APIs and cut only that dependency. Channel-operation/native metadata classification is not automatically the retired tools.yaml selector. Preserve operation binding, opaque generation, exact target/permissions, private admission and native non-HTTP policy. Any HTTP projection uses fixed conservative behavior, never a read-retry escape from internal metadata. |
| runtime/pipeline/workflow_gate_decision.go; runtime/decisioncard/proposed_effect.go; store/internal/backend/decisionpersistence/proposed_effect_cards.go | Keep existing approved-effect continuation, frozen card/digest, own reply binding and current-generation authority. Internal recorded class/digest fields are not removed or rewritten; fresh key-free HTTP plans still use this same owner. Approval is not weakened to compensate for removed authoring syntax. |
| runtime/runforkexecution/activity_fork_execution_test.go; store/internal/backend/runforkpersistence/run_fork_activity_generation.go and existing activity-result continuation readers | Retain exact fixed-cut attempt/result, recursive result-content equivalence, pending-input accounting, selected authority and current generation. No retained read-only policy may authorize fresh redispatch. Finished evidence is read as evidence, not normalized into a new request or mutated in place. Preserve confirmation/refusal for unresolved effects. |
| runtime/tools/agent_tool_effect_declarations.go; tools/effective_tool_verification.go; runtime/authoringview/view.go; CLI notice-only tests | Delete the now-moot notice producer, notice-only factorization and INFO/spec/test expectations. Restore ordinary validation projection where helper factoring is now unnecessary; keep unrelated actor scoping, provider/MCP permissions, admission diagnostics and describe evidence. |
| runtime/failures/agent_tool_presentation.go; llm/conversation.go; mcp/gateway.go | Keep accepted plain agent text, strict canonical machine envelope and separate agent presentation. Keep transported uncertainty's early return: it must not consume a continuation, create feedback or start another model turn. Typed handler uncertainty is not replaced by agent text. |
| platform-spec.yaml; current repo-owned tool fixtures; audit and finite history successors | Delete common_optional_fields.effect_class and its current declaration examples/policy text. Clarify one interruption behavior and timeout honored-or-refused. Current fixtures use key-free tools; old spelling tests become teaching-refusal controls. Frozen packets and earlier receipts stay byte-identical; append exact successors where current-source checks require them. |

Explicit exclusions: the user's jobflow and twitter-gen bundles are USER-owned
edits. Do not touch them. Do not mass-delete internal external-effect classes,
channel/native non-HTTP accounting, result payload fields, proposal digests,
activity_attempts.effect_class, managed-effect schema or historical assertions.
No new migration or cut-derived identity; no changes to retry policy for
unrelated non-HTTP operations.

## Existing Durable Execution And Recovery State Machine

1. Shared source admission rejects the retired key before tool discovery/start.
   Verify and run start share it; describe does not re-implement validation.
   The timeout representability check stays the same shared O04 owner. There
   is no clamp, silently ignored key or notice-only acceptance.
2. Handler planning uses the fixed existing conservative HTTP policy, preserving
   the exact request ID, source and loop binding. Durable claim/identity checks
   happen through the current activity journal before provider dispatch. Claim
   acknowledgment loss reconciles by exact readback; it cannot grant a second
   attempt. Prelaunch preparation failures settle as known failures, not as
   evidence that a provider call may have executed.
3. Only the inserted/current admitted attempt launches. After launch, timeout,
   transport failure and interrupted body become a typed uncertain result;
   MarkActivityAttemptUncertain and publication keep the existing durable CAS
   and completion timestamp. Client cancellation does not clear the attempt or
   allow read retry. Settlement errors and acknowledgment-loss diagnostics stay
   visible; an unacknowledged terminal commit is reconciled, not resent.
4. On retry/reopen, exact terminal records reuse their original result event,
   timestamp and reply context. A retained started record is an unresolved
   possibly-launched attempt, never fresh work. Preserve the existing no-launch
   behavior and expose uncertainty through the existing caller/recovery result
   path. Do NOT automatically terminalize a potentially LIVE duplicate just
   because LoadActivityAttempt returns started. A recovery transition, if
   needed, must use the already-current execution/delivery owner and existing
   MarkActivityAttemptUncertain CAS; no speculative second coordinator or
   new ownership table. Prove the live-duplicate and process-reopen cuts
   separately. If the existing owner cannot establish that transition, surface
   the exact missing evidence before changing that state machine.
5. Read retained request/result fields without deleting or rewriting them.
   Exact native terminal journals and recorded-result readers keep class,
   attempt, source, identity and payload equivalence as historical evidence;
   terminal reuse precedes fresh-execution-only policy checks. Old unresolved
   work does not get a made-up journal or new request ID. It must remain
   conservatively unresolved/refused through the existing owner, never enter
   reexecute_read. Fresh bundle admission does not grandfather old tool syntax.
6. Keep existing credential acquisition/refresh/known-response validation
   separate from retry after an unknown result. A complete known401 remains
   governed by the existing managed credential owner; no timeout or partial
   response may trigger token refresh and redispatch. Test redaction and exact
   dispatch counts at that boundary. Mock success stays journaled and cannot
   bypass response admission or create a second physical call.

## REDs And Bounded Proof Matrix

New cells run first against dd39a85e4a with every result retained, then against
the bounded repair. Existing slow receipts carry only unchanged-path credit.

| Cell | Expected vs current behavior / exit proof |
| --- | --- |
| R01 retired-key admission | All old values (read_only/idempotent_write/non_idempotent_write/long_running/unknown), empty/null/non-string, handler and agent-only, refuse by key presence with exact message `effect_class is not a tool key; interrupted HTTP calls are outcome-uncertain and not retried` and source location. Current dd39 accepts legal old values. Omitted key works through actual source load/verify/describe; unrelated unknown-field/presence negatives stay. |
| R02 fixed planning/lineage | Key-free GET and POST handler/rule activities produce the same fixed durable policy and exact current request/input/result/generation identities. Current key-free handler admission rejects missing effect_class. No activity-only late bundle-level refusal after verify-green. Preserve approval/selection/foreign source and loop negatives. |
| R03 no read redispatch | Both stores, GET/POST x transport error/header timeout/body interruption/earlier caller cancellation after physical dispatch. Assert one provider call, one native attempt, typed uncertainty, retained cause, no retry/backoff or new result ID, exact once-only result settlement. Current read-only retry and timeout-as-known-failure are expected REDs. Keep prelaunch failure controls with zero calls. |
| R04 crash/reopen and live duplicate | Both stores, stop/reopen after durable start/physical launch before result settlement; reuse original request. Prove no second dispatch or attempt, uncertainty/unresolved evidence retained under current recovery owner. Separately hold live first attempt and deliver duplicate work: no stolen terminalization, extra dispatch or changed ownership. Race x3; original deadlines. |
| R05 acknowledged result reuse | Both-store successful and failed/uncertain terminal result publication, acknowledged commit plus post-commit diagnostic, lost acknowledgment, same-owner retry and reopened owner; exact result/time/reply/attempt counts. Preserve TestActivityTerminalResultUsesDurableTimestampBothStores, credential/mock terminal replay, acknowledged-settlement cleanup and invalid timestamp negatives. |
| R06 selected/fork/history | Retained terminal accounting after loop advance, result-consumption ordering, selected SIGKILL/reactivation, copied physical result refork and pending-input settlement. Existing current-generation, request/result-content and stale-work refusals remain. Unresolved HTTP work never gains permission from old read_only/fork policy; no historical packet mutation or confirmation relaxation. |
| R07 credentials/connectors/mock | Static/managed credential success/prelaunch refusal/redaction; complete401 refresh distinct from interrupted-response uncertainty; connector result mapping/schema; replay with terminal result does not require current response plan or credential. Preserve native channel generation/target controls and mock mode. Physical-call counts prove which owner dispatched. |
| R08 agent execution/presentation | Both-store GET/POST interruption/crash-reopen/prelaunch/acknowledgment controls through real managed owner. Keep write_or_unknown, exact native operation/attempt and dispatch counts; plain agent text in ordinary conversation and MCP; canonical envelope byte/cause controls; transported continuation still refuses consumption. Delete only declaration-positive notice cases, not general feedback/execution coverage. |
| R09 budgets/O04 | Keep exact authored request-deadline controls, >30s slow proof, headers/body coverage, shorter caller and shared-client immutability; reject unrepresentable seconds in shared admission before multiply. Add handler outgoing-budget/client-copy control through its normal prepare/dispatch path, retaining managed token and custom search checks. No allowance or budget-start change. |

Ownership/recovery/concurrency selections run race x3, both stores, original
business/package limits, via the normal runner. Tests assert physical provider
calls and canonical native attempt/settlement observations, not only text.
No production ForTest SQL hook, baseline/debt increase or new exemption.

## Scope Supersession And Qualification

The accepted dd39 timeout/O04, ordinary agent presentation, machine-envelope and
transported-fence evidence remains valid where code is unchanged. O01 notice
and O03 authored-class positive receipts become explicitly superseded, not
deleted or claimed as deletion credit. Current key-free controls replace
general execution coverage, while retired forms become negative admission
tests. Preserve all original REDs and incomplete invocations.

Hosted selection expected by reviewer-a: CI-Tier lifecycle; Local-Tier focused.
Path-to-family rationale: contracts/admission+engine and activity/journal/
selected-fork/recovery -> lifecycle's canonical contract/runtime/store units;
tools/effects/failures and LLM/MCP feedback -> their lifecycle-owned tool/core
and api-llm-bus consumers; provider connector/mock and CLI verify/describe ->
the catalog/verify consumers already selected there. Validate actual planner
root membership (especially new R01/R04/R08, LLM and current connector outputs)
at PLAN/FINAL. Add a specific missing disjoint unit only if demonstrated; no
automatic full and no invalid duplicate ownership. No workflow/planner edits.

PLAN-only canonical binding completed on the unchanged dd39 runtime tree:
swarm-test-timing -plan-ci, lifecycle PR body with no CI-Units, actual package
inventory and BindExecution. Exit0;77 units; digest
27296e3977fd7b9d6e6250601af19a9dbbdf7189ed60093a7e10081972a8938c.
Plan receipt: ~/.local/state/agent-a-2700/effect-retirement-lifecycle-plan-dd39.json
(SHA2567296ae83f1e438b2c53d99338eb1eb2d8e5bc3b5455c6305c95c1ba60fa0c0b8).
This validates selection, not execution or new unimplemented roots.

| Existing consumer package | Bound lifecycle family |
| --- | --- |
| contracts | contracts-first + contracts-rest |
| engine and runforkexecution | generic broad families; runforkexecution broad-01 |
| pipeline activity/lineage/journal controls | pipeline-rest (activation/timer partitions remain separately owned) |
| tools/effects/authoringview | broad-07 |
| failures | broad-04 |
| providerconnectors | broad-09 |
| MCP and native activityjournal | broad-10 |
| registration | broad-11 |
| LLM conversation/transported continuation | api-llm-bus-full |
| actual CLI verify/describe/O04 | catalog-required-verify |

Current retained acknowledgment, class/store, terminal timestamp, fork activity,
O04 admission and public event-construction guard roots each have one bound
owner. Recheck new key-free replacement roots and any new recovery root at
FINAL rather than borrowing this old-head membership receipt.

Affected construction/schema/source/public-API/retirement/census guards,
generation/diff, complexity, exact-head unused, debt ratchet20m and clean-head
preflight precede the eventual push. No duplicate local lifecycle/full/server2.
The upload-dependent preflight remains blocked by the explicit no-push hold;
do not claim the old receipt for this expanded tree. FINAL HEAD follows actual
B integration and #2592 priority, then the approved hosted selection.

Requested disposition: gate this finite same-PR handler/admission/recovery
cutover, then batch local implementation through a connected RED-to-GREEN.
No per-helper review or separate redesign is requested.
