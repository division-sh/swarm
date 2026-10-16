# #2700 Local HTTP Retirement Progress

Intermediate engineering snapshot only, not consolidated RED-to-GREEN or
FINAL HEAD. Governing gates6221203713/6223417528 and the amended USER
ruling6220888538. No push before
B #2698; actual merged #2592 integration still takes priority. Hosted selection
remains provisional lifecycle / local focused, no hosted run or local tier.

## Implemented Independent Cut

- Shared tool admission teaches on any presence of effect_class, including
  null, empty/non-string values and merged declarations. WithToolEffect,
  WithEffect, Effect, effect storage/serialization and effect provenance are
  removed. Current constructor callers no longer manufacture that selector.
- Moot ignored-declaration notice producer/test are removed. Actual CLI text
  and JSON now refuse the old declaration through shared source admission.
- Fresh handler/rule HTTP planning uses the existing conservative journal
  class, attempt1/no-backoff and confirmation policy for GET AND POST. Rate,
  handler, credential, approval, source, input and generation owners remain.
- Dispatcher uses the existing durable claim/settlement path, not read-only
  execution/retry. Exact native receipt reuse precedes fresh-only policy.
  Unsupported historical fresh execution is conservatively unavailable, never
  reexecute_read. Historical/fork controls still need the connected refresh.
- Prepared HTTP uses only a per-call client copy with competing Timeout zero;
  transport, jar and redirect policy stay shared unchanged. Authored budget
  still starts at the same boundary. Prelaunch cancellation differs from
  postlaunch uncertainty; original causes survive redaction. Complete known
  responses remain distinct.
- Current connector profiles/output and stock HTTP connector bodies are
  key-free; native channel classifications remain on their existing operation
  owner. Native registration role/category/schema/credential owners remain.
  Generated GitHub/Acme output was regenerated with swarm-openapi-gen; current
  conformance profile/manifest digests were refreshed without changing any
  expected request, response, assertion or source digest. Frozen packets and
  user jobflow/twitter-gen files are untouched.
- The earlier constant-carrier/result-column preservation is SUPERSEDED by
  amendment2 and gate6223417528. Authored activity policy carriers and the
  activity_attempts class column must be deleted together; that connected
  deletion is still open. Internal managed-effect accounting and exact
  platform-operation policy remain, not caller-authored policy.

## Amended Platform Slot And Teaching Cut

The two exact USER messages now use the same closed tool admission owner:
flow declarations teach fixed once-only behavior; connector declarations teach
that the admitted operation/registration slot supplies platform policy. The
connector entry point changes only this teaching context, not the grammar or
admission rules. Null, non-string, empty and merged old declarations still
refuse with their introduction coordinates.

Registration compilation now retains its exact identify/readback/apply policy,
checks that policy against the slot, and includes it in the existing compiled
generation evidence. Execution receives the opaque compiled slot, not a raw
tool plus a caller-selected policy. Native-inbox reads receive the existing
compiled outbound binding and exact operation. Category remains an eligibility
check, not authority. Existing native-profile read-policy checks are unchanged.
Public ingress and native-inbox callers forward those same compiled plans.

The new seven-read component root executes the real shipped connector request
and response mappings through these platform entrances, including all five
native reads. It checks credentials, exact method/URL/body, semantic output,
one physical call and zero managed-write attempts. This is component execution,
not served restart, all-platform-path closure or R04 qualification. Wrong/zero
slots, a native write passed to read, missing credentials, and compiler
category/mapping/credential refusals remain explicit controls. The existing
apply, preflight, acknowledgment-loss and post-commit controls remain mandatory.

| Amended receipt under ~/.local/state/agent-a-2700/ | Actual result / boundary |
| --- | --- |
| amendment2-shared-teaching-and-slot-original-red.log | Original exact-message controls RED; raw registration read also physically issued one POST for the apply slot instead of refusing. Original receipt retained unchanged. |
| amendment2-shared-teaching-and-slot-candidate1.log | PASS contracts0.010s/providerconnectors0.010s/registration0.407s, count1. Publicingress and serveapp compiled only, no selected tests and no execution credit. |
| amendment2-platform-reads-and-slot-race3-candidate1.log | PASS race x3: packs3.419s/contracts1.080s/providerconnectors1.045s/registration12.847s. All7 real connector reads execute in each iteration (21/21), with exact request/result and no managed-write attempt. All slot/category/credential/mapping negatives and existing apply/preflight/ACK-loss/post-commit controls pass. |
| amendment2-platform-served-consumers-race3-candidate1.log | PASS race x3: publicingress8.182s/serveapp187.339s/cliapp2.697s. Native-inbox startup, served restart and readback-failure readiness refusal each execute both stores x3 (18/18); all selected provider-registration lifecycle, rotation/currentness, prelaunch-marker and actual text/JSON teaching controls pass. Original business bounds unchanged, normal host load. |
| amendment2-imported-platform-read-policy-race3-candidate1.log | PASS tools1.729s, race x3. The actual shipped identify and command-read tools execute through ordinary authored-tool admission: interrupted transport and complete503 remain distinct typed outcomes; each uses one write_or_unknown managed attempt, one physical call and one settlement, and exact logical replay cannot redispatch. Existing managed HTTP/native-search outcomes also pass. Component fake-store accounting, not R04 native takeover credit. |

The platform specification now states the same exact operation/slot policy and
both shared-admission teaching messages, without changing the operation effect
table or its native read-policy checks. No deadline, retry allowance or authored
classification option was added. Remaining activity policy/schema/storage
deletion and R04 assembly are not closed by these platform receipts.

Raw amended receipt SHA256 pins:
original9be676985d24eb67f3210d89fd85ce3c6341d5f6581f773b279c38af4a373533;
first-GREEN7157daf9759dae5b0442b56ab2c668e03cd1cb550b97643689b89240d024ebd6;
platform-race3 44fa85aba552e1eb13616fa45881b8f40b2337e5b8ae6bacb7c0566e40c966de;
served-race3 49f876cc7a05e05d937bbbf899b6cbb15d45ac46cc546da198554789789a1c32;
authored-import-race3 2c623fff90091a2325ed251a0d14c0ac6c861192bdc533995779c627e815ac2c.

## Exact Focused Receipts

All raw logs below remain under ~/.local/state/agent-a-2700/. Race x3 uses
GOMAXPROCS2, -p1, original package/business bounds and the normal runner. Early
candidate receipts are component evidence, not substituted for final-head
affected checks or the unresolved R04 integration.

| Receipt | Actual result / boundary |
| --- | --- |
| effect-retirement-admission-original-red.log | Original old-key acceptance and missing fixed-policy behavior RED; retained. |
| effect-retirement-handler-http-original-red.log | Competing client cap, incorrect prelaunch call, lost cause and timeout classification RED; retained. |
| effect-retirement-keyfree-native-original-red.log | Both stores, all20 GET/POST outcome cells RED: zero provider calls, missing native attempt, one policy-refusal publication; 5.509s. |
| effect-retirement-handler-overlap-diagnostics-race3-candidate2.log | PASS pipeline1.094s / authoringview3.024s; shared-client overlapping budgets/cancellation, response/refusal/redaction and ordinary diagnostic controls. |
| effect-retirement-keyfree-native-race3-candidate1.log | PASS60/60 native executions; 170.745s under180s. GET/POST x success/complete403/transport/body/deadline x both stores x3. One call/one attempt; exact journal result/time survives reopen and repeated replay. Before the later declaration-API removal, not a final-head rerun. |
| effect-retirement-fixed-planning-validation-race3-candidate1.log | PASS engine2.527s / runtime1.178s; GET/POST handler/rule fixed policy, unchanged persistence/selection/refusal controls, key-free validation and MCP refusal. |
| effect-retirement-api-consumers-admission-candidate1.log | Build-only RED at removed module field; no test execution. Fixed at its remaining owner. |
| effect-retirement-api-consumers-admission-candidate2.log | PASS31 teaching cells +2 key-free canonical/YAML/hash round trips +3 connector old-key refusals. Twelve affected consumer packages compile; ten had NO selected test and get compile-only credit. |
| effect-retirement-connectors-ack-race3-candidate1.log | Overall RED only on generated conformance freshness after intentional profile/body changes. Five stock connector imports + closed generated execution controls pass x3. Native post-commit error/result reuse12/12 pass, pipeline27.224s. Incorrect pack-root name selected NO pack tests; no pack execution credit. |
| effect-retirement-generated-catalog-check-candidate2.log | PASS generated catalog check, packs2, after exact current profile/manifest digest refresh. |
| effect-retirement-connectors-native-roles-race3-candidate2.log | PASS providerconnectors19.460s / packs3.459s. Generated deterministic/closed output, provider-neutral native inbox operations, all7 native capability controls and differential registration compiler x3. No native acknowledgment rerun. |
| effect-retirement-activity-reference-race3-candidate1.log | PASS1.009s: exact versioned grant/state reference, canonical UUID/positive signed range, no missing/invalid attribution fallback. DTO only, not authority or R04 qualification. |
| effect-retirement-keyfree-agent-race3-candidate1.log | PASS24/24 executions, tools43.544s: GET/POST x transport/read_response x both stores x3. Existing native write_unknown accounting, one physical call, exact uncertainty result, reopen and replay assertions retained. |
| effect-retirement-lineage-oracle-original-red.log | RED0.009s: two current-source lineage fixtures still authored the retired selector, so shared admission refused before their assertions. |
| effect-retirement-lineage-oracle-race3-candidate1.log | PASS pipeline3.214s: both existing lineage roots x3. Key-free GET planning uses the fixed policy; source/run/parent/entity/flow/instance/handler/activity/tool/result/retry/fork-policy and generation negatives, plus exact/nonterminal parent controls, remain active. No historical packet changed. |
| effect-retirement-validation-oracles-original-red.log | RED runtime0.062s: four stale expectations retained from authored policy selection. Approval/GET-static-credential cases now legitimately admit; old idempotency declaration must refuse at shared source admission; unsupported connectors still refuse missing credential/response-success policy, not a deleted class. |
| effect-retirement-validation-oracles-race3-candidate1.log | PASS runtime3.995s, race x3. GET/POST approval positives and missing-consumer/noncanonical-decision negatives; all4 retired declarations refuse with exact teaching/source location. Connector credential and response-success negatives, rate/managed-credential refusal, input/MCP/result collision and ordinary connector positives remain active. |
| effect-retirement-registry-tail-controls-race3-candidate1.log | PASS runtime1.052s, race x3. The invalid connector now asserts BOTH actual credential/response-policy refusals; every original registry observation, dependency, no-execution timestamp, selected-source scope and complete-tail assertion remains unchanged. |

Earlier declaration-positive/notice receipts are superseded, not silently
counted for retirement. Agent credit above does not qualify handler takeover
or R04 recovery.

## Still Open

R04 now has the nullable native execution_binding column and strict codec,
exact pre-mutation identity checks and explicit non-executing fork-copy NULL.
The original-role process/grant/selected fences are now called by each native
owner writer, but runtime installation/per-dispatch capture and E's fixed-session
recovery integration remain unimplemented. Joint gate6221888821/6221925281 is approved;
the exact port and historical/non-executing writer map are in
issue-2700-r04-activity-port.md. The native staging boundary and all original
REDs are accounted in issue-2700-r04-native-progress.md. No recovery sweep has
been enabled, no reference-only claim supplies permission, and the original
component reopen RED remains visible. Original writers refuse unbound roles;
the currently uninstalled runtime journal must be connected before native HTTP
execution can be qualified again. There is no fresh-NULL permission or completed
R04 repair. Earlier native HTTP receipts remain historical component evidence,
not exact-head credit across this pending assembly.

Validation-oracle raw SHA256 pins:
original d7b1e5228ca60d1deaa9e4f2605a41563ac8ba6d8fdfb4ec4808e1f3f8f8e339;
race3 821902dc92b1a4632c22056d9749ed0b81d9ef3ba7c7c2072d46949a3da7efe5;
registry-tail23521bdff5b34ee9b3860ce57aa68ccfba79c737521aa75e033752a329d54338.

Finish remaining current fixture/old-class oracle migrations through the new
shared refusal or key-free execution contract; preserve their other negative
cases, exact historical/fork evidence, source/input/result/generation fences
and original bounds. Refresh exact native/selected result controls after the
full integration, including both historical copy writers introduced by B.

Fresh affected guards/censuses/debt20m, unused, complexity, root-to-family
mapping and exact-head preflight are still owed before the eventual permitted
push. No old preflight or broad/incomplete run is current-head credit. The
upload-dependent preflight remains explicitly blocked by the no-push hold.
