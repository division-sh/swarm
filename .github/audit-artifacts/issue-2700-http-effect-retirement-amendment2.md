# #2700 Evening Ruling: Bounded PLAN Delta

Track: FULL, same connected landing. Reviewer-a: request the widened PLAN
delta under the patched USER6220888538 and orchestrator relay. Input is signed
34057d44e7 on agent-a/2700-e1-option2-local. No widened production edits yet,
no push before B, and actual post-B #2592 integration remains first.

## Scope Correction

The earlier PLAN's preservation of activity class/retry/fork-policy carriers
and activity_attempts.effect_class is SUPERSEDED. Its preservation of internal
managed-effect accounting and PLATFORM operation classes still stands. The
corrected census is20 shipped connector tool declarations (13 writes/7 platform
reads), four generator profile declarations and the user's own bundle edits;
the prior repository-flow-only count was not a pack census. User bundles stay
untouched. Current pack bodies/profiles are already key-free on this branch,
but their import/schema receipts are NOT proof of all7 platform read executions.

| Widened cut | Existing owners / connected consumers |
| --- | --- |
| Flow activity carrier deletion | contracts/workflow_contract_activity.go; engine/{types,executor}.go; pipeline/{activity_engine,activity_lineage,activity_attempt_record,author_activity,workflow_gate_decision}.go. Remove class/fork/retry policy from intent/request/result schemas, codecs and publications together, rather than serializing constants. Constant retry-attempt exposure also goes from authored carriers; native attempt1 CAS/identity remains internal, not a new retry knob. |
| Decision/authoring projections | decisioncard/proposed_effect.go; backend/decisionpersistence/proposed_effect_cards.go; pipeline/workflow_gate_decision.go; authoringview/view.go. Remove obsolete fields from continuation codecs/digests/cards/approval points as one current-contract cut. Preserve frozen input, reply, run/generation authority, decide/defer/input checks, exact settlement and approval consumer requirement. |
| Native storage and fork copy | backend/activityjournal/{journal,execution_binding,execution_grant,owner}.go; backend/runforkpersistence/run_fork_activity_generation.go plus result/continuation readers; platform-spec activity_attempts DDL. Drop ONLY activity_attempts.effect_class; align INSERT/SELECT/scan/copy/duplicate comparison and current revision projections. Keep execution_binding, exact native attempt/source/run/mode/loop fences, timestamps, atomic story/result publication and R04's original-role proof. Compose B's historical activity writer at actual B, not an invented fixture or rewritten packet. |
| Platform-owned pack calls | packs/channel.go::validateOperationBinding and OperationEffectClass, channel_generation.go; packs/channel_registration.go::compileRegistrationOperation/RegistrationOperationPlan; runtime/{registration/provider_http,tools/channel_runtime}.go; serveapp/channel_native.go. Derive/check channel class from its admitted platform interface and registration class from its exact slot (identify/readback read, apply write). No tool declaration, method heuristic or caller flag. Keep private-target/generation/credentials/schema/mapping fences and platform read execution; do not route those reads through a flow-only constant-class refusal. |
| Shared teaching/spec/generation | contracts/workflow_contract_tools_value.go, providerconnectors connector parsing/generation/profiles, pack loader, platform-spec tool/activity/connector sections and current tests. Flow teaching is exactly `effect_class is not a tool key; the runtime attempts each HTTP call once, and an interrupted call is outcome-uncertain`. Connector teaching is exactly `effect_class is not a tool key; a connector tool takes its class from the operation or registration slot it is bound to`. Preserve presence/null/alias/unknown-field/source-location refusals. Remove obsolete activity table/credential/approval/mock clauses and regenerate current outputs. |

## First REDs And Exit Proof

- A10: shared source + actual request/result/card/describe projections must omit
  all retired policy fields; generated DDL on BOTH stores must lack the activity
  class column. Current signed head still exposes these fields/column: expected
  RED. Check exact codecs, digests, native terminal readback, story rollback,
  copied terminal evidence and current source/history successors together.
- A11: both distinct teaching messages, all old values/null/empty/merged forms
  and source coordinates. Earlier single-message GREEN becomes superseded
  evidence, not acceptance under the amended wording.
- A12: execute each of Telegram's7 platform reads under its REAL interface/slot,
  plus registration apply/write and all existing private native target controls.
  Verify bound class, result, credentials and physical-call/managed accounting;
  wrong slot/binding/category/mapping refuses. A flow importing the same connector
  still gets once-only uncertainty, not platform read policy. No call path is
  permitted solely by method or a tool class that was removed.
- A13: real GET/POST authored approval, static/managed credentials and mock
  activity paths through the ordinary journal on both stores. Keep missing
  revision consumer, invalid decision/input, credential conflict, unrelated
  managed-provider eligibility, response/schema and generation refusals.
- A14: exact settled result reuse and unresolved-source refusal after field
  removal, selected generation/claim fences and copied-terminal non-permission.
  The separately identified unapproved-activity fork defect stays cross-pinned
  to the #642 owner; this PLAN does not silently repair it or waive it.
- R03/R04/R05/R08/R09 remain mandatory: one physical flow HTTP call, original
  cause and uncertain result, no redispatch, terminal replay, rollback/ACK loss,
  genuine takeover/late predecessor refusal, independent call budgets/O04 and
  plain agent text. No automatic read retry, new scheduler or compatibility path.

Ownership/recovery execution stays both stores/race x3 at existing business
bounds. Frozen packets, old REDs and historical assertions remain append-only;
current contract successors are explicit, not retroactive history normalization.

Hosted remains provisionally lifecycle/local focused per6220601595; refresh
actual root-to-family binding for the widened diff before FINAL, add only a
demonstrably omitted disjoint family, and ask the qualification owner if a plan
cannot compose. No blanket full, duplicate local tier or server2 campaign.
Affected guards/census, regeneration/diff, zero-addition debt20m, unused,
complexity and actual-head preflight remain owed before the permitted push.

Request: reviewer-a gate this bounded widened delta. Existing non-enabling R04
stub/native-fence staging is separate; runtime capture and E's real hook remain
open. Do not count the old single-message or constant-field receipts as closing
this amendment.
