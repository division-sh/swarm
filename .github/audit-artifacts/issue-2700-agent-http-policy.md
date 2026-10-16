# Issue 2700: Agent HTTP Policy And Presentation

## Superseding Retirement Amendment

USER6220888538 deletes authored effect_class. Reviewer-a6220928385 selects the
same #2700 landing and requires the focused PLAN in
`issue-2700-http-effect-retirement-plan.md` before handler cutover. The former
ignored-declaration INFO and notice-only helpers are MOOT; their tests/receipts
below remain historical, not retirement credit. Timeout/O04, plain agent text,
typed machine evidence and transported-uncertainty fencing remain required.
No production retirement implementation or permission to push is claimed.

## Earlier Accepted Slice (Historical Scope)

Part of #2700. Track: FULL. Local work only; no PR push before B #2698
lands. The post-B #2592 integration retains priority.

Base: `0b7f1188a63cf375f4da11d03d1699edfbf5faf1`.
USER rulings: E1 option 2 (6218884753) and verify promise 8(i)
(#2702/6217749380). Reviewer-a PLAN gate: 6219466832, including its
required O04 representability correction. This does not close N19, Y2,
activity-cap validation or the user's future effect_class redesign.

## Owner And Scope Map

| Path / owner | Bounded change |
| --- | --- |
| `contracts/tool_http_execution.go` | One shared timeout conversion/admission leaf rejects seconds exceeding the duration representation before multiplication. Typed admission and preparation use it; authored admission uses the same rule with its source coordinate. No clamp or smaller policy maximum. |
| `contracts/workflow_contract_tools_value.go` | Preserve the authored integer/presence/positive-value contract, including explicit zero refusal and the distinct omitted/default case. Attribute representability refusal to the exact timeout value. |
| `contracts/tool_schema_entry_value.go` | Detached read-only effect_class provenance, not inference from the effective class. No new declaration grammar. |
| `tools/effective_tool_verification.go` | Factor the existing scoped actor/permission projection without changing configured-tool validation. |
| `tools/agent_tool_effect_declarations.go` | Project notices through existing scoped declaration, execution-candidate, permission and MCP-declaration owners. No live provider lookup. Deduplicate by declaration name plus original authored file/line/column, not by public tool name alone or agent count. |
| `authoringview/view.go`, existing CLI renderer | Append deterministic informational diagnostics. Preserve all existing report diagnostics, evidence and remediation. No new bundle invalidity or per-call warning. |
| `failures/agent_tool_presentation.go` | Validate the canonical envelope and derive a detached agent-only presentation. Uncertain results keep class/detail/retryability/determinism/component/operation but omit operator remediation and do not claim the canonical envelope schema. Non-uncertain values remain canonical controls. |
| `llm/conversation.go` | Use that presentation only on ordinary tool feedback. Preserve the transported-uncertainty early return before continuation consumption or a new model turn. |
| `mcp/gateway.go` | Render the ruled agent-facing content text. Preserve the independent typed runtimeError carrier for canonical mock/CLI decoding, logs and operator evidence. |
| `platform-spec.yaml` | Record the ruled notice/presentation scope and shared duration-representability rule. No broader grammar, policy or recovery redesign. |

The original HTTP client copy, request budget start, transport, jar, redirect
policy, cancellation, effect registration, launch, settlement and recovery
owners are unchanged. Agent HTTP remains write_or_unknown for every authored
class. Handler-activity admission remains a separate owner; an authored
read_only class does not grant a new agent retry path.

The new notice projection is not execution authority. Required MCP tool
declarations can be explained without live discovery, but this does not
manufacture a discovered binding or bypass execution admission. Module tools
cannot gain agent exposure through the notice.

## Retained Original Evidence

All logs are under `~/.local/state/agent-a-2700/`.

| Receipt | Exact observed outcome |
| --- | --- |
| `e1-duration-original-red.log` | RED: typed and authored 9223372037 / 9223372036854775807 seconds were admitted. Typed Prepare produced a negative wrapped duration / -1s. Omitted, 10s, largest representable whole seconds and the existing invalid-input controls passed separately. SHA256 `17cf3a53b4f7d5efaef7d7aa390fb0218a8912fba4d91287244b0989b9ea4033`. |
| `e1-presentation-original-red.log` | Test fixture compile failure due to a nonexistent test constant; no executed-test RED or qualification credit. The typo was corrected without changing production. SHA256 `532eb00ac57071d15ddd72209352be105a5bcb32158f10af22f533d61a27e6c5`. |
| `e1-presentation-notice-baseline-red.log` | RED: complete ordinary conversation feedback includes operator reconciliation remediation; MCP text lacks the ruled plain wording; scoped authored-tool describe has zero notices. Ordinary MCP failure control passes. SHA256 `c268a209e94cefbba8980d6b7401731fd1702bb192804aa15248f0add831b1cf`. |

The baseline presentation and notice owners were unchanged from the pinned
base. The duration repair was already present during that separate
presentation/notice run; neither result is presented as a whole-baseline tier.

## Execution Proofs

| Cell | Active execution and exit evidence |
| --- | --- |
| O01 | `TestDescribeAgentToolEffectNoticeUsesScopedAuthoredDeclaration`: two referencing agents, two same-name declarations in distinct scopes, HTTP and legal non-HTTP MCP, omitted and activity-only controls, denied built-in control, stable repeated projection. `TestDescribeAgentToolEffectNoticeTextAndJSON`: actual CLI text and JSON, exactly one source-bound informational notice. |
| O02 | `TestConversationToolUncertaintyUsesAgentPresentation`: complete ordinary transport/read-response result text, retained identity/cause, exactly one execution and absence of reconciliation instructions. `TestGatewayToolUncertaintySeparatesAgentTextAndMachineEvidence`: actual MCP tools/call text plus strict machine decoding; non-uncertain control. `TestAgentToolFailurePresentationKeepsCanonicalEvidence` / `...RefusesInvalidEnvelope`: canonical class controls, unchanged bytes/cause, detached attributes and no presentation-as-envelope acceptance. |
| O03 | `TestAgentHTTPAuthoredClassesKeepWriteUnknownBothStores`: all four authored classes, both interruption stages, real native authorization/settlement on SQLite and PostgreSQL, one physical request, exact uncertain operation/attempt state, replay refusal and independent reopened-owner refusal. No inferred GET safety or new retry policy. |
| O04 | `TestToolHTTPTimeoutDurationAdmission`, `TestAuthoredToolHTTPTimeoutDurationAdmission`, `TestVerifyToolHTTPTimeoutDurationParity`: actual typed admission/Prepare and source load/CLI verify parity, largest whole seconds and +1, omitted default, explicit invalid zero/null/negative/wrong kind, source coordinates. Existing HTTP and custom-native-search deadline/body/caller/client-immutability/admission controls remain mandatory where named in receipts. |
| O05 | Existing `TestHTTPToolAcknowledgedSettlementCleanupPreservesResponseBothStores`: real native acknowledged and unacknowledged settlement, post-commit diagnostic kept, exact response/state/count and replay refusal. The new O03 native readback/reopened-owner proof is separate from harness-only controls. Transported mock uncertainty still cannot consume its continuation or create tool feedback. |

`e1-mechanics-candidate1.log`: GREEN across contracts, failures, LLM,
MCP, authoringview and actual CLI verify/describe. SHA256
`9125d6073151df23105ce7d9034c96cad33779b37f1aad24fe01936b8a66e6d4`.

`e1-native-class-settlement-candidate1.log`: GREEN, tools package 5.964s.
All sixteen class/store/interruption cells pass, with real native attempt
classification and reopened-owner replay refusal. The separate both-store
acknowledgment proof also passes. Its injected post-commit cleanup warnings
remain in the receipt. SHA256
`92deceb5194afb41aac14503f33154d7522a07ce59a5ed94e96a95e9ec052239`.
`e1-fast-controls-race3-candidate2.log`: GREEN under race x3 across all eight
focused packages, including ordinary conversation feedback, strict MCP
decoding, the transported uncertainty fence, scoped notices, actual CLI
verify/describe, body/cancellation/header budget, both shared-client paths,
native policy, permission and candidate controls. SHA256
`608ee54d4c50f6de499a8ef1baceef0b83db928d03d84a093dbde2ebf0d39efa`.

`e1-native-strengthened-and-guards-race3.log`: GREEN under race x3. All 48
native class/store/interruption executions pass, now also asserting unchanged
physical-attempt count through the reopened owner. All six acknowledged /
unacknowledged store controls pass; the injected cleanup warnings stay visible.
Maximum-representable ordinary HTTP and custom-search Execute deadlines pass;
the custom policy refuses max+1 before any attempt/dispatch. Authored
presence/closed-field/source controls and the focused HITL, actor-catalog and
completion-consumer guards pass. Package times: contracts 2.737s, tools
95.480s, MCP 1.037s, LLM 2.188s, CLI 7.685s. SHA256
`36d20177a67efc758a0b9282a591bd238cbfe42bfb67259b92dcb5e671ef8572`.

Exact-head static/unused and final guard receipts are recorded with the
consolidated RED-to-GREEN checkpoint. No hosted qualification or preflight
pass is claimed. The staging/PR push is held by the explicit B landing
boundary; the real preflight verifies that its candidate was uploaded, so it
must wait for the permitted staging push. Final preflight must run on the
actual integrated clean head before a PR push.

## Selection And Integration

PLAN provisionally selected core plus contracts-rest if needed, local focused.
Current-source coverage inspection finds a qualification composition gap:
core runs the generic tools/effects/failures/authoringview/MCP packages and
the CLI catalog-required-verify unit, but not the special LLM package.
contracts-rest selects Test[M-Z], omitting the Authored admission controls
in contracts-first. The only current whole-LLM unit, api-llm-bus-full, also
selects the unfiltered bus package that core already owns through
local-runtime-bus-full. Ask the qualification owner for a valid existing
selection at RED-to-GREEN/FINAL; do not omit required roots, move test names,
modify the planner or silently fall back to full. Refresh the actual plan
after B and the pinned preceding integration land. No duplicate local tier
or server2 qualification.

No new SQL, table/migration, durable identifier, authority hook, recovery
policy, field requirement, effect-class change, timeout allowance, debt or
complexity baseline change. Original #2708/#2712 slow and mutation receipts
retain only their exact unchanged execution-path credit. The #1745
amendment's authoringview/spec hunks remain on its own local branch and must
be composed explicitly; its coordinate-dependent state adapter is paused
for the separate USER/LEAD decision.
