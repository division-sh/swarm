# Bounded Runtime Defect Escalation: #2321 Live Acceptance

Agent: agent-g. Date: 2026-09-08. Implementation head: 605032bd2.
Independently reproduced on freshly fetched origin/master@05528a92e.

## Observation And Cause

Two actual public SQLite live journeys passed approval and produced the exact
telegram_send_message.succeeded event, then dead-lettered the original agent
delivery. The user confirmed receipt of lifecycle 1001. The expanded diagnostic
journey failed in 29.334s after a 13m37s queue wait. Its public conversation turn
pins the failure to claude_cli_capability_validation; the conversation's projected
turn_count remains zero. This is external-effect success followed by failed turn
acceptance, not successful lifecycle convergence. No old delivery was retried.

Root cause: cliStreamAccumulator correctly separates exact native provider names
from exact MCP names and the derived canonical combined VisibleTools list.
When ProviderVisibleTools is empty, exactCLIProviderVisibleTools falls back to
VisibleTools. Canonical MCP names have already lost the MCP prefix, so that reader
reinterprets them as native builtins. A valid MCP-only turn is rejected as an
unplanned native surface after its MCP tools have executed. Adding a native
capability masks the defect, but is not an acceptable repair.

Locations: internal/runtime/llm/cli_stream_parser.go:307, capability_surface.go:629,
capability_surface.go:419 and :479. The fallback dates to 7dff5f86b, not #2321.
The relevant adapter/parser/capability owner is unchanged on current master.

## Execution Evidence

- Real isolated Claude 2.1.87 census with --tools ExitPlanMode and no MCP server:
  init tools=[ExitPlanMode], successful result, no tool execution.
- Same real CLI with a non-executable two-tool MCP census server:
  init tools=[ExitPlanMode, mcp__runtime-tools__emit_telegram_reply_requested,
  mcp__runtime-tools__read_chat], runtime-tools connected, successful result,
  no tool execution. Neither census sends Telegram messages. Costs were
  USD 0.0263715 and USD 0.0267315 respectively.
- Adjacent TestCommandLiveMCPOnlyCapabilityProbe feeds that init shape through the
  actual parser, managed capability planner, observation and validator. It fails
  MCP-only acceptance 3/3 on this branch and 3/3 on untouched current master.
- Both trees pass the probe's mixed native+MCP positive control and its unexpected
  native/missing native negative controls on all three repetitions.
- Exact failing projection: provider=[], planned=[],
  canonical=[emit_telegram_reply_requested read_chat],
  interpreted=[emit_telegram_reply_requested read_chat], mismatches=true.
- Focused existing Claude managed-call/parser tests pass (0.024s); selected-store
  provider-head settlement tests pass on SQLite/PostgreSQL (2.660s). Those baselines
  do not negate the failing parser-to-validator probe or prove live PostgreSQL.

The failing probe is retained as .go.txt, not installed as a passing test. The
live journal is /tmp/agent-g-2321-live-sqlite-6.log; no credentials or full private
transcripts are included in this artifact.

## Approved Bounded Amendment

The independent gate at https://github.com/division-sh/swarm/issues/2321#issuecomment-5592428086
supersedes the pending-disposition statements below. Absorption into #2321 is
approved for **exact CLI capability observation channel and presence integrity**.
No additional gate or issue is required. #2432 remains separately frozen.

Implementation removes the display fallback and adds explicit valid, missing and
invalid inventory state to the existing parser/Response. Only provider init
metadata supplies inventory; assistant/result data cannot supply it. Invalid
entries invalidate the inventory rather than silently disappearing. Exact native
extraction is checked by managed observation/validation and fork-chat validation.
Startup and each fresh/resumed/tool-result process require their own observation.
Post-provider refusal still settles outcome_uncertain; it cannot retry effects.

| Added manifestation | Required proof (not yet claimed executed) |
| --- | --- |
| MCP-only, empty and controls-only | Parser -> managed/fork validation accepts exact empty native inventory. |
| Native-only, mixed and canonical collisions | Exact channel membership survives canonical display normalization. |
| Missing/null/non-array/malformed inventory | Managed/fork/startup refuse; invalid cannot be erased by later message metadata. |
| Missing/unexpected native, disconnected/unplanned MCP | Existing authority refuses or marks required evidence unavailable without granting tools. |
| Fresh/resumed/tool-result processes | Actual invocation tests prove no inventory inheritance and uncertain settlement on invalid post-provider response. |
| API/mock and local sandbox | Existing independent policy/transport tests remain green. |
| Live SQLite/PostgreSQL | Fresh authorized journeys prove accepted turn, causal reply, original delivery settlement, retained restart and no duplicate effects. Never replay already-sent delivery. |

Watchlist decision: consume lead refinement swarm-docs@038244f on the existing
transport_policy_and_surface_parity and effective_agent_execution_contract_identity
nodes. No new framework, permission, compatibility reader or architecture issue.
Intended closure is the complete bounded observation class; broader #2321 review
readiness still requires diagnostic repair and integrated full proof.

## Boundary And Consumer Census

Proposed class: exact_native_vs_mcp_capability_observation_separation.
Parent: transport_policy_and_surface_parity / effective_agent_execution_contract_identity.
This is a pre-existing capability interpretation defect exposed by #2321's live
acceptance, not command-owned execution selection and not #2432 diagnostic
projection convergence. No broad lifecycle or scheduler change is justified.

| Owner / consumer | Classification and required repair proof |
| --- | --- |
| cliStreamAccumulator -> Response | Canonical producer of separate exact provider/MCP lists and derived canonical display list; preserve empty-native truth. Test MCP-only, native-only, mixed, empty, controls and missing observation metadata. |
| observeCLIResponse / ValidateCLIProviderCapabilitySurface | Same class; normal managed provider completion and startup probes both consume this interpretation. Prove valid empty-native acceptance and genuinely missing/unexpected native refusal. |
| validateClaudeInvocationProviderBuiltins | Same reader in separate fork-chat sandbox authority. Prove MCP-only fork acceptance and forbidden builtin refusal without changing fork policy. |
| cli_runtime_startup_probe.go | Same managed observation/validation path, before normal provider execution. Keep startup authority/evidence separate from provider-turn evidence. |
| CLI conversation sandbox canonical summaries | Different channel: derived display/canonical names are legitimate summaries, not native capability authority. Sweep these readers so no substitution survives. |
| API and mock adapters | Different transport observations; preserve their exact API-definition/local-runtime admission and existing negative tests. No live-to-mock fallback. |
| Public live serve/dev, retained restart | Actual failing acceptance path. After the gated repair, rerun the unchanged SQLite/PostgreSQL L journey, then required broader proof. No connector-send-only closure. |

Production ProviderVisibleTools has one producer, cli_stream_parser.go. The reader
has three direct consumers: observeCLIResponse, ValidateCLIProviderCapabilitySurface,
and validateClaudeInvocationProviderBuiltins. Repair must audit synthetic responses
that populate only VisibleTools rather than retaining that fallback for tests.

## Governing Contract And Requested Disposition

Binding platform-spec.yaml sections:
engine.agent_session_management.native_tools.description,
engine.agent_session_management.native_tools.relationship_to_tool_filtering,
engine.agent_session_management.llm_provider_adapter_contract.native_tools.
Native capability defaults are all false; platform/MCP tools are a separate channel;
visible native tools must match exact callable native authority. The current
fallback violates that separation. Exact empty native observation is not authority
to infer native capabilities from canonical display names.

Recommended minimal direction: delete the cross-channel fallback, consume the exact
provider-native observation, and migrate synthetic test producers to the real
typed channel. Preserve missing/invalid observation and unauthorized-native refusal;
do not indiscriminately accept mismatches or expand the builtin/control allowlist.
If these requirements reveal a missing observation-presence contract, stop and
specify that bounded decision rather than inventing inference rules.

Tracker decision: record this new acceptance blocker on #2321 and request explicit
lead absorption/split disposition before production implementation. #2432 remains
separately gated/frozen; #2319 remains nonblocking. Do not reopen historical #2037
or silently absorb this into diagnostic projection. Relevant existing watchlist
nodes are runtime-operations.yaml#transport_policy_and_surface_parity and
#effective_agent_execution_contract_identity; refine their concrete issue mapping
with the lead's disposition, not a new general architecture node.

No production capability repair, final proof audit, full-suite success, live restart
success or merge readiness is claimed. Independent gate requested for this bounded
repair; no broader framework, compatibility path or extra native permission.
