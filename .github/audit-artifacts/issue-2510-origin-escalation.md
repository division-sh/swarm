# #2510 Implementation Boundary Escalation

Agent-g, 2026-10-01. **Production implementation frozen pending reviewer-g's
bounded disposition.** This is not a post-implementation closure audit.

**Request to reviewer-g:** independently classify the new exact-origin gap and
record split or bounded absorption on #2510 before G changes runtime admission.
Additive proof/reproducer commit: `5dd4a8a4e7da719e3235e5d43027e99367c63ddf`.
Existing watchlist refinement is published on swarm-docs master `d2bf6bd`;
all four watchlist YAMLs and diff checks pass. No runtime repair is authorized
by this request or by its own test failure.

## Approved Work And Actual Progress

The independent first-slice gate is
https://github.com/division-sh/swarm/issues/2510#issuecomment-5933895918.
The provider-sibling classification was corrected before extraction in
`1d58069d3`. Caller characterization was committed at `8e2fe986b`, before
production extraction `bc8516dc7`. The two private orchestration entrances
fall from cyclo68/60 to8/6; the exact pinned extraction ratchet passes, with
every extracted callable below25. G1/G2 corrections are included through the
existing spec and usage owners. No admission, retry, settlement, store, schema,
compatibility or vendor repair has been made.
Exact committed-snapshot ratchet from merged `e110bfb36` to proof `5dd4a8a4e`
also passes; generated test-file classifications preserve the unchanged
analyzers and production population. This is not final qualification.

Focused post-extraction Claude/tool race controls pass. Real selected-store
Executor proof passes SQLite/PostgreSQL keyed and keyless reads, pagination,
old run pins after global-head advancement and adapter reopening, and corrupt,
pruned and mismatched-pin refusal. These are not full process/provider backing
retention proof. Q02 genuine Claude/Telegram is NOT RUN and receives no credit.

The compiled Q01 journey now uses a verified authored source, public data/run
admission, ordinary live-posture serve, real turn-token HTTP MCP discovery and
resource row/page/emit calls, selected-store settlement, graceful shutdown and
hash-selected restart. Its first normal agent turn and both deployment-feed
rows settle successfully. Every pre-existing public event/delivery fact is
unchanged after restart; additional lifecycle observations are lawful. The
fixture deliberately keeps entities nonterminal while testing a later turn.

**Q01 is still FAILED:** the chosen public second-turn command,
`swarm agent directive ... --run-id ... --flow-instance worker`, refuses
before another provider invocation. Therefore neither the resumed-turn
checkpoint nor the complete 52-row class is claimed closed. No full suite,
hosted CI or review-ready PR is claimed.

## New Counterexample And Attribution

Candidate and untouched `origin/master@e110bfb368369d701d3e7784f418addf02d2f4ec`
both reproduce on SQLite and PostgreSQL:

| Surface / phase | Actual execution evidence |
| --- | --- |
| Compiled verify/serve and initial normal delivery | Source admitted, actual MCP row/page returns pinned resource-version identity, emitted worker/agent.completed has one successful node delivery, original agent delivery is delivered |
| Deployment feed | Both worker/records.loaded rows are delivered, without dead letters |
| Same-store public hash restart | All pre-existing event/delivery records retained byte-for-byte; new lifecycle diagnostics are allowed |
| Memory-enabled directive | Public exit6 / AGENT_DIRECTIVE_EXECUTION_FAILED, detail directive_board_step_failed; public runtime log action mark_delivery_in_progress_failed on platform.agent_directive |
| Provider dispatch | No second claude_live subprocess record; refusal precedes invocation, not a provider/tool inventory failure |
| Untouched master control | Same public failure on both stores with only additive test/harness files copied into a disposable worktree; none of the extraction, usage, spec or store changes is present |

Reproducer command, with the canonical host test PostgreSQL DSN:

```sh
go test ./internal/releasee2e \
  -run '^TestClaudeResourceReadSupportedServeRestart$' -count=1 -timeout=5m -v
```

The initial same command on untouched master failed in22.120s, with both
backend leaves executed (3.46s each). A second master control removes every
Swarm import from the process test and retains only the baseline description
literal in its black-box tools/list oracle; production bytes remain untouched.
That control fails in10.860s (SQLite3.15s/PostgreSQL3.24s), while its unchanged
public-process import-boundary guard passes. To replay this control from the
candidate test on master, copy only the three additive releasee2e test/harness
files and replace `releaseResourceReadUsage` with master's existing hint:
`Read only declared deploy-time reference files from your owning flow data root. Provide one filename from the delivered enum; do not use host paths or this tool for mutable artifacts.`
This expectation change matches the actual pre-G2 description only; it changes
no provider selection, session, delivery, directive, store or admission code.
Candidate public failures also execute both leaves. The final candidate probe
fails in14.238s (SQLite4.33s/PostgreSQL3.82s).
The disposable worktree is `/tmp/agent-g-2510-master-directive-proof`.
Local detailed evidence is retained in
`/tmp/agent-g-2510-master-directive-proof.log` and
`/tmp/agent-g-2510-public-resource.log`,
`/tmp/agent-g-2510-master-directive-blackbox.log` and
`/tmp/agent-g-2510-public-resource-finalprobe.log`. No genuine provider calls or messages
were sent; all credentials and provider responses are test-owned doubles.

The existing selected-store origin controls execute successfully on both stores:
`TestProviderAttemptDrainRejectsMissingOrForeignOriginClaimParity` and
`TestProviderDirectiveOrigin{CurrentSuccess,RejectsMissingAmbiguousAndForeign,
Supersession,PrelaunchAbandonment,Recovery}Parity`, package12.962s. This proves
the current durable origin authority; it does not fix or close the public
session-binding failure. `TestReleaseE2EPackageStaysAtPublicProcessBoundary`
passes for the candidate test. No relaxed guard, substituted internal runtime,
or provider dispatch after the refused directive is credited.

## Binding Contract And Missed Consumer

The authoritative `managed_external_effect_authority` sections
`context_authority.managed_agent` and
`invariant_ids.runtime.provider_attempt.origin_exact` require exactly one normal origin:
an exact current agent-delivery claim OR an executing directive-operation
origin. A directive must not have a fabricated delivery. `CompletionOrigin`
and normal `beginCompletion` already implement this closed distinction.

The earlier C consumer census tabulated Conversation managed and fork-chat
entrances but did not separately prove Manager's BoardStep/directive entrance.
The public directive calls into the same continuation, so this is not a second
Claude adapter. It exposes a **shared session-to-work admission interpretation**:

- `llm/session_events.go:markInboundDeliveryActiveForSession` sends every
  memory-enabled session to `EventPublisher.MarkDeliveryInProgress`, without
  distinguishing a directive origin. Stateless sessions skip this call.
- `bus/eventbus.go:MarkDeliveryInProgress` correctly requires an exact agent
  delivery claim. With a directive origin, it returns
  `agent session binding requires the exact current delivery claim`.
- `publishAgentStarted` also calls the shared helper and logs a failure before
  continuation; `requireInboundDeliveryActiveForSession` then propagates the
  failure, preventing provider completion authorization and launch.
- `manager/runtime.go:SendDirective` owns directive execution/heartbeat and
  injects the existing typed directive origin before BoardStep. Do not replace
  it with event coincidence, payload tags or a synthetic delivery.

Repo-wide production caller sweep: **all five** Claude CLI, Anthropic API,
OpenAI-compatible, OpenAI Responses and Mock continuation paths call
`requireInboundDeliveryActiveForSession`. Their session-start paths share
`publishAgentStarted`. Thus a Claude-only conditional would leave the same
interpreter live in sibling providers. Actual compiled failure is proven for
Claude; sibling effect is code-inferred and still requires execution proof.

### Added Entrance And Owner-Consumption Classification

The directive reproducer is an entry point, not the proposed repair boundary.
The relevant ordered path and gates are:

| Gate / path | Classification and owner | Evidence / reachability |
| --- | --- | --- |
| Compiled CLI -> authenticated agent.send_directive RPC -> exact run/agent/route resolution | Different authority entrance, already owned by API/Manager; not an authored event or synthetic delivery | Both-store public command reaches the stored directive operation; current agent is runnable after hash restart |
| Prepare persisted operation/idempotency -> lifecycle execution lease -> AdmitDirectiveExecution | Already consumes directive and lifecycle owners | Manager checks acknowledgment before proceeding; selected-store current/negative/supersession/recovery controls pass both stores |
| NewDirectiveExecutionOrigin -> WithDirectiveCompletionOrigin -> joined directive heartbeat -> BoardStep | Already carries the canonical typed directive authority | Existing Manager path, no missing-origin workaround; public failure is directive_board_step_failed, not resolution or preparation refusal |
| Managed Conversation -> memory session start/adoption -> publishAgentStarted / requireInboundDeliveryActiveForSession | Still bypasses the closed delivery/directive distinction and explicitly escalated here | All five provider families share the helper; first invocation succeeds through delivery, directive fails before provider dispatch |
| EventBus.MarkDeliveryInProgress -> exact claim validation -> BindAgentSession | Already consumes the canonical delivery owner; not a directive owner | Exact current agent claim is correctly required; normal public delivery passes and absent directive claim refuses |
| beginCompletion -> exact CompletionOrigin -> provider launch/settlement | Already consumes the canonical closed origin; not reached by the failed directive | Durable origin controls pass both stores; execution cannot be credited from helper/store controls alone |
| BoardStep failure -> joined heartbeat -> FinalizeDirectiveFailure -> public exit6 | Existing directive failure owner, outside the proposed session-binding change | Public operation state is failed with the exact typed code; no second provider invocation is observed |

The selected-contract and fork-chat entrances retain their separate execution
authorities; neither can be used to infer normal directive permission. Their
existing focused controls pass, but no new directive/fork equivalence is
claimed. Noop has no provider dispatch/shared continuation binding call and
is not a sixth executable provider repair consumer. Resource/static/operator
read owners are unchanged and cannot authorize a directive session.

## Proposed Class And Gate Request

The original C/R maintenance class is not permission to change admission.
This newly demonstrated runtime class is **origin-aware session-to-work
binding for memory-enabled normal-provider turns**. Its immediate parent is
shared provider-turn lifecycle/authority composition; #2447 and #2250 remain
open. #1992 concerns future declared operator ingress and compensation, not
the existing agent.send_directive operation, and is not claimed to own this
defect.

Request a bounded lead disposition before further production work:

1. **Split:** retain the approved #2510 maintenance boundary, explicitly track
   the directive-origin defect separately, and ratify a normal event-delivery
   re-entry for the same-session Q01 checkpoint. It must still reach exact
   resumed head, tool calls, settlement and retained original facts; no weaker
   two-unrelated-runs substitute.
2. **Absorb:** explicitly approve the complete shared-origin repair in #2510,
   including all five adapter consumers and both session-start/continue phases,
   then update the spec/consumer/proof matrix before coding it.

If absorbed, consume the existing closed origin and its admission owners; no
second origin predicate, origin inference, weakening of EventBus's delivery
claim, synthetic delivery, blanket no-claim success, adapter-only exception,
new registry, retry, recovery or lifecycle framework is proposed. Mandatory
proof would include both stores, normal delivery versus executing directive,
memory-enabled/stateless, fresh/adopted turns, malformed/foreign/missing/dual
origin refusal, unchanged exact delivery binding and acknowledged-cleanup
behavior, directive terminal settlement and no redispatch after completion.
Provider siblings need named execution tests, not same-helper credit alone.

Tracker decision: amend #2510 and the existing watchlist now; request
reviewer-g's decision. No new issue or POTENTIAL_ISSUES entry is created before
that decision. No prior approval is self-revoked by a new approval claim, and
no runtime closure is asserted. The original two-container/three-family
maintenance tail remains; this runtime defect adds one bounded potential
repair stream pending disposition. Estimated1-2 engineering days plus proof
and review, medium scope confidence/low effort confidence. Its ROI is high:
one existing shared owner can prevent a supported directive from failing
across five providers without introducing any framework.
