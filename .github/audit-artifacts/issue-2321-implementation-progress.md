# Issue #2321 Implementation Progress

Historical checkpoint. Current additive status and remaining acceptance work are
recorded in `issue-2321-activation-progress.md`; the diagnostic class is now #2432,
not an unclassified request. No historical failure below is erased or credited as
a final-head pass.

Agent: agent-g. Status: verification exposed an additional lifecycle diagnostic
projection defect; awaiting lead classification, not review-ready.
This is not a Post-Implementation Proof Audit or a first-slice
closure claim. The approved 29-manifestation class remains binding.

## Authority And Integration

- Independent gate cycle 2: https://github.com/division-sh/swarm/issues/2321#issuecomment-5584592517.
- Implementation base: origin/master@bdc82dfba, preserving its run-scoped identities,
  receiver/lifecycle contracts, terminal retirement and fresh activity lineage.
- Approved audit and 13-target spec delta remain historical design artifacts;
  authoritative rules are promoted into platform-spec.yaml alongside implementation.
- Existing docs watchlist refinement efac8cd is consumed. No new issue or architecture
  framework. #2319 remains F-owned and open; #1801/#2284 remain separate.
- Original Agent G worktree's unrelated #2008 WIP is untouched.

## Implemented

- Structural readers share admitted effective source and the existing validation
  report. No credential/MCP/machine observations; declared model/tool/workspace
  constraints, supplied doubles, severity and harness independence remain checked.
  Output separates structural validity from unobserved live readiness.
- Commands select Live (serve/dev) or MockOnly (test). Authored doubles do not select
  a backend or waive live credentials/native/Claude/workspace requirements.
- ResolveAgentExecution owns write-time executable descriptor projection. Live
  descriptors omit executable doubles; mock descriptors retain exact admitted
  performance and live-model metadata. ValidateAgentExecutionDescriptor consumes
  complete persisted truth unchanged at dispatch, adoption, recovery and selected
  fork. Removed unused backend-migration helper; no read-side reselection.
- Manager fresh/blueprint/reconfigure paths use the selector; persisted paths
  validate. Selected-fork blueprint creation now carries configured model aliases
  before sealing, rather than depending on later read-side model resolution.
- Native boot checks use the same command-selected descriptor and real runtime
  capability owner. Same-source mock/live negative proof preserves native refusals.
- Retired runtime.execution_posture and mock backend selectors reject with originating
  file/key, including masked config layers. Recovery defaults true; explicit false
  survives merging. Removed all five shipped deployment configurations and stale
  graduation teaching. Artifact census rejects newly introduced deployment files.
- TestSessionRunner is injected into the CLI; RunTestSession uses the same
  buildRuntimeComposition as Run. Admission of source, scenarios, variables, setup,
  effective identities and exact mock completeness precedes acquisition.
- Public test owns fresh temporary SQLite, workspace/data roots, ephemeral loopback
  listeners and generated memory auth. No ambient API/context/store/credential
  selection, provider ingress, public exposure or registration. Existing process,
  selected-store, source, workspace and supervisor owners perform joined teardown.
- Private-root cleanup runs after joined shutdown and removes immutable projections
  without following symlinks out of the owned root. Cleanup errors remain failures.
- Internal retained mock proof uses TestOwnedMockLifecycleProcessEntry in a go test -c
  binary, not a production flag. Parent retains isolated SQLite/PostgreSQL and signs
  real local ingress across child restarts. Internal scenario protocol fixtures use
  explicit injected parent-owned sessions, never a connected-test compatibility path.
- Golden possession/invocation-root tests retain real public live serve, now using the
  agent-free canonical RootIngress fixture because these tests prove process ownership,
  not agent execution. Retained agent workload tests stay explicitly internal (H).

## Executed Evidence

These are development results, not a final integrated-head closure matrix.

- Public compiled verify/describe/graph/routes passed on unchanged webhook scaffold
  without external executables or readable credential documents; explicit structural
  output and invalid-source/no-probe tests pass.
- Actual compiled public bare test and verify passed on both unchanged scaffolds
  without live credentials/Claude/Docker.
- Full CLI package passed (41.906s); full catalog package passed (368.598s).
- Full manager, bootverify, llm and selector packages passed at the earlier command
  selection checkpoint; later integrated run remains required.
- Private session success/callback failure/cancellation tests passed: auth required,
  ingress absent, listeners joined, DB closed, private root removed.
- Private startup failures before store acquisition, after schema acquisition and
  during workspace preparation passed. Workspace release invoked once; injected
  release failure surfaced. Common public listener-bind refusal tests also passed.
- Concurrent public live runtime noninterference and immutable projection cleanup
  tests passed.
- Internal MockAgent signed-ingress/retained-restart proof passed on both stores.
  Golden SQLite internal smoke passed. Full internal J1-J5 lifecycle matrix passed
  on SQLite/PostgreSQL (54.878s), preserving receipt chain, semantic identities,
  old snapshots, crash convergence and fresh-epoch cardinality.
- Internal authored/derived/root-setup/atomic-publication scenarios passed. Exact
  scenario profiles and mismatch refusal passed across SQLite/PostgreSQL.
- Selected-fork provider/model/failure-compensation focused tests passed (4.712s),
  including custom model preservation after configured aliases change.
- Controlled live Anthropic/Telegram transport proof passed with source doubles
  unchanged. This is not paid external-provider or provisioned Claude acceptance.
- Native boot purpose/refusal matrix passed. API specification tests passed (1.820s).
  Production async-owner census passed (0.865s); focused persistence boundary guards
  passed. Retired product-surface census and git diff checks passed.
- Actual compiled public live possession and invocation-root tests passed (14.064s),
  including retained/dev ownership, hostile ancestor and borrowed-project refusal.
  Agent-free fixture: this does not claim live-provider turns or agent restarts.

Large/whole suites use go run ./cmd/swarm-test; targeted tests use ordinary go test.
The first unfiltered whole suite failed in nine packages. In-scope fixture/guard
repairs are committed in 009e18c34 and pass targeted checks, including Claude
retry/restart, directive acknowledgment/rollback, private-runtime isolation and
exact descriptor consumption. The second unfiltered run completed with two failing
packages (conformance and serveapp). The remaining fixture repairs pass targeted
startup, decoder, abandonment and complete dual-store mock emission checks. New
dual-store persisted descriptor corruption/read-only proof also passes. The next
unfiltered run will enable the full proof profile, including retained lifecycle
journeys. No whole-suite success is claimed. The complete 29-row checklist and proof
gaps are recorded in issue-2321-proof-accounting.md; it is not a final proof audit.

## Remaining Work And Acceptance

1. Complete the unfiltered integrated swarm-test run and repair in-scope failures.
   Rerun changed proof families on the final commit rather than aggregating obsolete
   checkpoint results into a final-head pass.
2. Finish explicit M1-M29 owner/consumer/proof accounting, including failure injection
   for every acquired resource and all required selection/reintroduction negatives.
   A shared constructor or pre-existing passing test is not alone closure proof.
3. Complete actual live serve/dev/signed-ingress/graceful/crash restart proof on both
   stores with provisioned prerequisites. This host lacks Claude and the workspace
   image. An environment/configuration location has been requested; no credentials
   were logged and no paid live turn has been attempted.
4. Keep credential-absent webhook startup/activation and Telegram example collapse
   explicitly split/tracked with F in #2319, not passed. The approved split permits
   independent #2321 review/merge/closure; the broader onboarding claim stays open.
5. Record the final-head exhaustive proof audit, then open the single normal PR
   only when review-ready. No final proof audit, PR or failure-class closure exists
   at this checkpoint.

## Additional Verification Finding

The third complete run used the full proof profile and passed releasee2e (533.640s),
including the retained J1-J5 and golden restart/burst assertions. It did not pass
the whole suite: conformance reported a terminal lifecycle diagnostic projection
conflict; serveapp found one unmigrated mixed-agent fixture and two ambiguous audit
text references; runtimepersistence exhausted the default aggregate ten-minute
package timeout while its current subtest had run only two seconds.

The mixed-agent fixture now uses the existing H entry, not public live serve as a
mock host; the exact readback/cardinality assertions remain. Both store cases and
the audit-text guard pass (15.019s). These are bounded test/document corrections.

The lifecycle conflict is not dismissed as flaky or suppressed. Ten focused real
SQLite three-child repetitions passed (61.285s), but a deterministic two-reader
barrier probe fails three of three times: the same pending diagnostic is logged
twice and the second mark returns `lifecycle diagnostic projection conflict`.
The probe exercises the production manager projector with an explicit snapshot/CAS
test double; it is not additional real-store execution credit. The real SQLite
manifestation is the full-profile failure. Reproduction source is retained in
`issue-2321-diagnostic-projection-probe.go.txt`, not in the passing test inventory.

`projectLifecycleDiagnostics` is byte-identical to the integrated bdc82dfba base.
Its list/log/mark steps have no concurrent claim or serialization, and the two
store mark operations intentionally require one newly updated row. Terminal
completion, loop/start paths and startup hydration can all call this projector.
Lifecycle diagnostic convergence is distinct from command-owned selection and
needs lead tracker/scope disposition before production remediation. No projector,
store CAS, lifecycle ownership, or diagnostic failure policy was changed here.
No new issue or reopening of a closed tracker has been performed by this work.

## Lead Disposition And Separate Repair Gate

The preceding request for classification is superseded by the lead ruling on
#2321: https://github.com/division-sh/swarm/issues/2321#issuecomment-5588210838.
#2321 keeps its approval. The lead created separate acceptance prerequisite #2432
for lifecycle diagnostic projection convergence AND provenance, including sequential
acknowledgement failure and cross-manager caller-context contamination. #1927 stays
closed; #2250 retains broader phase-ownership debt.

Agent G posted the bounded #2432 Pre-Implementation Coverage Audit at
https://github.com/division-sh/swarm/issues/2432#issuecomment-5588649360,
with checked-in artifact at 5e303a531 on agent-g/2432-diagnostic-audit, based on
fresh origin/master 496498457. It requests an independent repair gate for one
selected-store atomic projection/ack operation, exact persisted mode/provenance,
and auxiliary-error separation. Production diagnostic repair remains unstarted.
The existing watchlist refinement swarm-docs@12e94ca was consumed. No additional
watchlist change or issue is proposed.

Independent #2321 work remains authorized. F's #2319 is still open; a current
gate/activation schedule and PR reference have been requested on its thread.
Live-provider provisioning was rechecked: Docker exists, but this box has neither
Claude nor a Swarm workspace image (only Go and PostgreSQL images). No external
provider execution is claimed. The exact planned TestCommandLiveServeAndRestartParity
journey still needs implementation and execution; the existing paid Claude lifecycle
test is not an automatic replacement for the approved serve/dev/restart matrix.
An environment/configuration location, not credential values, has been requested.

After integrating the separately gated repair, rerun the failing conformance family
and dual-store lifecycle proof, then the whole suite with explicit package budget:
`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -timeout=30m ./...`.
Individual scenario deadlines remain enforced; a repeated timeout requires diagnosis.
No final proof audit, review-ready PR, whole-suite pass or runtime closure is claimed.

## Diagnostic Gate Amendment And Provisioning

The first independent #2432 gate was insufficient: selected-fork lifecycle
producers and both activation validators consume the same durable diagnostic path.
G posted the additive provenance amendment at
https://github.com/division-sh/swarm/issues/2432#issuecomment-5589274582,
pushed as cfc60b3b4 on agent-g/2432-diagnostic-audit. It requests a focused
independent re-gate; diagnostic implementation remains frozen. Watchlist 7a23362
already records this correction. #2321's approval is unchanged.

Live environment provisioning has started through the supported command:
`env -u SWARM_TEST_POSTGRES_DSN go run ./cmd/swarm workspace build --backend claude_cli --image swarm-workspace:agent-g-2321`.
The first invocation correctly refused the inherited test-quarantined variable;
the retry reached Docker package installation. Build/Claude validation has not yet
completed at this checkpoint, so no provisioned image or authenticated provider
turn is credited. The command log is /tmp/agent-g-2321-workspace-build.log.
The requested supported credential configuration location remains outstanding.
F's #2319 remains open with no reply to the existing coordination request.

## Approved Independent Acceptance

The user-approved split supersedes earlier F-dependency wording:
https://github.com/division-sh/swarm/issues/2321#issuecomment-5590488648.
Watchlist a5ed497 is consumed. #2321 can be reviewed, merged and closed without
waiting for #2319 once #2432 repair, genuine provisioned L live/restart proof,
all in-class checks, the integrated full suite and independent review pass.
Credential-absent ingress startup/activation and Telegram example collapse remain
F-owned and explicitly split/tracked, never passed or credited to H. Existing
fail-closed ingress credential checks remain valid; no mock fallback is authorized.
#2432's fork-provenance re-gate remains pending; no diagnostic implementation starts.

Provisioning follow-through: the default-network workspace build failed because
the build container could not resolve deb.debian.org. Host and host-network
container DNS resolve it. Retrying the unchanged Dockerfile.workspace through
Docker with build-time --network host succeeded; no runtime networking or credential
policy change. The supported public workspace build then passed against cached
layers, including its runnable-Claude validation. Image
swarm-workspace:agent-g-2321 is available; direct `claude --version` returned
2.1.87. Logs: /tmp/agent-g-2321-workspace-host-build.log and
/tmp/agent-g-2321-workspace-build.log. No authenticated provider turn, runtime
network readiness or retained restart is credited. The expected OAuth credential
is absent from this process environment; the supported configuration location
request remains outstanding. At this checkpoint the exact L harness still needed implementation;
the next section records its subsequent addition, not passing live execution.

Administrative split verification: API-spec package passed (4.773s), both YAML
documents parsed, and git diff --check passed. No production runtime code changed;
the full suite is deferred to the integrated diagnostic repair, not claimed green.

## Public Live Restart Harness

Added `TestCommandLiveServeAndRestartParity` in releasee2e. It builds and launches
the actual public binary, never the H entry, against the unchanged standing Telegram
source including its authored double. It uses existing ordered-receipt, exact-route,
delivery, event, decision-card and public readback assertions with an explicit live
expectation; H still asserts mock mode. No runtime semantic owner changed.

The planned execution covers retained SQLite and isolated PostgreSQL, graceful
restart, paused-delivery SIGKILL followed by intrinsic default recovery and fresh
same-conversation ingress, then two SQLite dev epochs and reopening the untouched
retained store. It checks immutable old event/delivery facts while permitting new
startup diagnostics, exact transport/route continuity, live agent/conversation/event
mode, connector success counts and source bytes unchanged. Dev is SQLite even when
the retained store is PostgreSQL, not a claim of PostgreSQL dev-scratch support.

Opt in with `SWARM_COMMAND_LIVE_E2E=1`. Required inputs are
`CLAUDE_CODE_OAUTH_TOKEN`, `TELEGRAM_BOT_TOKEN`,
`SWARM_COMMAND_LIVE_TELEGRAM_CHAT_ID` (authorized dedicated private chat),
`SWARM_COMMAND_LIVE_WORKSPACE_IMAGE`, and `SWARM_TEST_POSTGRES_DSN`.
`SWARM_COMMAND_LIVE_WORKSPACE_NETWORK` optionally selects a supported deployment
network. Credentials enter through public `secrets set --stdin`, not source edits;
API/signing credentials are fresh local transport secrets. The test performs real
provider turns and sends real Telegram messages; no bot or provider double is used.
Process output is redacted before diagnostic rendering, including split writes.

Execution so far: prerequisite, output-redaction, public-boundary and existing
journey-census tests pass (0.017s); existing H SQLite smoke passes (26.100s).
Explicit L opt-in without the OAuth credential fails before process creation with
the exact missing prerequisite (0.003s). That is refusal evidence, NOT live success.
The retained H dual-store J1-J5 matrix is submitted through swarm-test and currently
queued behind other work; no matrix result is claimed yet. No final full-suite run
or provisioned L pass is claimed. Bare live archetype proof is still separate work,
not automatically credited by this fixture or the installed workspace image.

## Provisioned Live Attempt Follow-Through

The user supplied protected local credential files and authorized a dedicated
private Telegram destination. Bot getMe and the exact private smoke marker were
verified without printing credentials or changing an existing webhook. A real
tools-disabled Claude prompt in the workspace succeeded in 2.612s, with one turn
and reported cost USD 0.0221115. This proves authenticated provider access, not
Swarm lifecycle or tool-gateway convergence.

The previously queued retained H J1-J5 SQLite/PostgreSQL regression passed in
50.505s through swarm-test. That remains H credit, not L.

The first actual L SQLite attempt reached readiness but the harness's millisecond
message ID exceeded the provider-pack integer schema and was correctly refused.
Replaced it with a deterministic, schema-valid per-journey counter spanning restarts.
The next attempt admitted ingress and created the agent but timed out waiting for
the reply approval; no successful Telegram delivery is claimed.

An independent Docker connection probe found the harness's loopback MCP listener
unreachable through the container host address, while the host bridge interface
was reachable. The L harness now requires SWARM_COMMAND_LIVE_MCP_HOST, an explicit
container-reachable host interface IP, and passes it to the existing public MCP
listener flag. It rejects loopback/wildcard inputs. H keeps its original loopback
default. No runtime gateway owner, credential rule, retired environment side
channel, source file or production semantic changed. Added sanitized process/run
diagnosis on failure and per-ingress progress for the next actual attempt.

The queued diagnostic-only rerun was canceled before acquiring a slot after the
network mismatch was identified. The corrected L SQLite rerun is queued through
swarm-test; /tmp/agent-g-2321-live-sqlite-4.log owns its pending result. No passing L
turn, connector success, restart or full-suite result is claimed. #2432 remains
separately gated and frozen. No new production defect has been established by
these harness/deployment findings.

## Live Approval Assertion Repair

The reachable-listener SQLite attempt executed in 34.335s after acquiring the
shared test slot. Real Claude produced a live connector approval card. The test
then stopped before mailbox.decide because the reused H helper incorrectly
required mock mode. This is a test-contract error, not a production failure or
successful Telegram-send proof.

Approval now takes an explicit expected mode: H retains mock and L requires live.
TestLifecycleEffectApprovalUsesExpectedMode exercises both modes with a local RPC
double and verifies exactly one approval against the exact card/content identity.
The focused approval, prerequisite and public-boundary checks pass (0.019s).
This local helper regression is not L proof. No runtime code changed.

The repaired real SQLite journey is first in the shared swarm-test queue at this
checkpoint; /tmp/agent-g-2321-live-sqlite-5.log owns its result. Telegram delivery,
live restart, PostgreSQL L and final whole-suite completion remain unproven.

## Post-Approval Live Failure Investigation

The repaired SQLite journey acquired a slot after 25m50s and failed after 22.624s.
It passed live approval, but the inbound telegram delivery terminally dead-lettered
with agent-manager/process_event.on_event unclassified_runtime_error. The user
independently confirmed receiving the lifecycle 1001 Telegram message. That is
external-send evidence, not a successful provider-turn/delivery completion or
restart result. Do not automatically retry that failed delivery.

The original temporary store was cleaned up by the test. Failure-only capture now
reads the existing public runtime.logs, event.list, conversation.list and per-session
conversation.list_turns in addition to run.diagnose, before process/store cleanup.
Every rendered response uses the existing supplied-secret redactor. A fresh isolated
SQLite diagnostic journey is queued through swarm-test; it does not replay the
previous failed delivery. Its log is /tmp/agent-g-2321-live-sqlite-6.log.
The root cause is not yet established. No production repair, broader-class
absorption, new gate approval or full live proof is claimed.

The expanded diagnostic run subsequently failed in 29.334s. It confirms the
successful connector result and locates the turn failure at
claude_cli_capability_validation. The deterministic parser-to-validator probe
reproduces an empty-native-to-canonical-MCP fallback defect 3/3 on this branch and
3/3 on current origin/master@05528a92e. Mixed native+MCP and missing/unexpected
native controls behave as expected. See issue-2321-mcp-only-capability-escalation.md
and the adjacent executable probe text for exact evidence, consumer census and
requested bounded lead disposition. No further Telegram sends are needed for
classification, and no production capability change is authorized or implemented.

## Approved Capability Repair

The bounded independent approval at issuecomment-5592428086 supersedes the last
checkpoint's implementation freeze for this class only. The parser now retains
valid/missing/invalid inventory state and accepts inventory only from provider
system/init metadata. One strict entry parser replaces the two permissive entry
parsers. The native reader no longer falls back to canonical display names.
Managed observation/validation, startup and separate fork-chat comparisons consume
the checked reader. Fork display recognizes valid-empty while retaining its
existing local sandbox policy. No native grant, control allowlist or retry changed.

Normal post-provider observation failure now reaches the existing uncertain
completion settlement, just like a genuine native mismatch, rather than returning
before settlement. Actual fake-provider fresh and resumed/tool-result invocations
prove one uncertain settlement and no extra invocation or repeated local effect.
Synthetic native fixtures now use the real inventory parser. The unrelated drained
completion fixture now emits real stream initialization instead of inventing a
successful inventory-less JSON response.

Focused parser/channel/presence, startup, managed process, fork policy, API and
mock controls passed three repetitions (0.438s). API-spec tests passed (1.751s)
and git diff --check passed. The broader LLM package and authorized fresh
SQLite/PostgreSQL L journeys are queued through swarm-test, not yet credited.
Their logs are /tmp/agent-g-2321-inventory-llm.log and
/tmp/agent-g-2321-live-inventory-repair.log. No already-sent delivery is replayed.
#2432 remains separately frozen. No final audit, full-suite or merge-readiness
claim is made by this checkpoint.

## Capability Repair Execution Results

The full LLM package passed through swarm-test (2.105s). Focused race-enabled
inventory/startup/fresh/resumed settlement proofs passed (13.431s). The complete
retained H J1-J5 matrix passed on SQLite/PostgreSQL (55.288s). These remain
distinct proof surfaces, not substitutes for L or the integrated full suite.

The first repaired L run passed the live SQLite turn and delivery but exposed a
test-only receipt join hardcoded to mock chat 42. The join now takes the exact
authored test conversation reference for both H and L; ordered receipt IDs and
downstream target equality remain mandatory. TestCommandLiveReceiptUsesExactConfiguredConversation
proves both reference shapes and wrong-reference/occurrence/order refusals. The
three focused L helper tests passed three repetitions (0.009s).

PostgreSQL secret setup initially refused SWARM_GOLDEN_POSTGRES_PASSWORD as an
undelegated environment variable. The live harness now removes that database-only
variable from source-free secret commands and SQLite dev invocations, matching H.
No production environment guard changed. Failure output uses the existing secret
redactor, including database passwords.

The corrected full L attempt ran 246.257s: each store completed its first live
Claude/MCP/Telegram turn and original delivery. Each then timed out awaiting the
second reply after graceful restart. PostgreSQL public readback independently
showed a failed second turn at claude_cli_process_failed/run_streaming, one provider
dispatch and zero tools/output. The attempt was outcome_uncertain; re-entry was
refused with external_effect_replay_fingerprint_conflict despite equal request
fingerprints. This is not the corrected capability-validation failure. No old
delivery was replayed or reset by the harness/operator.

No restart, SIGKILL recovery, repeated dev epoch or full-suite L closure is claimed.
See issue-2321-live-restart-observation.md. Before broadening production scope,
the separate post-restart provider failure needs classification. A fresh PG
diagnostic rerun is queued through swarm-test to retain the underlying attempt
stderr/stdout before cleanup; it does not retry the failed delivery. #2432 remains
unchanged and frozen. No full issue/merge-readiness claim is made.

The approved bounded repair was pushed as 2c958d04f. The fresh PG diagnostic rerun
subsequently failed in 120.763s, with the first turn/delivery successful and the
second provider process returning error_during_execution with zero turns/API
time/cost. Persisted stdout is truncated before its explanation; missing
provider-local session state is still a source-backed hypothesis, not confirmed
root cause. See the updated observation artifact and #2321 comment
https://github.com/division-sh/swarm/issues/2321#issuecomment-5592833963.
All launched proof jobs have completed. No further sends, production expansion,
full-suite success or review-ready PR is claimed. #2432 remains frozen.

## Approved Restart Addition: Failure Handoff Implemented

The preceding hypothesis/classification-request status is superseded by the lead's
confirmed Docker reproduction and bounded approval:
https://github.com/division-sh/swarm/issues/2321#issuecomment-5593141304.
The full approved census is transcribed in issue-2321-provider-restart-amendment.md.

First coherent repair implements nonretryable uncertain CLI failure handoff and
bounded structured error explanations, with authoritative spec updates. Exact
prelaunch retry remains unchanged. The original cause and explanation persist
into terminal readback; no later retry/replay-conflict overwrites them.

Passing proof: selected-store delivery/restart/settlement-fault/prelaunch controls
(SQLite/Postgres, 5.893s); targeted timeout/inventory/diagnostic/startup tests x3
(0.697s); focused race tests (13.605s); apispec (1.652s); full LLM package through
swarm-test (2.220s). Tests first exposed stale expectations for retryable started
failures, corrected alongside the binding contract. Positive fixture output now
includes authoritative init; result-only JSON remains a negative inventory case.

Provider-private backing implementation, offline retention/isolation/cleanup proof
and new dual-store L restart are still outstanding. No paid live calls, delivery
replays, full integrated-suite pass or review-ready PR claimed. #2432 remains a
separate blocker. No new issue, framework, migration or retry policy introduced.
