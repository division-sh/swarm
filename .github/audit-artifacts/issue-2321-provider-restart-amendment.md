# Pre-Implementation Coverage Audit: approved provider restart addition

Gate: https://github.com/division-sh/swarm/issues/2321#issuecomment-5593141304
Baseline: fadd6b36e. Category: bounded semantic lifecycle/parity addition to #2321.
This artifact transcribes the approved addition before production repair. No row
below is an execution pass. #2432 remains separately frozen; #2319 is nonblocking.

## Classes and owners

The observed second-ingress failure is an entry point, not the boundary. The
chosen class is provider-private resumable conversation artifacts having the
same usable lifetime and exact identity as the canonical reusable session,
rather than the disposable container. Immediate parent: provider-session
continuation integrity across execution reconstruction. Broader parent: durable
artifact ownership/lifetime/isolation (#1965, still open). Two additional bounded
classes are uncertain-completion/public-failure agreement and bounded structured
provider error rendering. All three are absorbed, not new issues or first slices.

Canonical owners remain selected-store session/lease and completion authority
(memory identity, confirmed head, fencing, outcome), workspace lifecycle
(backing and disposable mounts), Claude adapter (provider-private layout and
launch binding), and the existing failure registry (retry disposition). These
are semantic owners, not merely the first failing helper. No new lifecycle,
logging, retry, migration, or persistence framework is authorized.

Full path: command selection -> source/actor admission -> lifecycle/topology ->
exact memory acquisition -> workspace/provider backing admission -> completion
authorization -> process launch -> provider observation/validation -> atomic
completion/head settlement -> conversation/output -> delivery/receipt. Selection
and topology retain their already-audited owners; backing and failure handoff
are this addition. Completion authorization, fencing, and receipt consumption
already consume their owners and are negative controls, not weakened gates.

## Systematic consumer and manifestation matrix

Every same-class consumer below moves to the checked binding or failure reader;
existing canonical session, completion, and delivery consumers retain authority.
Test names prefixed Planned are proof obligations, not implemented tests.

| ID | Consumer / manifestation | Classification and exact planned proof |
| --- | --- | --- |
| R1 | Memory-enabled StartSession hydration and continued turns | Move to checked backing; PlannedClaudeRetainedState proves exact head/context before/after real network-disabled DockerManager release. |
| R2 | Graceful release and fresh container attachment | Same owner; R1 asserts different container, identical retained backing, recognized synthetic transcript without network/auth. |
| R3 | Forced loss, stopped-container replacement, recovery=false | Same owner; PlannedClaudeProjectionReplacement varies all three lifecycle postures and proves unchanged exact state, no old delivery dispatch. |
| R4 | Per-agent and per-flow-instance workspaces; equal display slugs | Move to checked binding; PlannedClaudeStateIdentityMatrix proves shared workfiles do not share actor/session provider state. |
| R5 | Different full source, store epoch/run, session rotation, memory disable/re-enable | Same owner; identity matrix proves distinct namespaces and no predecessor selection. |
| R6 | Stateless invocation and tool-result continuation | Same owner, invocation lifetime; PlannedClaudeInvocationState proves within-invocation continuity, next invocation isolation and cleanup. |
| R7 | Runless startup probe | Same launch owner, ephemeral authority; PlannedClaudeProbeState proves no managed namespace and cleanup on success/failure/cancel. |
| R8 | Selected-contract run fork | Same launch owner, fresh run/session; PlannedClaudeSelectedForkState proves no source provider head/artifact adoption on both stores. |
| R9 | Conversation fork-chat | Same launch owner, separate execution-local authority; PlannedClaudeForkChatState proves allowed local continuity without ordinary reusable-memory promotion. |
| R10 | Missing/corrupt/unwritable backing, stale lifecycle, mount failure | Same owner; PlannedClaudeBackingRefusal proves no launch/head promotion/fresh fallback for each negative. |
| R11 | Streaming/nonstreaming started exit/timeout | Move to canonical uncertainty handoff; TestClaudeCLIUncertainFailureDisposition and supported fake-process tests prove terminal classification, original cause, exact one dispatch. |
| R12 | Response observation, capability/tool validation, settlement and projection failure | Same handoff; managed supported-path tests plus settlement-failure injections prove uncertainty remains outermost even with retryable joined causes. |
| R13 | Exact prelaunch refusal | Existing policy control; supported adapter tests retain retry only where the canonical original prelaunch class permits it. |
| R14 | Single JSON/JSONL structured errors; malformed/unknown/plain output | Local parser owner; TestClaudeStructuredErrorSummary proves allowlisted reason survives prefix noise, secret redaction occurs before truncation, unrelated fields never appear. |
| R15 | Delivery, receipt, terminal error readback | Existing consumers; dual-store supported adapter failure proofs require no retry transition/replay-conflict replacement, one provider dispatch, original cause detail. |
| R16 | Provisioned real live serve first turn/restart/distinct second ingress | TestCommandLiveServeAndRestartParity SQLite/Postgres; require reply, accepted turn, exact delivered receipt for both new ingresses. Never replay the sent delivery. |
| R17 | HTTP providers and mock | Different artifact concept: platform-managed history, no Claude JSONL. Existing provider completion and mock tests remain controls; no transport redesign. |
| R18 | Host Claude | Explicitly split #1213; existing unsupported-target test remains a fail-closed control. |
| R19 | Global artifact layout, portability, broader cleanup | Explicitly split #1965; same-host backing only. DB-only backup/cross-host failover is not a transcript restore contract. |

## Lifetime and cleanup decisions

The backing coordinate must include the full admitted source, exact typed
actor/run/flow identity, and platform session incarnation. It must not include
the process projection. That incarnation selects storage only, never --resume;
only the confirmed provider head selects a Claude conversation. Containers and
source projections remain disposable; standard /workspace scope and the three
logical mounts do not change. No operator home or shared per-flow provider home.

Reusable backing is retained across process release/forced loss. Terminated or
rotated session backing must be nonselectable through the session owner; any
retention/removal uses that exact artifact coordinate, not latest-session scans.
Run reset/deletion must not make retained files selectable in another epoch.
The implementation must record exact removal/retention mechanics and test-owned
cleanup before its backing commit. Ephemeral probe/invocation/fork state must
not acquire reusable authority and must have explicit terminal cleanup.

Missing/corrupt backing for a confirmed head refuses before provider dispatch.
Usable provider output must exist before candidate head promotion; existing DB
transaction/lease/lifecycle fencing remains authoritative. Unconfirmed children
are not adopted. No old-home copy, transcript reconstruction, fresh-session
fallback, obsolete-container retention, or redispatch is permitted.

## Failure and diagnostics decisions

### Concrete backing disposition (implementation in progress)

Reusable backing is a named volume keyed by full source hash, exact concrete
actor fingerprint and session UUID. It is mounted in a disposable execution
container that inherits the existing three logical mounts. Every launch uses
the checked private CLAUDE_CONFIG_DIR, never the operator or shared flow home.

The sibling continuation census includes `recoverCompletionContinuation`:
stateless session UUIDs are intentionally rebound after a process crash, while
the same delivery's consumed tool-result continuation remains recoverable.
Therefore a stateless *delivery* uses its existing opaque delivery claim's exact
delivery ID and actor identity as the private invocation coordinate, not its
process-local session UUID. It uses retained named backing without acquiring
agent memory. Recovery and every new dispatch still pass the existing completion
and delivery fences. No schema, recovery mechanism or second session owner is
introduced. Add R6 proof for reclaimed delivery/current session rebinding and a
different-delivery isolation negative, in addition to within-process continuation.

Non-delivery stateless invocations, fork-chat requests and startup probes use
tmpfs; their root invocation/probe completion releases the container on success,
failure or cancellation, retaining no provider files. Reusable and delivery
volumes are deliberately retained even after terminal settlement or session/run
termination/deletion as inert artifacts. Existing session/delivery owners revoke
selection; a new session UUID/delivery ID cannot reselect them. No generic GC or
cleanup registry is added. Tests remove only their exact generated volume keys.
All source projection containers remain disposable. This records retention, not
automatic artifact deletion, and does not claim #1965 cleanup closure.

No execution proof for these backing decisions is claimed by this amendment.

The CLI adapter returns the existing platform.outcome_uncertain envelope for
every started uncertain failure, retaining the original typed cause and bounded
details. Settling an attempt and returning a retryable original error is invalid.
Settlement-error joins cannot override uncertainty. Prelaunch rejection retains
its original canonical policy. No AgentManager exception or stdout-based retry.

Only documented structured result error explanations are projected from JSON
or JSONL, before size truncation. Existing supplied-secret redaction is applied
before bounding. Arbitrary result fields, prompts and raw structured dumps are
not explanatory evidence. Malformed/unknown/plain output remains bounded and
safe. This does not change #2432's durable diagnostic transaction.

## Governing spec, tracking and feasibility

Authoritative platform-spec.yaml updates accompany matching implementation:
engine.agent_session_management.llm_provider_adapter_contract.required_contract_dimensions.session_lifecycle;
engine.agent_session_management.provider_session_ids and agent_memory;
workspace_model.deployment_mapping.process_projection_separation and
durable_backing_identity; filesystem_source_model.runtime_projection;
managed_external_effect_authority.settlement and completion/failure contract.

Watchlist promotion is approved at swarm-docs e8bdb9f, existing
llm_provider_adjacency_contract_ownership and
workspace_backend_lifecycle_and_execution_boundary. Broader #1965 remains open;
#1780 stays closed/historical; #2432 is separately blocked. No new issue or
POTENTIAL_ISSUES entry. Parent tail is not re-estimated as closed by this child:
global artifact layout and host enablement remain separately owned, with low
confidence in any aggregate effort estimate absent their own audits.

Tracker state: current #2321 body already repaired by lead; consume this approved
addition and update proof accounting at each coherent commit. Intended closure:
failure class eliminated, contingent on every in-class row actually proven.
The target is complete bounded closure in this PR, not a mount-only local fix.
Feasibility: existing owners can cover all named consumers without new registry
or retry policy. Stop only for a newly contradictory contract or a required
change to these owner boundaries. A shared mount/helper alone is not proof.

Run deterministic tests before paid live work. Full integrated verification uses
go run ./cmd/swarm-test after #2432's separately approved repair. No final audit,
PR-readiness, live restart success or full-suite success is claimed here.

## First implementation commit: failure handoff and bounded diagnostics

R11/R13/R14/R15 now have executed proof for the following exact manifestations.
R12 has focused validation and settlement-failure proof, not complete provider
state/lease/projection closure. R1-R10 and R16 remain outstanding. The retained
artifact implementation is not included in this commit.

| Manifestation | Status | Executed proof |
| --- | --- | --- |
| Streaming and buffered process exit -> terminal receipt, restart preserves cause | reproduced and fixed | TestClaudePostlaunchFailurePreservesClassificationAndRestartRefusesProviderRedispatch, SQLite/Postgres x both formats; zero retries, one provider dispatch, identical failure before/after restart |
| Started timeout returned disposition | reproduced and fixed | TestClaudeStartedTimeoutIsTerminal, real fake-Docker subprocess in both output formats, uncertain settlement and retained timeout cause |
| Inner retryable error and joined persistence error | reproduced and fixed | TestClaudeCLIUncertainFailureDisposition; TestClaudeRetryableSettlementFailureRemainsTerminal on both stores |
| Provider-head commit error and recovery | execution-proven through the same corrected path | TestClaudeProviderHeadCommitFailureSettlesUncertain, SQLite/Postgres; no partial head/spend/turn commit and one dispatch |
| Exact prelaunch rejection still retries | execution-proven through the same corrected path | TestClaudeAttemptStartRejectionRetriesThroughSelectedStore on both stores, exact fresh ordinal and one actually started process |
| Missing/null/malformed/unexpected inventory after fresh or resumed launch | execution-proven through the same corrected path | TestClaudeCLIManagedInventoryFailureSettlesWithoutRetry; TestClaudeAttemptIdentitySelectedStoreMemoryAndProcessParity adds dual-store nonretryable result-only refusal |
| Structured error explanation after large usage prefix | reproduced and fixed | TestClaudeStructuredErrorSummary JSON/JSONL; delivery proof reads exact explanation from persisted terminal failure |
| Secret leakage, malformed/unknown structured data and UTF-8 size boundary | execution-proven through the same corrected path | TestClaudeCommandDiagnosticRedaction and TestClaudeStructuredErrorSummary; provider OAuth and MCP authorization values redacted before truncation |

Execution results: focused new/startup tests x3 PASS (0.697s); focused LLM race
matrix PASS (13.605s); selected-store serve tests PASS (5.893s); apispec PASS
(1.652s); full LLM package through swarm-test PASS (2.220s); diff check PASS.
No network/provider/Telegram calls were made. Full integrated suite is not run.

Fixture accounting: the old postlaunch test explicitly expected a retryable
connector receipt followed by replay-conflict replacement. That expectation is
removed. Success fixtures now use stream-json with real init evidence. Buffered
result-only JSON is a negative capability-observation control, not fabricated
init evidence and not credited as a successful buffered live turn. Streaming
success and exact prelaunch retry remain proven; both formats have process-error
proof. This consumes the already-approved inventory contract, not a new selector.

The original error remains reachable via errors.Is/Unwrap and its canonical
envelope is carried in cause_failure for persisted/public readback. Validation
paths build the same uncertain failure before settlement; the CLI return boundary
also covers postlaunch projection/settlement errors. No engine/manager retry
classifier, effect guard, provider-head writer, or #2432 diagnostic owner changed.
