# Activation lifetime and startup implementation progress

Agent G, 2026-09-09. Additive implementation against d14c0d0e9. This is NOT a
final Post-Implementation Proof Audit or a review-readiness claim. The binding
gate and complete required matrix are in
`issue-2321-activation-lifetime-amendment.md`. The prior provider-backing progress
artifact remains historical evidence, including its failed forced-restart rows.

Verification checkpoint: code/test head `eace6c528` passes the complete full-profile
swarm-test invocation (exit 0). Runtime head `97a9ea04a` passes genuine dual-store
live restart; the only later executable change adds scaffold proof/harness flags.
The remaining acceptance blocker is #2432 approval, repair and integration, not
another pending live run or a current full-suite failure. A final integrated-head
audit/review is still required after that separately gated work.

## Exact changed concepts and owners

The bounded class is exact executable channel-publication admission and lifetime
across startup, model presentation, transport descendants, validation and
execution while publication changes. Its parent is loss/reacquisition of runtime
capability authority at execution boundaries; the wider transport-policy parent
remains open. This amendment aims to eliminate the entire chosen class, not only
the observed tools/list timeout.

| Owner | Systematic consumption and old-path disposition |
| --- | --- |
| `channelactivation.Owner`, immutable snapshot, `Lease` | Still the sole executable publication owner. Typed presentation scopes validate source, actor identity/config, flow, run, entity and input against the issuing owner/current snapshot. Root admission keeps the fence; retained descendants consume the predecessor and are counted/joined before replacement. The tools-private presentation context type is deleted. |
| `LLMAgent.applyTurnToolDefinitions` | Both normal and callback turns already own root presentation; their existing deferred release now cancels and joins retained descendants. No new agent registry or durable lease was introduced. |
| `Executor.AcquireToolDefinitionsForActorInContext` | Nested model/catalog acquisition validates and retains the exact inherited pin rather than entering root admission. Missing owner with inherited authority is an error, not a fallback. |
| Existing `TurnContextRegistry` | Managed and startup registration share `RegisterTurnContextWithCapabilitySurface`; conversation-fork registration separately carries the binding, source, inbound and run. Registration holds no publication refcount. Each admitted request retains a child; unregister/reset revoke under the registry lock and join outside it. Expiry cancels children; owner/root lifetime still joins them before publication. An already-resolved expired binding is synchronously refused. |
| MCP `Gateway` | tools/list, tools/call and direct `/tools/` requests all acquire the registered binding before context reconstruction. Definition hashes, managed effects/occurrence checks and fork policy remain authoritative. Eager unleased catalog projection is removed when a leased executor exists. |
| Channel runtime/private activity | Existing exact operation checks and synchronous execution-child borrowing remain. Transport children reach this same execution owner; no successor lookup or policy shortcut is added. |
| Context manager/channel refresher | Existing occurrence validation, writer serialization and replacement drain remain unchanged. HTTP handlers bind the runtime gateway directly; actor hydration uses AgentManager, not the ContextManager mutex held by replacement. |
| Startup provider probe | `startupToolPlan` acquires a presentation and holds it through provider preflight/MCP requests and registry cleanup. Preparation remains non-business, declared-only validation, not authority to send channel effects. |
| Selected-contract fork | Existing managed preflight and agent turn construction consume the same gateway/executor owners. Protocol fixtures now explicitly provide their provider-state resolver and valid inventory instead of relying on removed production fallback. |
| Conversation fork | Existing explicit sandbox is separate policy. A registered inherited presentation is preserved, but cannot authorize channel operations excluded by `CanonicalConversationForkSandboxPolicy`. A genuine fresh fork without a parent uses current admission, not a persisted/process lease reconstruction. |
| Durable replay and API/mock turns | No process-local binding is serialized. New turns obtain current owner authority; non-provider structural/mock admission remains unchanged. |

## Startup producer census and phase change

`Runtime.Start` still delegates to the same construction/startup implementation;
it calls `PrepareStart` then its single-use release. Serve uses that same
preparation but installs executable channels before invoking the release.

| Entrance/consumer | Placement and authority |
| --- | --- |
| All-context topology observation, author catalog and lifecycle admission | Existing preflight remains ahead of startup mutation. Preparation establishes the exact runtime occurrence. |
| Static topology, ingress store synchronization and directive reconciliation | Existing preparatory owners remain before release; no provider/business loop is started by these calls. |
| System nodes | Subscription readiness remains preparatory. Business manager loops, delivery continuations, maintenance, timers and outbox are withheld. |
| Credentials, workspaces, managed provider preflight | Remain fail-closed preparation requirements. Probe execution retains isolated probe/provider and tool authority, not business admission. |
| Source-scoped readiness canonicalization | Existing source-scoped writer runs before Manager.Run; no predecessor reconstruction or unscoped read is added. |
| Standing services and dynamic process preparation | Existing standing reconciliation, run admission, flow activation transaction and publication-sequence writer prepare canonical facts. `PrepareStandingFlowInstance` reconstructs process topology only. The existing exact activation finalizer, creation work and timer restoration are deferred. No fake publication sequence or copied readiness rows. |
| RuntimeContext registration | Registers the prepared occurrence and exact standing targets so existing channel recovery/publication can validate them. Registration is not public readiness. |
| Destructive and retired/local connected-state reconciliation | Existing owners run before executable publication. Their errors refuse startup. |
| `publishChannelActivations` | Installs the complete executable publication before business release. No declared plan is promoted to learned authority. |
| Manager.Run/completed topology/standing finalization | Run only in release, after publication. Failed finalization joins cleanup and never starts the subsequent autonomous producers. |
| Manager hydration, managed recovery and delivery continuations | Existing topology-before-replay order retained inside release. |
| Generic schedules, workflow timer restoration, pipeline maintenance and run completion worker | Existing `releaseAutonomousStartupProducers` remains after topology and continuation synchronization, and now necessarily after channel publication. |
| EventBus outbox and boot publication | Remain after business-release prerequisites. |
| Public readiness, effects recovery and ingress | Existing serve admission remains after successful release; partial preparation and publication failure cannot commit readiness. Cleanup errors are joined rather than discarded. |
| Multi-context serve | All occurrences prepare before channel publication; existing supported backend admission and unsupported multi-context Claude refusal are retained. No new provider posture. |

## Executed evidence and unclaimed rows

These are actual focused results, not proof-by-shared-owner. Broader/integrated
commands are still outstanding below. Final closure must bind a committed head.

| Manifestation | Exact proof/result |
| --- | --- |
| Nested predecessor catalog and successor drain | `TestPresentationDescendantsJoinPredecessorUnderFence`; `TestChannelPresentationExecutorAndRegisteredForkCatalogUnderReplacement` PASS. Real executor with deterministic owner-fence barrier. |
| Managed HTTP list/call and direct route/private activity | `TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores/registered_managed_*` PASS on both stores, race count=3. Actual managed registration, selected-store completion/delivery claim, capability hashes/evidence, HTTP handler, channel executor, private activity and localhost connector. The replacement points to a distinct unreachable successor. |
| Source/actor/run/input/token negatives | The same managed test independently changes source, actor, run, inbound presence and expiry; zero extra connector calls. Owner scope matrix also checks typed identity, flow/entity, foreign owner and revoked parent. |
| Fork HTTP catalog | `TestChannelPresentationExecutorAndRegisteredForkCatalogUnderReplacement` PASS count=3. Actual conversation-fork registration and canonical sandbox policy. Catalog succeeds under the fence but does not expose the forbidden channel tool; not claimed as a live fork conversation. |
| Unrelated requests, canceled replacement, revocation/join | `TestPresentationUnrelatedAdmissionAndCancelledReplacement`, `TestPresentationExpiryRefusesAlreadyResolvedBindingWithoutTimer`, `TestTurnPresentationRevocationJoinsRequests` PASS under race count=3; unregister/reset/expiry/parent cancellation/completion each covered. |
| Due schedules/timers and aborted preparation | `TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores` PASS. Added preparation-abort and standing-finalization failure controls; zero due-work publication before release, injected error preserved, release after shutdown and double release refused. |
| Retained standing ingress | `TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies` and PostgreSQL sibling PASS (45.868s aggregate). Real serve composition/local provider protocol, not paid Claude credit. |
| Selected-fork capability/startup protocol | `TestExecuteSelectedContractRunForkClaudeOAuthPersistsStartupAndTurnCapabilityAuthority` and `TestSelectedContractForkManagedPreflightUsesExactProviderPromptAndExecutesEligibleMCPToolCall` PASS (1.721s aggregate). Explicit fake backing is only protocol evidence. |
| Release binary/emulator controls | `TestClaudeCLIManagedLifecycleFromReleaseBinaryDefaults` and strict malformed Docker command controls PASS. Emulator validates exact provider-container command shape; does not prove filesystem retention. |
| Real stateless reclaim and startup probe isolation | `TestClaudeStateDockerRetentionAndRefusal` and `TestClaudeStateDockerSharedWorkspaceIsolation` PASS with real Docker, network disabled (46.366s aggregate). Typed session/fork/delivery constructors produce backing; real Claude recognizes the reclaimed delivery and temporary fork/invocation transcript before refusing offline authentication. Probe cannot read ordinary state, ordinary state cannot read probe head, probe release destroys its tmpfs. Foreign actor/marker and corrupt/missing retained backing refuse. This does not claim a paid stateless tool-successor completion. |
| Async lifetime and persistence inventories | Complete worklifetime/MCP/channelactivation packages PASS; persistence-authority registry and API specification checks PASS; `git diff --check` PASS. |
| Broader startup failure/configured/no-channel/multi-context matrix | PASS through swarm-test: runtime 3.959s, serveapp 38.390s. Exact selection: `TestChannelOnboarding`, `TestConnectedChannel`, `TestDynamicTopologyStartup`, `TestValidateServeMultiContext`, `TestServeRuntimeConfiguredChannel`, `TestPrivateTestStartupFailure`, `TestInboundAdmissionSupportedSurfaceStartupFailures`, `TestRuntimeStartWithholdsDueSchedules`, `TestRuntimeStart_DisablePersistentStartupRecoverySkipsUnscopedStoreReads`. |
| Complete integrated suite | `SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -timeout=30m ./...` PASS, exit 0, code/test head eace6c528. Releasee2e 462.329s, conformance 266.181s, serveapp 405.360s, runtimepersistence 582.435s, manager 19.062s, CLI 90.476s, API 199.577s. Cached unchanged packages are explicitly reported by Go. The earlier four guard/fixture failures are fixed. This full run does not enable opt-in paid/live flags: the real dual-store journey and scaffold proof are separate executed results above/below, not inferred from skips. #2432 is still unimplemented and unapproved; a green run does not retract that separately reproduced class. |
| Genuine live forced/dev and complete retained history | `TestCommandLiveServeAndRestartParity` PASS against runtime head 97a9ea04a, SQLite 103.97s and PostgreSQL 100.49s (209.675s aggregate). Real public binary, Claude, Docker and Telegram; no H entry. Six fresh ingresses per backend prove initial turn, graceful retained restart, paused pending delivery recovered after SIGKILL, subsequent new ingress with old delivery unchanged, and two fresh dev epochs. Each checks reply/card and delivery settlement, not delivered status alone. Dev storage is SQLite by contract in both parent journeys. Reopening the retained source after dev preserves exact event/delivery/card history. Twelve authorized new messages total; no old settled delivery replay. |
| Untouched generated archetypes | `TestCommandLiveUneditedScaffoldReadiness` PASS (23.930s), using unchanged runtime 97a9ea04a plus the new test-only harness option. Both zero-agent-automation and webhook-responder pass bare verify/describe/test then real public retained/dev readiness. Claude/image/network are legitimate operator-global provisioning; no project config or command source/backend/store selector. Credentials enter through public secrets set. Authored tree snapshots stay identical. No business inputs/messages sent or turn proof claimed. First run correctly refused the unprovisioned built-in Anthropic profile; the test was corrected to select its provisioned Claude profile globally, not by changing production defaults. |

## Tracking and closure limits

Spec changes are in `tool_model.hitl_channel_pack_interface.connected_channel_onboarding.activation_rule`.
The lead's watchlist refinement 11ec18c covers inherited presentation lifetime and
publication-before-execution. No new issue, compatibility layer, retry mechanism,
startup registry/framework, ledger or table is introduced. The architecture repair
is promoted within #2321; estimated remaining work is final verification and any
ordinary in-scope failures, not another owner design. The fresh live journey now
passes; this does not retroactively prove the exact interleaving of the old hang.

#2432 remains a separately frozen acceptance dependency awaiting review of the
already-submitted fork-provenance amendment. #2319 remains F-owned/nonblocking.
Broader artifact layout remains #1965; this does not claim those parent classes
closed. Parent tail estimate is unchanged by this bounded correction. No final
failure-class elimination or merge-readiness claim is made yet.

Final verification logs (local, not merge artifacts):
`/tmp/agent-g-2321-activation-startup-matrix.log`,
`/tmp/agent-g-2321-managed-transport-final.log`,
`/tmp/agent-g-2321-owner-mcp-final.log`,
`/tmp/agent-g-2321-offline-backing-typed-final.log`,
`/tmp/agent-g-2321-activation-integrated.log` (failed run),
`/tmp/agent-g-2321-census-guards-final.log`,
`/tmp/agent-g-2321-release-guard-final.log`,
`/tmp/agent-g-2321-activation-integrated-final.log` (canceled while queued), and
`/tmp/agent-g-2321-activation-live.log` (fresh dual-store live PASS),
`/tmp/agent-g-2321-scaffold-live-final.log` (both generated archetypes PASS),
`/tmp/agent-g-2321-activation-full-verified.log` (full profile PASS at eace6c528).
