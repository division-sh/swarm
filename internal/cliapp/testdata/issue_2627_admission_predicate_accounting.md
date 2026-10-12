# #2627 Startup Predicate Owner, Caller And Execution Accounting

The hosted d0ea0a114 census failure is caused by this PR's changed
`internal/runtime/llm/workspace_mcp_admission.go::compareWorkspaceMCPDefinitions`.
The existing AST/body generator refreshes exactly that fingerprint, retaining
its A11/A18/A25 and P09/P15/P21 classification. It does not change a surface,
registry check, omission policy or negative ratchet. These numbers refer to the
[original #2286 admission audit](https://github.com/division-sh/swarm/issues/2286#issuecomment-5970334585),
not #2627's A/M/G/O/S/Q/W manifestation numbers. Hash agreement alone earns no
execution credit.

## Owner And Exhaustive Production Callers

The syntax owner remains `llm.ValidateProviderToolSchema` / list admission.
`compareWorkspaceMCPDefinitions` owns target-observed definition comparison:
non-null object/name/duplicate and exact identity first; recognized provider
wire syntax second; no confirmation on refusal. Its complete direct caller set
is `validatePlannedWorkspaceMCPDefinitions` and `validateWorkspaceMCPDefinitions`.
Neither schema hashes nor prior immutable source admission replace that check.

| Consumer chain | Authority preserved | Named execution proof |
| --- | --- | --- |
| `validatePlannedWorkspaceMCPDefinitions` -> `probeWorkspaceMCP` managed arm | Only planned visible/callable MCP tools; earlier object/name/hash admission; target-local observation is not model dispatch. | `TestProviderSchemaWorkspaceObservation/*/managed_startup_activation_turn` explicitly passes matching malformed wire through this wrapper; old changed/missing/foreign/duplicate controls remain. |
| `validateWorkspaceMCPDefinitions` -> `probeWorkspaceMCP` fork arm | Frozen sandbox definitions and exact fork identity, never fabricated managed authority or the live source executor. | `TestProviderSchemaWorkspaceObservation/*/fork_chat`, `TestWorkspaceMCPDefinitionAdmissionRejectsEveryChangedPlannedDefinition`, actual frozen TCP catalog `TestProviderSchemaGatewayAdmission/fork/frozen`; selected/fork storage proofs remain separately credited. |
| `ObserveWorkspaceGateway` -> normal `Runtime.observeActivationWorkspaceGateway` | Exact unpublished lifecycle token, admitted runtime/process grant, actor plan and leased tool catalog precede the probe. `newRuntime` supplies this callback to Manager. | `TestProviderSchemaWorkspaceTargetRefusal/activation_observation/*` reaches the actual native host worker/HTTP comparison and unregisters once; Manager/lifecycle authority is not fabricated by this component test. Existing exact runless/live-run startup controls protect the outer composition. |
| `ObserveWorkspaceGateway` -> `PreparedSelectedForkProviderCatalog.ObserveActivationGateway` -> selected materialization | Frozen catalog/configuration revision, prepared actor plan, exact process possession and preparation coordinates. | `TestPreparedProviderPreflightUsesRunlessPlansWithoutManager`, `TestPreparedProviderPreflightRejectsChangedGatewayCatalog`, `TestPreparedProviderPreflightRejectsCensusBeforeProviderResolution`, `TestPreparedProviderPreflightFailureDoesNotReturnReceipts`, `TestPreparedSelectedForkStartupSurfaceNeverProjectsLiveActor`. These use the real catalog/context/HTTP protocol and controlled provider/effect ports, not paid Claude. |
| Mock `ProbeStartupVisibleToolSurface` -> `probeWorkspaceMCP` | Startup-probe surface is mandatory; host target resolution and real HTTP observation precede effective provider evidence. | `TestProviderSchemaWorkspaceTargetRefusal/mock_startup/*` calls the actual startup method with malformed matching schemas, gets no response/evidence and unregisters once. Registration is a fixture, not durable authority or Docker proof. |
| Claude `ProbeStartupVisibleToolSurface` -> Claude `probeWorkspaceMCP` adapter -> common probe | Existing startup effect/session/probe target and bridge prerequisites remain; comparison occurs before model launch. | Existing `TestStartupProbeConsumesSessionContractWithoutExecutionFrame`, `TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_UsesUUIDSessionID`, exact definition controls and every-launch refusal. Provider command/Docker selection are protocol fixtures, not genuine provider/Docker execution. |
| Mock continued session -> common probe; Claude continued turn -> adapter/common probe | Actual executing actor/model/performance/frame/session authority stays earlier. No cached successful observation authorizes a later launch. | Mock earlier-gate controls and compiled host MCP; `TestClaudeEveryLaunchRechecksGatewayBeforeModelInvocation` first/resumed/tool-result refuses before model after real initial probe. |

Repo-wide callers of `ObserveWorkspaceGateway`, `probeWorkspaceMCP` and both
comparison wrappers were checked. There is no direct verify caller and no second
provider-subset interpreter. Final-list owner corrections and recognized wire
syntax now follow the independently amended gate6119665740; unknown vocabulary,
schema dialect and immutable-type redesign remain outside this bounded class.

## Refreshed Inherited Execution Rows

| Row / family | Current owner/caller disposition | Execution witness and exact limits |
| --- | --- | --- |
| A11 / P09: concrete profile/model selection for declared/scoped/imported/retained actors | Unchanged selection and model-admission owners run before delivered definition comparison. The new schema refusal never resolves another actor/profile or substitutes fallback fulfillment. | `TestDeclaredAgentModelAdmissionUsesConfiguredProfileWithoutRuntimeOrCredentials` covers all four live API/CLI profiles and wrong-profile refusal; public verify model alias/selected profile controls; `TestValidateClaudeStartupConfigForActiveAgents_RequiresFullCLIBindingForRecoveredManagerAgent` and gateway requirement for recovered Manager actor. Mock startup's exact surface and the planned wrapper are separately tested. No claim of paid/live provider readiness from verify. |
| A18 / P15: configured gateway/provider/bridge requirements | Config/binding and credential laws remain unchanged; the exact target-observed schema now also consumes canonical recognized-wire admission. Verify can inspect prerequisites, not mint a runtime-owned binding/token. | Missing/recovered binding and nonexecuting workspace controls; `TestWorkspaceMCPDisabledBridgeCannotAuthorizePlannedTools`; invalid matching schema through both wrappers and actual native host activation/Mock startup probe; `TestValidateManagedProviderPreflightFailsClosedWhenLiveClaudeRuntimeLacksStartupProbe`. Invalid schema produces no confirmed surface/response, no model launch. |
| A25 / P15,P21: delivered capability/handshake and execution authority | Process-owned preflight and retained/selected activation remain execution obligations, not a pure verifier callback. Only the existing target worker/registry may earn list evidence; malformed wire never confirms. | Normal runless-blueprint/live-run separation and executable-fork refusal; selected preparation/context/catalog controls; actual native HTTP observation negatives and compiled public host MCP positive. `TestVerifyDeploymentManagedCatalogConsumesLoadedSourceWithoutExecution` reports live_readiness=not_evaluated and creates no .swarm runtime state. Listener observation releases its bindings and retains execution obligations; public both-store schema inspection remains unchanged/read-only. A passed static catalog never claims handshake settlement. |

These witnesses cover the changed owner/callers; they do not rename the
historical P09/P15/P21 plan into fictitious new test roots. Preserve the actual
frozen/source/credential/process gates and all earlier refusal precedence.

## Regeneration And Qualification

Regenerate through the existing `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1`
`TestVerifyBootStartupPredicateCensus` owner only. The generated JSON diff is one
body_sha256 cell: old3911b7f4 -> new599d7c38; owner/classification/membership stay
unchanged. Final qualification must run with update mode absent and include the
census plus all three `TestAdmissionPredicateRatchetRejects*` hostile controls.

The original local and hosted census reds remain failed receipts. Initial owner/
caller baseline controls passed race3 before the cycle-1 owner correction; final
recognized-wire, observer, declared/retained/prepared, read-only verify and
compiled-host controls are rerun on the integrated repair. Receipt hashes and
actual execution source/version belong in the PR's proof-audit addendum, not
implicitly inferred from a generated fingerprint. No assertion/deadline/guard
waiver, schema compatibility path or registry-only repair.
