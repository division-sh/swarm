# Pre-Implementation Coverage Audit: #2555

Agent: agent-g. Phase: implementation under the approved independent gate.
Audited origin/master: `76fbddd6dacec435e7807de14562be03b2a47e1d`.
The production implementation is committed locally in this branch; no complete
class closure, paid provider invocation, or Telegram call is claimed. The original
matrix below remains a proof plan, not a completed proof audit. The implementation
stop-condition addendum at the end retains the counterexamples and their
superseding bounded dispositions. Fork-chat coding is approved; compensation
and final closure remain dependent on E's #2525 merge and crossed proof.
The M09 lost-client proof subsequently exposed an unjoined native Docker
worker; reviewer-g approved its bounded existing-owner repair in
https://github.com/division-sh/swarm/issues/2555#issuecomment-5978927926.
Its counterexample remains recorded below; approval is not repair proof.

Independent gate: **approved**, complete chosen class, one PR, not a first
slice. Binding conditions and full/full qualification:
https://github.com/division-sh/swarm/issues/2555#issuecomment-5976477595
The gate ratifies the workspace-only test selector, target-native artifact
provisioning, and provenance-controlled Linux Docker listener default. It
requires exact definition mismatches to remain distinct from transport/auth
unavailability and forbids parent redispatch of mock child effects.

## Binding governing context

The full issue body/thread, field reproduction, and both dispositions were
read. The latest user-ratified disposition resolves the three pre-analysis
questions and supersedes the original mock-under-serve acceptance:

- Four-part, one-PR ruling:
  https://github.com/division-sh/swarm/issues/2555#issuecomment-5976078034
- Command, workspace, doctor, and pre-dispatch corrections:
  https://github.com/division-sh/swarm/issues/2555#issuecomment-5976230920
- Linux field reproduction, not a new probe performed by G:
  https://github.com/division-sh/swarm/issues/2549#issuecomment-5972937872
- Banked paid-doctor contract, remaining separately open:
  https://github.com/division-sh/swarm/issues/1779#issuecomment-4948686334

IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md were re-read. Exact governing
`platform-spec.yaml` references at the audited base:

| Reference | Binding meaning and planned correction |
| --- | --- |
| `engine.process_execution_posture` | Public serve/dev remains live; public test remains MockOnly. No mock serve switch. |
| `engine.agent_session_management.llm_provider_selection_config_authority`, including `mock_agent_runtime` (7370) | Command-owned selection, captured performance bytes/digest, pinned CPython-WASI/Wasmtime and bounds remain authoritative. Retire mock `in_process` transport and local tool-dispatch claims. |
| `test_specification.compiled_process_full_lifecycle_profile` | T owns fresh public mock execution; H owns retained compiled lifecycle execution; L owns provisioned live-provider credit. Host mocks remain Docker-free; selected Docker workspaces require Docker. |
| `cli_specification.foundations.local_cli_test_gateway_startup` (23846) | Bind before constructing runtime; public test owns private resources and never borrows deployment connections, stores, credentials or listeners. Add an explicit workspace-only test selection, not ambient deployment-resource import. |
| `cli_specification.foundations.local_tool_gateway_binding` (23887) | Runtime-owned listener/token provenance, host/workspace endpoints, normal and selected-fork lifetime. No env-selected URL/token or fabricated grant. |
| `cli_specification.foundations.local_claude_cli_preflight_admission` (24004) | Static dependency checks stay distinct from a free target-local gateway observation and paid credential validation. |
| `cli_specification.command_catalog.doctor` (26215) | Default source-free/static diagnostics remain non-mutating with respect to retained state. An explicit free gateway probe owns and joins its temporary resources; it does not validate credentials. |
| `cli_specification.command_catalog.serve.listener_topology_v2_1` (26682) | API loopback and independent listener precedence remain. Correct the MCP default for Linux Docker and document authenticated non-loopback/hardened-host topology. Explicit listener choices are never silently rewritten. |
| `managed_agent_capability_surface` (39117), startup, provider-turn and enforcement children | Exact planned and delivered bindings must agree before execution. Missing planned MCP evidence is a refusal, not merely a narrower success. Preserve exact inventory/hash, attempt, actor, call-occurrence and narrowing rules. |
| `managed_external_effect_authority`, completion/launch/settlement and directive-origin contracts | No fake delivery claim. A pre-model refusal records no provider invocation/uncertainty; an actual dispatched failure retains truthful uncertainty and committed evidence. |

Spec changes must land in the implementation PR. Issue prose is not a merge
artifact. Paid `doctor --probe`, live-provider health, new native capabilities,
public mock serve, and post-boot bundle replacement are not authorized.

## Category, class chain and closure boundary

- Category: failure-class / semantic-drift / supported-platform parity.
- Observed symptom: a Linux Docker agent has no usable MCP tools, yet doctor,
  startup and a no-call turn can report success/delivered without an emit.
- Exact concepts changed: planned MCP availability as execution admission;
  execution-target-local transport observation; bound-listener/workspace
  projection; model-only substitution; truthful diagnostic proof credit.
- Chosen working failure class: **managed Swarm-MCP execution can proceed
  without proving the planned gateway/tool surface from its actual workspace,
  while mock execution bypasses that transport entirely**. Includes every
  mock construction/continuation, Claude startup/launch, and free diagnostic
  consumer of that same boundary, not only Linux name resolution.
- Immediate parent: execution-target/provider transport and surface parity.
- Broadest plausible parent: runtime execution-boundary policy drift, mapped
  by `transport_policy_and_surface_parity`.
- Issue framing: original platform symptom is too narrow; body repaired to
  the complete ruled class and corrected command acceptance before coding.
- The failing validator/helper is an entry point, not the audit boundary.
- Intended closure: failure class eliminated for this chosen class; broader
  transport parent remains open. This PR aims to eliminate the chosen class
  entirely, including the host mock bypass, not establish a second path.
- Feasibility: one PR is credible using existing selection, workspace,
  capability, MCP, lifecycle and settlement owners. Fixing only the endpoint
  leaves startup's host probe, mock local dispatch and later launches live;
  those paths are all included. No compatibility readers/fallbacks are kept.

## Concrete composition design to gate

1. Keep `Surface` and its planned bindings as semantic authority. Strengthen
   existing CLI admission to reject unavailable **planned** MCP bindings,
   without making denied/unplanned/empty native-only plans mandatory. Preserve
   independent mismatch, invalid inventory and exact native-set refusals.
2. Extend the existing target execution and MCP binding consumers with one
   bounded pre-dispatch transport operation. It executes in the selected host
   or Docker target and uses that target's URL, boot token, and exact turn or
   startup-probe context. It performs authenticated initialize/list, checks
   canonical definition identity and planned availability, and joins on exit.
   Safe startup calls retain the existing explicit validation-only contract;
   no business emit is permitted by a reachability probe.
3. Run this observation once per activation attempt, before executable agent
   lifecycle admission, and before **every actual launch**. Do not reuse an
   old readiness grant as current network evidence. A terminal pre-dispatch
   `workspace_gateway_unreachable` includes the redacted target URL, target
   kind and observed status/cause; no token, credential or context secret.
   Missing planned provider evidence from init also refuses startup. A
   disconnect after dispatch is not retroactively called pre-model refusal.
4. Linux Docker container creation adds
   `host.docker.internal:host-gateway`. For an otherwise-default Docker MCP
   listener on Linux, use a reachable authenticated bind (proposed minimal
   choice: wildcard MCP bind, with advertised host-gateway URL). Do not infer
   the alias from `mas_default`'s gateway or hard-code a subnet: Docker's
   host-gateway address can differ. Preserve explicit flags/config, host-only
   loopback, Docker Desktop selection and API loopback. Document exposure and
   token authentication; do not edit the firewall. An explicitly unreachable
   listener remains a typed failure. Hardened hosts may bind serve's explicit
   workspace-network address; the projected endpoint must match that bind.
5. Mock provider selection remains `mock`, but its one descriptor uses the
   existing subprocess/CLI transport kind, not `in_process`. `swarm` has a
   bounded internal mock-agent mode: captured Python source/digest and limits
   arrive through a private bounded input; the pinned interpreter evaluates
   them inside the selected target. Native host Python is never a fallback.
   The child makes real gateway HTTP calls with existing occurrence metadata.
   Parent settlement consumes results and existing continuation/output
   authorities; it never re-executes those calls in `Conversation`'s local
   executor. API-provider function-call execution is a different binding
   concept, not a mock exception. Internal mode is not a public backend switch.
6. Target-compatible worker provisioning belongs to workspace build/target
   admission, not a runtime compiler or another agent framework. On native
   Linux Docker, provision the current Linux swarm artifact read-only. An
   image/explicit artifact used from a different host OS must supply the
   compatible swarm mock mode and pass a bounded version/ABI/digest check;
   never mount a Darwin executable and claim Linux proof. Extend the existing
   workspace-build materialization with an explicit compatible-artifact input
   if needed. Missing/incompatible bytes fail before execution with remediation.
   The gate must ratify this provisioning port; no implicit downloads, new
   interpreter, vendoring, or broad release-system redesign is authorized.
7. Public test currently ignores deployment workspace configuration. Add only
   explicit `--workspace-backend host|docker` (and explicit image selection if
   required by that Docker selection), carried by `TestSessionRequest` to the
   existing private composition. Default host behavior and deployment-resource
   isolation stay intact. This small public port is submitted for gate
   ratification; importing all of `swarm.yaml` into private execution is not
   an alternative. Public test still uses its private SQLite store; PostgreSQL
   credit comes from the retained compiled H composition, not a fake public
   store selector.
8. Default doctor says gateway was **not checked from inside a container**.
   An explicit free gateway-probe mode uses existing target/network/binding
   owners with an owned temporary listener/container, performs no model call,
   touches no retained DB, and joins/disposes resources. It reports target
   reachability separately from CLI installation and credential presence.
   Successful infrastructure ping alone cannot certify an actor's planned
   tool inventory. Paid credential validity remains open in #1779.
9. Every composition constructing a mock runtime must provide its own exact
   binding. Public T is single-context. Normal H/recovery/reset and selected
   forks consume their existing runtime/gateway owners. If an internal
   multi-context mock composition is exercised, use separately owned private
   endpoints through the same existing gateway construction/lifetime, never
   the primary context's registry. Keep public multi-context live Claude
   explicitly unsupported; do not add a general multiplexing registry.

These are explicit proposed ports, not implemented facts. Reviewer-g must
confirm the workspace-only public selector, native-artifact provisioning and
reachable default within the one-PR gate. An incompatible product/spec ruling
or need for a general packaging/multi-context framework is a stop condition.

## Full execution path and gates

The failed turn is reachable only after all earlier stages succeed. Each row
is classified explicitly; no endpoint-only audit or "same seam" credit.

| Order | Stage/gate | Classification and proof |
| --- | --- | --- |
| 1 | CLI purpose, source/pack admission, exact scoped actor and captured mock module | Different semantic concept: selection/profile and source-admission tests preserve live/MockOnly, alias, missing module and byte/digest rules; M01/M03/Q04. |
| 2 | Structural verify versus deployment/native/credential readiness | Different semantic concept: verify/describe remain structural; existing #2321 separation tests and D01/D06/Q04 preserve no live execution during readers. |
| 3 | Workspace backend decision and immutable projection/target | Same chosen class for target selection/provisioning; N01-N07/M02/M11/Q01. Source-mount/path authority remains separately proved by workspace tests. |
| 4 | Actual listener bind, sealed binding and target URL/token | Same chosen class: N01-N07/A01/D02. API authentication is a different security concept, with N05/N06 and existing binding tests. |
| 5 | Static preflight and target-local startup/activation observation | Same chosen class: A01-A06/D01-D05/L01-L04. Credential validity is explicitly split, #1779 paid tail; ingress dormancy is separately F-owned #2319. |
| 6 | Attempt-owned registration, route/timer attachment, readiness and recovery release | Same chosen class for mandatory observation before executable admission; L01-L05. Attempt phases/compensation are E-owned #2496 and remain exact, not recreated. |
| 7 | Persisted delivery/directive or selected-fork origin, session and completion acquisition | Different semantic concept with proof: existing origin/launch/settlement matrices plus A07/M08/L06 ensure no forged delivery or foreign lifecycle authority. |
| 8 | Exact per-launch target-local probe before child/provider dispatch | Same chosen class: A02/A04/A06-A09/M02/M09/L03-L07. First/resume/tool-result/directive/fork and fork-chat are each executed, not assumed. |
| 9 | Native provider init inventory and MCP delivered definitions | Same chosen class for missing planned MCP; A01-A05. Native mismatch/invalid inventory are distinct existing security rules explicitly retained by A03/A05. |
| 10 | Tool call occurrence, MCP authentication/catalog lease and executor | Same chosen class for mock transport consumption; M04-M08. Existing capability/effect/catalog owners remain proof-bearing; replay/foreign/unplanned calls refuse. |
| 11 | Tool result, output/continuation commit, session/usage and delivery settlement | Different semantic concept: M06-M10/L06/Q01-Q03 preserve exact committed outcomes and no parent redispatch. Pre-model refusal versus post-launch uncertainty is part of chosen admission boundary, A07/M09. |
| 12 | Restart/reset/fork reattachment and next turn | Same chosen class for renewed target proof and binding lifetime: L03-L07/Q02/Q03; immutable run/source/fork authority is preserved independently. |

## Canonical owners and exhaustive systematic-consumption census

The owners below are actual semantic owners, not merely the first local files
encountered. Status meanings: **already consumes**, **moved in this work**,
**different concept with proof**, **split/tracked separately**. Census includes
production callers and existing tests that construct a runtime directly.

| Owner | Consumer/seam | Status and exact evidence/proof obligation |
| --- | --- | --- |
| `core/managedcapabilities.Surface` and `llm/capability_surface.go` | CLI native/MCP plan and evidence (`managedCapabilityPlanForActor`, `observeCLIResponse`, `ValidateCLIProviderCapabilitySurface`) | Moved in this work: missing planned MCP admission; A01-A05, preserve narrowing/hash checks. |
| Same | Mock `ObserveMockRuntimeCapabilitySurface`, local definition/provider contract | Moved in this work: delete local-runtime transport proof; M01/M04/M05 consume actual MCP evidence plus immutable interpreter input. |
| Same | Anthropic API, OpenAI-compatible, Responses definitions/calls | Different concept with proof: `BindingAPIDefinition`, provider contracts and API schema/committed-phase controls; Q05. No local execution removal for real API function-call adapters. |
| Same | Denied/empty/generated-current-entity-only startup surfaces | Already consumes plan; A03 confirms no fabricated missing-tool requirement when no concrete admitted MCP binding exists. |
| `toolgateway.Binding` + actual listener projection | `serveapp.createServeToolGatewayBinding`, private test, cold boot, retained reconstruction and dev reset | Moved in this work: target-correct projection and reachable conditional default, N01-N07/L03/Q01. Seal and per-boot token remain authoritative. |
| Same | `runforkexecution.startSelectedContractAgentRuntimeGateway` and prepared selected catalog | Already consumes dedicated fork owner; moved target observation/mock execution, L06. No borrowing normal gateway or predecessor plan. |
| Same | Fork-chat runtime construction and sandbox MCP transport | Moved in this work: target-local launch and mock HTTP execution; L07/M08. Preserve sandbox versus executable-fork distinctions. |
| Existing workspace resolution + fork-chat sandbox authority | Mock model/tool targets and `DockerManager.ResolveClaudeWorkspace` fork-state base | Moved in this work under ruling 5978140439: consume `ResolveForkChatWorkspace`, selected-store current-authority validation and exact disposable sandbox projection. Snapshot actor remains provenance; no source container/data/provider backing is inherited. Host-only proof does not qualify Docker. L07/M08 require both-store host and real-Docker continuation, noninterference and refusal/cleanup. |
| Existing workspace backend decision + selected provider contract | `cliapp.workspaceAdmittedForkChatExecutor.ExecuteForkChat` | Moved in this work: consume exact live-Claude provider identity rather than equating all CLI transports with Claude. M12/L07 require host mock public chat on both stores plus unchanged live-Claude host/none refusal. This is the already-gated fork-chat entrance, not a new class or permission for live host Claude. |
| Same | Secondary runtime contexts | Current live-Claude multi-context refusal remains; any mock runtime construction must get an exact owned private endpoint, not primary fallback; L05. Framework requirement stops coding. |
| Same | Retired `SWARM_TOOL_GATEWAY_*`, `SWARM_CLAUDE_USE_MCP` | Already fail closed for live Claude; M05/A04 prove no mock reintroduction or transport-disable/local fallback. |
| Existing workspace lifecycle/`ExecutionTarget` | `DockerManager.EnsureContainerRunningWithIdentity`, source/system/flow/agent scoped containers, reused identity inspection | Moved in this work: Linux add-host and worker admission at shared create/adopt boundary; N01/N02/N07/M11. Foreign/partial containers never silently adopted. |
| Same | Host manager, isolated host roots and workspace backend classifier | Already owns host target; moved mock child into it without Docker requirement, M02/N03/M12. Preserve native/exec safety reasons. |
| Same | `CheckWorkspaceCLICommandAvailable` and `workspace build` | Moved in this work for explicit target-native mock artifact proof; installation is not network evidence, M11/D03. Existing CLI version checks retained. |
| `selection.ResolveAgentExecutionSelection`, `AgentRuntimeSet`, `RuntimeFactory` | Normal actors, dynamically spawned/reconfigured actors, templates and persisted executable adoption | Moved in this work: one mock subprocess descriptor and mandatory workspace/binding dependencies, M01/L01-L04. No second selector or credential-as-mock heuristic. |
| Same | Selected-fork catalog/preparation/materialization and fork-chat | Moved in this work: consume frozen descriptor and exact target; L06/L07. No reselection of persisted work. |
| `runfork.SelectedForkPreparedActor.RequiresProbe` and preparation/surface validators | Prepared selected actor receipt census, `SurfaceIDs`, binding validation and materialization | Moved in this work: consume the selected descriptor's subprocess transport for live Claude and mock, retaining exact preparation receipt and integrity checks; L06 positive public fork on both stores plus missing/foreign-receipt controls. This is the existing selected preparation consumer, not a new owner or authority. |
| Same | Direct `NewMockRuntime` test constructors and injected execution fault seams | Moved in this work: tests inject the same bounded process/transport dependencies or test only pre-launch lifecycle operations. No production default local executor retained; M13/Q05 deletion census. |
| `MockRuntime`, captured `mockperformance`, `pythonmodule` | First completion, tool-result round, frame validation, fuel/memory/output limits | Moved in this work: evaluate immutable captured bytes inside target-native swarm child; M02/M03/M06/M10/M11. Pinned interpreter remains the same owner. |
| Same + existing completion/conversation owner | `executeMockCompletionWithExecutor`, `Conversation.executeToolResponse` and output/continuation consumption | Moved in this work: remove mock direct dispatch while preserving consumed continuation, logical call identity and terminal output; M04/M06/M07/M09. No parent double execution. |
| `llm.BuildMCPHTTPBinding`, MCP context registry/gateway | Target endpoint selection, HTTP initialize/list/call, per-boot/turn tokens and inherited catalog lease | Already owns auth/effects/occurrences; moved all mock producers and target-local probes to it; A02/M04-M09. No second token registry, occurrence ledger or HTTP allowlist. |
| Same | External MCP HTTP/stdio provider client | Different concept with proof: `mcp.Client` strict untrusted provider boundary and external credential tests, Q05; not the internal Swarm tool gateway, no new external-effect permission for mocks. |
| Existing lifecycle replacement and E's attempt-owned attachment | Fresh spawn, persisted executable adoption, readiness/reconcile, runless preparation and restart | Moved in this work: composed admission before runnable replacement and phase release, L01-L04. Lifecycle-only teardown remains non-executable, L04. Probe success cannot mark ready itself. |
| Same | Complete-source generation restamp and failed/abandoned attempt replacement | Already E-owned semantic transitions; L02 checks no second attempt on valid restamp, failure is joined through existing compensation before fresh attempt. |
| Existing CLI child launch/completion owner | Startup process, first/resumed/tool-result managed launch, directive, selected-fork and fork-chat | Moved in this work: per-launch target observation before actual dispatch; A06-A09/L06/L07. Existing streaming/buffered starts both included. |
| Existing effect/session/delivery settlement | No-dispatch refusal, actual dispatched failure, committed tool response and recovery | Already canonical; A07/M06/M07/M09/Q02 prove no uncertainty for pre-model refusal and no retry/replay of sent effects. |
| Local-preflight/report and serve presenter | Default/source-free doctor, JSON/text/target/schema modes, free gateway probe, boot diagnostics | Moved in this work: honest separate static/target/provider credit; D01-D06/N06. No DB or paid model access for static/free doctor. |
| Compiled T/H/L harness and Ubuntu image smoke | Public private host/Docker test, retained H both stores, dev-live refusal, Linux image conformance | Moved in this work: Q01-Q06. Preserve credential-free host golden/lifecycle proof and no mock-under-serve credit. |
| F's credential/onboarding/currentness owners | Serve ingress dormancy, provider activation and recovery publication | Different concept with proof: #2319 crossed startup controls Q04/L03; shared-file integration, not permission to bypass credentials. |

### Old paths now invalid or removal candidates

- Mock `TransportMock = in_process`, `ProviderTransportInProcess` mock contract
  and persisted descriptor expectations; remove dead enum branches only after
  full usage census, not through a dual reader. Old selected stores unsupported.
- `BindingLocalRuntime` / `mock_interpreter_input_delivered` as *tool transport*
  evidence and mock performance returning calls to direct local tool dispatch.
  Keep immutable interpreter digest evidence, but it cannot prove MCP delivery.
- Mock factory construction without workspace, runtime-owned gateway and child
  execution dependencies; test-only injected parent interpreters cannot remain
  a production escape hatch.
- Startup's host-side HTTP list/call as proof of Docker reachability; a host
  control can remain diagnostic but never confer target-local admission credit.
- Permissive planned-MCP-unavailable validation and reuse of a prior activation
  observation to authorize a new launch.
- Linux implicit hostname projection without host mapping/reachable listener;
  endpoint rewriting, ambient gateway tokens and private-context fallback.
- Doctor's static gateway-green implication and fixture comments asserting
  mocks always avoid MCP. Docker-free still applies to host targets.
- Agent-free Linux smoke as agent gateway conformance; retain its own credit,
  add the discriminating target-local agent transport journey.

## Manifestation matrix and exact planned proof

Prefixes locate proposed new tests: A = `internal/runtime/llm` plus startup;
N = workspace/serve listener; M = mock/transport/selection; D = cliapp/doctor;
L = manager/serve/runforkexecution lifecycle; Q = compiled supported surfaces.
Parameterized rows explicitly name every branch required, not aggregate
"same corrected path" assertions. Store-bearing proofs use SQLite **and**
PostgreSQL; skips cannot earn either store's credit.

| ID | Known manifestation | Named planned proof and decisive oracle |
| --- | --- | --- |
| A01 | Planned MCP provider/tool evidence unavailable is tolerated | `TestPlannedMCPAvailabilityAdmissionMatrix`: failed/missing server, absent one/all planned names -> typed refusal; expected/denied/empty controls; immutable planned bindings unchanged. |
| A02 | Host succeeds while execution target cannot connect | `TestGatewayObservationUsesExecutionTarget`: real child/HTTP endpoint, container-only refusal with host success control; zero model launches, exact redacted URL and target kind. |
| A03 | Denied/empty/static-generated surfaces could become false refusals | `TestGatewayAvailabilityRequiresOnlyConcreteAdmittedBindings`: native-only, zero tools, fully denied, generated-current-only versus one concrete MCP tool. |
| A04 | Auth/DNS/connect/status/malformed list and disable env drift | `TestGatewayPreDispatchRefusalMatrix`: bad boot/turn token, unresolved name, refused socket, timeout, 5xx, malformed inventory/hash and transport-disabled config; no local fallback/provider launch. |
| A05 | Stricter MCP check could mask native/unknown inventory errors | Extend exact native and malformed/missing/explicit-empty inventory tests; preserve original error precedence and mismatch refusal with reachable gateway. |
| A06 | Only first turn receives the check | `TestClaudeLaunchGatewayCheckMatrix`: first, resumed session, tool-result successor, streaming and buffered launch, each with unreachable-after-prior-success; launch sentinel stays zero. |
| A07 | Pre-launch rejection becomes outcome_uncertain/retry | `TestGatewayRefusalSettlementBothStores`: delivery and directive origins, failed pre-model operation, zero invocation/turn/spend/output, no delivered outcome; actual-started failure uncertainty control. |
| A08 | Selected execution accidentally asks for ordinary delivery authority | `TestSelectedGatewayPreDispatchAuthority`: prepared runless startup and executable selected fork carry exact existing variants; no source-run/normal claim substitute. |
| A09 | Fork-chat launch skips target check | `TestForkChatGatewayLaunchRefusal`: sandbox context, real target child sentinel, typed pre-model refusal before session/response acknowledgement; managed-turn token rejected. |
| N01 | Native Linux Docker has no host-gateway alias | `TestLinuxWorkspaceGatewayHostMapping`: shared create/adopt arguments plus real Ubuntu container lookup/connect; verify exact mapping, not only mock argv. |
| N02 | Correct hostname still reaches loopback-only socket | `TestLinuxDockerDefaultGatewayProjection`: default Docker MCP bind -> successful authenticated container initialize/list; explicit loopback negative, no inferred subnet. |
| N03 | Docker fix changes host-only/macOS behavior | `TestGatewayListenerTargetDefaultMatrix`: host loopback, Linux Docker reachable default, Desktop projection, explicit IPv4/IPv6/wildcard; independent API address unchanged. |
| N04 | Explicit address or Docker alias override is hidden | `TestGatewayExplicitListenerAuthority`: flag > config > conditional default; no silent rewrite; observed projected URL follows actual listener and exact target. |
| N05 | New non-loopback MCP listener leaks unauthenticated execution | `TestReachableGatewayAuthentication`: missing/wrong boot and context tokens refuse /mcp and /tools, correct token/exact actor succeeds; API remains numeric loopback with its existing auth rules. |
| N06 | Hardened host transport is falsely reported healthy | `TestHardenedWorkspaceGatewayTopology`: explicitly reachable serve-network endpoint positive and network-disabled/unreachable control; doctor/boot typed status and documented topology agree. |
| N07 | Reused/partial/foreign container misses new immutable target inputs | Extend workspace source/projection lifecycle tests: owned reuse confirms required mapping/artifact, incompatible/foreign identity refuses, failed create/removal retains joined cleanup and no runnable child. |
| M01 | Mock descriptor still selects in-process transport | `TestMockWorkspaceTransportSelection`: all-mocked, mixed, normal/dynamic/persisted/selected/fork-chat construction resolves one mock provider/subprocess descriptor; live/source-double inert controls. |
| M02 | Public host mock now unnecessarily needs Docker/Claude/credentials | `TestHostMockAgentUsesRealGateway`: compiled host child with absent Docker/Claude/Python/credential commands; real HTTP tool call and settled emit, no parent local dispatch. |
| M03 | Container execution rereads mutable module or uses host Python | `TestMockChildPinnedInputBoundary`: captured module bytes/digest, changed source path, wrong digest, invalid ABI, imports, fuel/memory/output limits, cancellation; exact pinned interpreter only. |
| M04 | Docker mock performs tool call in parent | `TestDockerMockAgentGatewayEmission`: container process evidence + real HTTP initialize/list/call + store emit identity; parent executor trap fails if invoked outside gateway. |
| M05 | Mock capability plan/list definition uses a local waiver | `TestMockMCPDefinitionAndAvailabilityParity`: exact schema/description hashes, planned names, unavailable/foreign/unplanned denial, disabled env cannot select local runtime. |
| M06 | Child tool call is executed again by Conversation | `TestMockGatewayToolRoundCommitBothStores`: terminal emit and nonterminal result/next frame; exactly one call/effect/output, exact consumed continuation and usage, no parent redispatch. |
| M07 | Retry/restart repeats a committed child effect | `TestMockGatewayOccurrenceRecoveryBothStores`: lose response after committed tools/call, reopen/restore and continue via existing receipt/occurrence; one durable effect, no minted second call authority. |
| M08 | Mock token crosses sibling run/fork/sandbox | `TestMockGatewayAuthorityIsolationBothStores`: same slug foreign run, selected fork/source and fork-chat sandbox cross-use refused before executor; exact legitimate origins succeed. |
| M09 | Target cancellation/leak or missing gateway triggers fallback | `TestMockChildFailureAndJoin`: prelaunch unavailable typed/no model, cancellation during interpreter/HTTP joins child and request, post-effect failure preserves committed evidence; no detached cleanup or synthetic success. `TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest` passes, but `TestWorkerRealDockerLostClientJoinsGatewayRequest` fails 3/3: loss of the local Docker client returns while the exact native child and HTTP request remain live. Approved repair must prove request/process absence before return, exact sibling/source noninterference, no replay and retention of both client and cleanup failures. A deadline-only control cannot close this row. |
| M10 | Mock child framing permits unbounded/ambiguous input/results | `TestMockAgentProtocolBounds`: missing/unknown/trailing/duplicate fields, oversized stdout/stderr/input, structured error, malformed results refuse under existing bounds; no secrets logged. |
| M11 | Wrong-OS/stale worker binary is counted as transport proof | `TestWorkspaceMockWorkerArtifactAdmission`: current Linux executable positive; Darwin/missing/wrong ABI/digest/version negative; warm artifact remains exact; no runtime download/build/fallback. |
| M12 | Agent-free/native/exec consumers lose prerequisite safety | Workspace classifier/preflight matrices: agent-free no child, host mock, mixed live Claude, native bash/file IO, exec tools retain their separate permissions/dependencies; no blanket mock waiver. |
| M13 | Test-only direct MockRuntime constructors hide production bypass | `TestMockTransportOwnerCensus` plus existing constructor consumers: no production local-dispatch/parent-interpreter path; legitimate pre-provider rotation/release tests remain bounded owner tests. |
| D01 | Static doctor claims container path was checked | `TestDoctorGatewayProofCredit`: text/JSON/source-free say not checked from inside a container; credentials present != validated; no Docker launch/model/retained DB for static report. |
| D02 | Free doctor probe executes on host instead of target | `TestDoctorFreeGatewayProbeTarget`: real Docker probe proves positive/host-only negative, correct explicit network/mapping and actual endpoint, bounded typed status. |
| D03 | Installed CLI/image is confused with reachable gateway | `TestDoctorGatewayPrerequisiteMatrix`: image/CLI/version/static listener, failed transport, credential presence remain distinct category/findings; original checks retained. |
| D04 | Doctor probe owns temporary resources badly | `TestDoctorGatewayProbeCleanup`: partial listener/container failure, cancellation, concurrent probe, cleanup errors and request join; zero retained state or paid calls. |
| D05 | Doctor target/schema modes acquire execution resources | Extend target/schema-inventory tests: no probe unless explicit free mode, incompatible mode combinations fail before acquisition; paid probe remains unimplemented/open. |
| D06 | Readiness wording causes verify/describe/live-mock confusion | Compiled verify/describe/doctor/test/serve command matrix: structural truth separate, serve source doubles inert, no paid/live claim from public mock success. |
| L01 | Fresh/dynamic admission becomes runnable before check | `TestGatewayProbeFencesActivationAttemptBothStores`: fresh spawn, publish/materialize, persisted executable adopt, reconcile; pause probe, assert no runnable dispatch/phase-ready, fail compensates exact attempt. |
| L02 | Same attempt gets second authority or fresh attempt skips check | E-crossed attachment test: once per attempt, same-plan generation restamp retains attempt, failed joined replacement/new attempt rechecks; stale probe result cannot advance replacement. |
| L03 | Cold/recovered/dev-reset work runs on stale/unpublished gateway | Retained H both-store boot/recovery modes and dev-reset test: binding publication before observation/execution, retained pending work blocked until exact target check, no old token after reconstruction. |
| L04 | Teardown-only adoption or partial failure launches work | Lifecycle-only adoption and activation compensation controls: non-executable adoption skips model/work, failed target owns/joins resources, no guessed running state or second readiness owner. |
| L05 | Secondary mock context borrows primary gateway | Exact two-context internal-composition probe: each mock uses its own endpoint/registry and cannot access other source; public multi-context live Claude still refuses as specified. |
| L06 | Prepared/executable selected-fork gateway path is missed | Both-store supported selected fork execution and recovery: exact catalog/binding/probe/attempt, reachable positive, gateway-loss pre-model refusal, terminal cleanup/restart cannot repeat committed emit. |
| L07 | Fork-chat uses another factory/continuation path | Both-store public conversation fork/chat: host/Docker mock path, normal exact response and later continuation, unavailable target before launch, sandbox restrictions and acknowledgement semantics retained. |
| Q01 | CI image smoke has no agent transport proof | New Ubuntu image job journey: compiled public `swarm test --workspace-backend docker` authored mock emits via actual gateway, public scenario/store assertion; after initial success deliberate unreachable per-turn refusal proves no delivered/no emit and zero model invocation. |
| Q02 | Restart proof is accidentally a fresh private test | Compiled retained H on SQLite/PostgreSQL, graceful and forced interruption: stable causal/session identities, unsettled checkpoint, exact post-restart HTTP call/output cardinality; not credited as public serve or live provider. |
| Q03 | Boot refusal is tested through unsupported mock serve | Compiled **live** `serve --dev` with deliberately unreachable gateway: typed boot refusal before Claude/provider sentinel, no mock under serve; free doctor agrees. This is network/boot proof, not successful paid provider proof. |
| Q04 | Private Docker test imports deployment DB/listener/credentials | Extend `TestPrivateTestIgnoresDeploymentResources`: explicit workspace choice only, invalid selector/image refusal before acquisition, deployment traps/sentinels untouched, default host preserved. Cross F's dormant ingress and E's activation ordering. |
| Q05 | Parent/API/external-MCP/native siblings regress | Existing provider-contract, origin, completion, external MCP parser/auth, toolgateway seal, session/rotation, workspace projection and catalog lease controls; Linux/Darwin target correctness and deletion census. |
| Q06 | Partial focused proof is counted as final qualification | Same-head focused/race + both-store supported matrix, authoritative spec/API checks, generated proof inventory/complexity checks and full `swarm-test` on server2; hosted Ubuntu transport job and exact-head full CI. Reviewer sets CI/Local tier; proposed full/full. |

## Parent probes, watchlist promotion and tracker decision

The mapped node already records transport token parity, provider-visible versus
callable truth, startup versus provider-turn authority, selected-fork gateways,
mock false-green proof, exact definition hashes and per-occurrence evidence.
It directly demands promotion beyond a hostname/validator fix: mock execution,
host-only startup observations and every later launch are absorbed now.

Parent sibling probing inspected actual code, not labels: API adapters deliver
`BindingAPIDefinition`; external MCP uses a separate strict untrusted provider
client; native tools use their provider inventory; paid doctor uses real
credential/provider execution. These are different concepts. Their baseline
controls are named above; no new defect or closure is inferred for them.

- Parent action: absorb every known same-class workspace/MCP bypass in #2555;
  keep the broader transport/security/credential parent explicitly open.
- Tracker-state decision: update #2555 body before coding to corrected
  acceptance/class/ports and this complete matrix; preserve the full historical
  field report. #1779 stays open for paid backend probing, with its free
  gateway half linked here. F's #2319 and E's #2496 stay separate and unchanged.
- Watchlist decision: refine existing `transport_policy_and_surface_parity`
  with #2555/#1779, target-local evidence, mock transport, all launch consumers,
  diagnostics and proof separation. No new issue/node/POTENTIAL_ISSUES entry.
  Repair published on swarm-docs master at
  `b5b89ab78185cee125b5a8fbe3c403b9430ea34b`; YAML and issue mappings validated
  after integrating concurrent docs updates. This is pre-audit tracking,
  not runtime closure or independent gate approval.
- Estimated child tail to close this chosen class after this PR: **zero**, if
  the matrix is proved. Confidence medium before Docker/process implementation.
  Broader parent tail includes paid #1779, external-provider/tool-policy and
  provider-specific lifecycle seams already mapped; not a finite promised
  estimate. Paid doctor is roughly one bounded diagnostic child, medium
  confidence; other parent groups need their own current census.
- Architecture smell: model substitution was coupled to execution transport,
  and canonical unavailable evidence was interpreted as success by admission.
  Better direction: preserve existing canonical owners, substitute only the
  model primitive, and demand exact target observations at their admission
  boundaries. Tracking: **#2555 absorbed repair + existing watchlist**, not a
  second framework issue. Broader phase ownership remains E/F's tracked work.
  Rough effort: one multi-day implementation/proof pass, high regression ROI
  because ordinary mocks finally test the transport that live Claude needs.

## Coordination and implementation ordering

F #2319 shared files: `internal/serveapp/main.go`, `internal/runtime/runtime.go`,
`internal/serveapp/main_runtime_test.go`,
`internal/serveapp/serve_lifecycle_presentation.go`, `platform-spec.yaml`, and
proof-plan/generated inventory. Its separate credential/currentness and ingress
publication boundaries are preserved. Coordination was posted on #2319; its
worktree was inspected read-only and no merge dependency is manufactured.

E #2496 read-only census at `c7b1df7e8` includes
`manager/flow_runtime_readiness.go`, `flow_attachment_resources.go`,
`agent_manager.go`, `types.go`, `runtime.go`, `runtime_claude_startup.go`,
`platform-spec.yaml` and inventories. It uses the same attachment receipt across
planned -> agents_registered -> route_installed -> timers_armed -> ready.
G's transport observation must complete before executable lifecycle admission
and readiness release, through an existing composed admission port; it cannot
mint a new readiness/attempt authority. Revalidate before every actual launch
even after a ready or restamped attempt. Proposal and shared-file census are
posted to #2496 for coordination before implementation; response is not
represented as agreement until recorded.
Coordination threads: F
https://github.com/division-sh/swarm/issues/2319#issuecomment-5976188635;
E https://github.com/division-sh/swarm/issues/2496#issuecomment-5976348525.

Implementation order after independent approval: spec/typed admission and
target projection; target-native internal modes and sealed binding; global mock
consumer migration/deletion; doctor/private test ports; crossed lifecycle and
supported Ubuntu/dual-store proof; final full qualification and proof audit.
Whichever overlapping PR lands second integrates actual owners, regenerates
generated artifacts, and runs crossed controls. No other agent's WIP is edited.

## Baseline receipts and gate status

Passed at the audited unchanged production base, not repair proof:

1. Focused native inventory, workspace selection and retired doctor-input
   controls; complete `llm/selection` and `toolgateway` packages (recorded in
   the linked pre-analysis). The earlier filtered gateway package had no
   matching tests and is explicitly not counted.
2. `go test ./internal/runtime/mcp ./internal/runtime/workspace
   ./internal/runtime/llm -run
   'TestTurnContextRegistryAdmitsOnlyExactRunlessStartupPlanProjection|TestTurnContextRegistryRejectsSameSlugSiblingCapabilityPrincipal|TestMCPToolCallLogicalIdentityRequiresProviderCallCoordinateForManagedTurns|TestExecutionTargetDistinguishesDockerHostAndEmptyContainer|TestHostManagerResolveWorkspaceCreatesScopedHostTargets|TestObserveMockRuntimeCapabilitySurfaceBindsExactInterpreterInput'
   -count=1 -timeout=3m`: all three packages passed.
3. `go test ./internal/runtime -run
   'TestValidateManagedProviderPreflightConsumesRunlessBlueprintBeforeLiveAdmission|TestValidateClaudeMCPToolsForManagedAgents_AcceptsFullyDeniedToolPlan|TestValidateClaudeMCPToolsForManagedAgents_RequiresCLIStartupProbeForMCPOnlySurface|TestValidateClaudeManagedAgentWorkspacesUsesNonExecutingCapabilityAdmission'
   -count=1 -timeout=3m`: passed. Runless/denied/probe-required/nonexecuting
   workspace controls are preserved baseline, not target-local repair proof.

These are baseline ownership controls only. No new Docker, free-doctor,
compiled mock-worker, dual-store target-local, live-provider or full-suite
closure credit exists yet. No successful paid turn is required or authorized
by the stated gate; genuine live calls would need separate authorization.

Blocking conditions: missing independent gate; unratified proposed public or
artifact port; target-local observation cannot retain exact existing authority;
new same-concept interpreter; a general multi-context/packaging/readiness
framework required; contradictory spec or unknown supported fallback;
pre-dispatch check cannot run before model without business effects;
mock tool/output identity cannot use existing effect/continuation owners.
Escalate any such condition, do not narrow claims or preserve a bypass.

Historical pre-audit outcome above: requested at audit-only head baadec1e4.
The independent coding gate was subsequently **approved** in comment 5976477595.
That approval remains the original boundary; the following newly exposed
contradictions require a bounded disposition before their implementation.

## Implementation stop-condition addendum: compensation and sandbox target

Recorded 2026-10-04 against audit-only HEAD baadec1e4 plus uncommitted #2555
implementation. No production commit, final qualification or PR is claimed.
The existing 48-row class and one-PR ceiling are unchanged; L01/L04/L07/M08
cannot be called closed from host-only or in-memory controls.

1. **Persisted activation compensation, L01/L04.**
   `agentLifecycleCoordinator.abortUnlaunchedLoopLocked` writes
   `OperationKind: "start_failed"`. Both selected-store schemas exclude that
   literal from the lifecycle operation union. A compiled lifecycle control
   reached the SQLite CHECK refusal when target-local admission failed. The
   bad literal is already present on the audited master. The existing
   `agent_lifecycle_authority.transitions.prepared_execution_publication`
   contract requires joined fail-closed compensation, not a surviving running
   durable cell. Requested repair: use the existing canonical compensation
   operation with exact generation and non-executable disposition, then prove
   persisted refusal, no publication, settled completion and legitimate fresh
   retry on both stores. No new operation kind, schema, ledger or lifecycle
   framework. The canonical operation choice is not yet ruled; production
   compensation remains unchanged and frozen. Disposition requested at
   https://github.com/division-sh/swarm/issues/2555#issuecomment-5977855287.

2. **Docker fork-chat source-target interference, L07/M08.**
   `conversationForkChatActor` retains the immutable snapshot's exact concrete
   source identity. Both `MockRuntime` and `ResolveClaudeWorkspace` fork state
   consume `ResolveWorkspaceForCapabilityAdmission`; its concrete-identity
   branch calls `resolveWorkspace(..., false)`. That branch addresses the source
   actor's existing execution container without its data-projection identity,
   so the identity-checked workspace owner removes and replaces the source
   container. `TestForkChatWorkspaceAdmissionCannotRetireSourceExecution`
   reproduced this **3/3**: one source-container removal, no returned error.
   This is an injected Docker-state owner counterexample, not real native-Docker
   transport proof. Host public fork-chat passes do not cover this mismatch.
   The governing sandbox and provider-private-state contracts require source
   noninterference and isolated fork execution, while preserving snapshot actor
   identity. Requested repair: project the exact existing fork-chat authority
   through the workspace target owner without choosing the ordinary source
   actor target; no identity erasure, fabricated normal-run authority, live-data
   materialization, new workspace framework or shared provider state. Production
   target changes on this path remain frozen pending disposition. Required proof:
   source target/labels/files/loop unchanged, exact sandbox tool restrictions,
   host and real Docker public fork-chat/continuation, refusal before model, and
   cleanup of only the sandbox resources, on SQLite and PostgreSQL.

Systematic-consumption delta: ordinary concrete activation, runless startup
admission, ordinary live data projection, selected-fork execution and fork-chat
must remain distinct exact authorities inside the existing workspace owner.
The two fork-chat consumers named above share the defective target entrance;
neither may retain it after the repair. Existing lifecycle compensation is the
only observed owner for refused activation; no duplicate owner was found.

Tracker/watchlist action: keep #2555 as the complete class tracker and refine
`transport_policy_and_surface_parity` with these counterexamples. E #2496
coordination remains required for compensation; #1779's paid half and F #2319
remain separate. No new issue, POTENTIAL_ISSUES entry or staged closure claim.
Independent transport and resource-cleanup proof may continue, but no final
qualification or merge-ready claim is appropriate while these paths are frozen.

### Independent proof delta after the stop-condition report

All receipts below exercised audit-only HEAD baadec1e4 plus the uncommitted
implementation, not a final committed head. They earn only the named partial
credit. Evidence is retained outside the worktree in
`worktrees/agent-g-2555-evidence/`; no paid provider or Telegram invocation ran.

| Receipt | Exact executed proof and scope |
| --- | --- |
| `forkchat-source-interference.log` | `TestForkChatWorkspaceAdmissionCannotRetireSourceExecution`, count 3: FAIL 3/3, one source-container removal with nil error. Workspace owner counterexample using injected Docker state, not real Docker transport credit. |
| `cli-transport-consumer-delta.log` | Managed and sandbox definition-admission matrix, native child recheck after successful observation and gateway shutdown, existing CLI fresh/resumed inventory settlement and startup UUID framing: PASS. The CLI fake-Docker fixture now understands bounded probe messages; its assertions are preserved and it earns no native-Docker credit. |
| `transport-race-controls.log` | Focused planned/disabled/empty MCP admission, exact definition matrix, native HTTP child auth/occurrence/recheck, worker outcome/identity/bounds/protocol and HTTP observation/refusal/lost-reply controls: PASS under race, count 3 in llm, mcp, toolgateway and workspace/worker. No selected-store restart or lost Docker-client join credit from these tests. |
| `compiled-host-and-lifecycle-controls.log` | `TestWorkspaceMCPCompiledHostConformance`, `TestClaudeCLIManagedLifecycleFromReleaseBinaryDefaults`, duplicate-closure negative controls and release-package public-boundary guard: PASS. Host journey crosses the real gateway and native worker; the legacy Docker emulator is not a real-container proof. |
| `doctor-listener-private-controls.log` | Free doctor gateway resource cleanup/concurrency/cancellation, explicit listener provenance, workspace-build identity and private-test controls: PASS. Partial Docker-creation fault injection is owner compensation proof, not default Linux gateway reachability. |

`git diff --check` passed. Watchlist refinement is published at swarm-docs
df97076, and E coordination update is comment 5978069634. Full/full clean-head
server2 qualification, hosted Ubuntu default Docker positive/negative, all
remaining both-store retained/lifecycle rows, lost-client join and final proof
audit remain pending. The canonical compensation and sandbox target repairs are
still unchanged pending the requested bounded disposition.

### Binding stop-condition dispositions, 5978140439

Independent ruling: https://github.com/division-sh/swarm/issues/2555#issuecomment-5978140439.
Fork-chat target repair is approved inside this one PR through the existing
workspace/container owner. Mock and Claude fork-state consume the same exact
current-authority projection; source identity stays provenance. No persistent
sandbox state or new execution contract is authorized. Existing selected-store
effect authority currency rejects a retired/foreign fork-turn group before any container
mutation; exact target cleanup cannot retire the source. The original L07/M08
host/Docker/both-store rows remain obligations, not closure claims.

Pre-model target admission uses the existing selected-store fork authority
reader through `effects.ForkChatWorkspaceCurrent`, accepting only the exact
owned prepared/executing group with a live lease. `IsExternalEffectAuthorityCurrent`
remains executing-only; no provider attempt is minted by workspace observation.
Both stores share the existing fork-field comparator, with an explicit
workspace-only phase allowance. Ordinary startup/selected/normal readers do
not gain this permission. The workspace owner then preserves source actor
provenance and derives only disposable target identity. The both-store authority
matrix proves prepared observation is not execution, all identity/fence
dimensions reject foreign input, and terminal groups refuse.

E's #2525 alone owns `self_release` compensation with `start_failed` trigger.
No duplicate edit or cherry-pick in #2555. L01/L04 and final closure depend on
the actual merge, integration and G's target-local refusal/retry proof on both
stores: exact generation, durable stopped/non-executable state, joined cleanup,
retained refusal cause and clean later retry. Independent transport work may
continue without waiting. If E changes/drops the hunk, re-gate that dependency.

### Fork-chat implementation and partial proof progress

The bounded target repair is implemented in the uncommitted #2555 worktree;
audit-only HEAD remains baadec1e4. These are WIP receipts, not exact final-head
qualification or closure of the complete 48-row class.

- Canonical target entrance: `workspace.ResolveForkChatWorkspace`, consumed by
  mock model/tool execution and Claude fork-state resolution. Source actor
  identity remains frozen provenance; exact fork turn, request, owner, fence,
  bundle and process projection select isolated disposable resources.
- Canonical currency: the existing selected-store effect owner exposes
  read-only `IsForkChatWorkspaceAuthorityCurrent` over the same fork-field
  comparator. Only current prepared/executing groups admit target observation;
  ordinary executable currency remains executing-only. Terminal cleared
  leases produce clean refusal rather than a nullable-boolean scan error.
- Container creation delegates to the existing owner without ordinary stale
  identity replacement for sandbox targets. Foreign targets refuse; partial
  start failures join exact identity-checked cleanup. Host roots and Docker
  tmpfs never inherit live source data or provider backing.
- Gateway dependency failure retains its existing classification; the failed
  fork occurrence is terminal. Reusing its request key cannot launch another
  completion. A new request consumes a distinct exact sandbox authority.

| Receipt | Executed proof, credit and limits |
| --- | --- |
| `forkchat-exact-target-race.log` | `TestForkChatWorkspaceAdmissionCannotRetireSourceExecution`, `TestForkChatWorkspaceRejectsBadAuthorityBeforeMutation` and `TestClaudeStateNamespace`, race x3: PASS. Includes malformed/missing/expired/foreign authority, source/mode mismatch, foreign named container and partial-start cleanup. Injected Docker is owner fault proof only. |
| `forkchat-consumer-crossed-controls.log` | Mock supported-surface retained restart on both stores, canonical sandbox descriptor, immutable HTTP dispatch authority, no-dispatch/observed/lost-response settlement, exact selected-store workspace currency and owned startup-evidence controls: PASS. Store-bearing leaves exercised both databases; this is not the complete retained crash matrix. |
| `forkchat-host-public-typed-refusal.log` | Public fork/chat/continuation/replay and stale-target refusal on SQLite and PostgreSQL through native host worker and real MCP HTTP: PASS. Durable and RPC envelopes match; refused occurrence has zero completions, keyed retry cannot redispatch, new occurrence succeeds. Source files and session survive, and a later public directive completes through MCP. Internal H composition, not public mock serve or paid provider credit. |
| `forkchat-server2-docker-public-typed-refusal.log` | Public Docker fork/chat/continuation/replay, stale-target and actual network-disconnect refusal on both stores: PASS. Source immutable container ID, labels, running state, sentinel file and session survive admission, execution, refusal, joined cleanup and later source directive. Frozen snapshot reads and stubbed emits remain isolated; no live fact changes or duplicate effect. Explicit bridge topology on server2 is hardened-host proof, not implicit default-Linux or hosted Ubuntu acceptance. |
| `forkchat-host-exact-lifecycle.log`, `forkchat-server2-exact-lifecycle.log` | Strengthened final WIP journeys: PASS on both stores, respectively host and real Docker. In addition to the above oracles, the exact source lifecycle identity/epoch/generation/phase/config/topology/process binding stays unchanged. The Docker H composition explicitly selects the Docker backend, not a host-config presentation with only an injected target. |
| `forkchat-server2-claude-state-offline.log` | `TestClaudeStateDockerRetentionAndRefusal`: PASS with real Docker, synthetic transcripts and network none. Claude fork-state uses the same isolated target; continuation recognizes its private head, never inherits source/provider files, removes only its tmpfs, and leaves the normal conversation readable. No successful paid Claude turn or Telegram delivery. |
| `worker-server2-interpreter-http-join.log` | `TestWorkerRealDockerIdentityReuseAndCancellationJoin` and `TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest`: PASS. Real immutable worker reuse and bounded interpreter cancellation leave no native worker; the HTTP case first reaches a deliberately held list request, then deadline refusal retains observed pre-model facts, cancels the actual request and joins the worker. This does not prove an externally killed Docker client or every retained interruption branch. Both leaves are added to the existing hosted workspace-image proof command. |
| `forkchat-spec-current.log` | Complete API specification package: PASS; governing spec includes shared exact sandbox projection, prepared observation versus execution, foreign-container refusal and joined cleanup. |

Earlier failed receipts remain evidence: the one-time create handler and the
directive's intentionally narrowed tool surface exposed fixture mistakes, not
new runtime obligations. The later source directive now uses its explicitly
delivered notice tool in an isolated harness with no external delivery channel.
No real Telegram message was sent.

Remaining: original A/M/L/Q temporal, multi-context and complete retained crash
proof not yet credited; hosted implicit-Linux Docker positive/negative; final
clean-head full/full qualification and PR proof audit. L01/L04 additionally
require #2525's actual merge and G's both-store exact activation-refusal/retry
proof. No compensation hunk was duplicated or cherry-picked. The current
receipts do not justify claiming only #2525 remains.

### Claude caller and temporal launch delta

`resolveSessionClaudeState` runs before `prepareCompletionContext`. The public
fork executor supplies the exact fork authority, but not the selected-store
controller. The Claude consumer now attaches its existing controller to that
read-only workspace observation, just as the mock consumer does; no authority
is fabricated and no completion attempt is created. The new regression fails
before the handoff repair and passes after it. The opaque provider backing
request remains the exact fork-private request on first and continued-head
resolution; a changed fence fails before binding.

| Receipt | Executed proof and limits |
| --- | --- |
| `claude-fork-controller-before.log` | `TestClaudeForkWorkspaceReceivesExistingControllerBeforeBackingMutation`: FAIL before the caller handoff repair; no current controller reaches the workspace owner. |
| `claude-fork-controller-after-race.log` | Above regression plus existing fork continuation, acknowledged cleanup, release/readback and admission-precedence controls: PASS race x3. This is caller/protocol-owner proof, not paid-provider or durable-store backing credit; the independent both-store public mock and real-Docker state rows retain their separate credit. |
| `claude-temporal-gateway-launch-race.log` | `TestClaudeEveryLaunchRechecksGatewayBeforeModelInvocation`: PASS race x3 across first/resumed/tool-result calls and stream-json/json formats. Each real native-child probe first succeeds, then the endpoint is closed. The actual launch owner refuses with observed pre-model `workspace_gateway_unreachable`, zero provider attempts, no model sentinel, unchanged session outcome and joined context-token cleanup. The protocol backing and inventory server are fixtures, not live Claude or store-settlement proof. |

### M09 counterexample: remote worker survives attached-client loss

Independent bounded disposition requested:
https://github.com/division-sh/swarm/issues/2555#issuecomment-5978888837.

The approved M09 census includes child/request join, not only loss-of-response
classification. A discriminating real-Docker test now first reaches a held
HTTP `tools/list` from the native worker, kills **only its own attached local
Docker client**, then observes `RunWorker` return. The worker and its actual HTTP
request remain live after return. The normal request-deadline control joins both.

- `worker-server2-lost-client-join-x3.log`: lost-client FAIL 3/3; request-deadline
  control PASS 3/3. `docker top` retains the exact
  `/opt/swarm/bin/swarm --internal-workspace-worker` process in every failure.
- Owner/consumer census: `workspace.RunWorker -> workerCommand -> exec.Cmd.Wait`
  currently joins the attached local client, not remote execution after that
  client's failure. Mock model, gateway probe and gateway tool-call consumers
  all share this owner. No second remote worker interpreter was found.
- Class model remains the chosen complete #2555 transport/lifetime class;
  M09 is refined, not split or credited as fixed. Tracker decision: repair
  the issue and existing transport watchlist before more remote-cleanup code.
- Requested bounded architecture: retain exact remote child ownership and join
  or dispose it through the existing worker/workspace owner even after client
  loss. Do not remove a live source/ordinary container, infer completion from
  elapsed time, turn a lost response into known pre-model success, retry a
  committed tool or introduce a process/attempt ledger, generic supervisor,
  compatibility path or new framework. Proposed spec delta binds remote
  execution cleanup separately from attached-client completion.
- Superseding independent disposition: bounded repair approved in comment
  5978927926. The existing owner may use a worker-local disconnect signal only
  with real-Docker proof of cancellation and join. The proposed ephemeral launch
  coordinate is carried in the exact child argument/handshake for read-only
  process observation, never persisted as a PID/attempt ledger. A child cannot
  execute without the parent's exact launch acknowledgement. Loss of stdin
  cancels joined work; the parent must observe the admitted worker gone or retain
  a cleanup-unproven outcome alongside the original client failure. No container
  retirement, retry, supervisor or new persistent owner is authorized. If that
  bounded mechanism cannot be proven, stop for LEAD rather than detach work.
  #2525's separate E-owned
  compensation dependency remains unchanged; neither final class closure nor
  a review-ready/full-qualification claim is made.

### M09 bounded repair and WIP proof under 5978927926

The shared `RunWorker` now binds Docker execution to an immutable container ID
and an ephemeral exact launch coordinate. The native worker sends its exact
identity/coordinate before execution and requires the matching acknowledgement.
The parent keeps stdin open; attachment loss cancels the native execution and
joins its input observer and HTTP/interpreter work. Inherited Linux stdin is
duplicated into Go's poller so closing the owned descriptor actually interrupts
its read. No PID is retained, no execution ledger/supervisor or dependency is
added, and no source/sibling container is retired.

Read-only Docker process observation checks the exact admitted binary/argument
in the exact immutable container before cleanup is claimed. Docker requires a
PID column for that observation, but its value is never execution authority or
a kill target. A failed/malformed observation or unacknowledged launch retains
cleanup-unproven uncertainty. The original client failure and independent
cleanup failure remain in the error chain. All model/probe/tool consumers get
an uncertain error on a lost launched response, not a retryable call failure;
mock settlement additionally refuses to classify unproven cleanup as no dispatch.
Cancellation has a closed native result field so logical cancellation/deadline
does not become an unclassified internal failure or hide an independent failure.

| Receipt | Executed proof and exact limits |
| --- | --- |
| `worker-server2-disconnect-final-race-x3.log` | All six `TestWorkerRealDocker*` leaves PASS race x3 (79.300s): immutable reuse/interpreter cancellation; held HTTP deadline; externally killed attached client; a committed HTTP call with lost reply and exactly one dispatch; both original killed-client and injected cleanup exit retained; held sibling request survives and completes, exact source container ID survives and can execute later. Successful joins assert actual request/process absence before return. The injected observer-failure case requires an uncertain outcome, then the proof separately owns exact eventual disposal; it never fabricates a successful join. Explicit bridge, not hosted default credit. |
| `worker-m09-owner-and-consumers-race.log` | Remote launch-acknowledgement/framing/cancellation controls, exact process-argument/header corruption, cleanup failure, native host refusal, mock observed/lost/unproven settlement and real MCP native-child authority/auth/recheck controls PASS race x3. No store-credit from the in-memory settlement fixture; the zero selected toolgateway leaves in this command earn no credit. |
| `worker-cli-framing-race-adequate-timeout.log` | Existing canonical first-turn frame and fresh/resumed malformed/missing/null/native-inventory controls PASS race x1 (59.444s). An earlier race x3 receipt exceeded its two-minute test budget and remains recorded; isolated non-race and adequately bounded race proof pass. No live Claude or native-Docker credit from the CLI framing fixture. |
| `worker-protocol-public-fork-both-stores.log` | Public host and real-Docker fork/chat, continuation, exact keyed replay/refusal/new-key retry, snapshot-only tools, source container/files/exact lifecycle/session noninterference and later source directive PASS on SQLite and PostgreSQL (113.898s). Internal H composition, explicit bridge; no public mock serve or paid-provider claim. |
| `worker-m09-authoritative-spec.log` | Complete API specification package PASS after the remote-lifetime/spec delta. |
| `worker-m09-host-call-cancellation-race.log` | Real native-host call reaches a committed/held HTTP checkpoint, is canceled, and returns only after the request retires. It retains started/unobserved uncertainty and `context.Canceled`, exactly one call, and subsequent workspace continuity: PASS race x3 (11.033s including host refusal controls). No durable-store credit from the counted test server. |
| `worker-crossed-authority-and-cleanup-race.log` | Normal/selected-fork MCP chronology, exact sibling provider-coordinate isolation/replay fencing and prepared-gateway accepted-handler retirement PASS race x3. Existing owners are preserved; no additional settlement or gateway owner. |
| `worker-m09-compiled-host-doctor-live-negative.log` | Compiled private worker entry and public host `swarm test` read/emit/store assertion, doctor static/no-credit versus actual in-container initialize, explicit-loopback refusal, and live `serve --dev` pre-model/ready refusal PASS (45.694s). Doctor Docker positive is explicit authored bridge; the paid/provider-launch sentinel remains untouched. No public Docker test/default-Linux credit from this receipt. |
| `worker-m09-retained-restart-both-stores.log` | Existing public-RPC internal-H mock emission, follow-up, exact effective frame and retained graceful restart PASS on SQLite/PostgreSQL (15.867s). This is not the complete forced interruption/retained crash matrix. |

The real-Docker matrix exposed and repaired the non-pollable input observer;
its earlier failed receipt is retained. A compiled command delta first exposed
an invalid source sync: an unanchored rsync exclusion of `swarm` also omitted
`cmd/swarm`. Corrected syncing excludes only `/swarm`, validates the source
copy by checksum, and includes a compiled private-entry regression. The real
Docker observer also forwards the exact handshake and joins its own forwarding
reader; it never simulates worker execution or tool responses. Its previous
compiled failures earn no proof credit. Hosted implicit-default and final
same-head full/full qualification remain required.

The later compiled Docker attempt correctly refused the server2 default
`mas_default` network (known host firewall restriction). `swarm test` deliberately
accepts only its private workspace backend selector, not deployment network
settings; passing a deployment `workspace.network` config cannot override that
contract. No runtime flag or topology inheritance was added. The public Docker
positive/successor-refusal proof remains unchanged and required on hosted Ubuntu,
without an override. Explicit-network doctor and internal-H Docker credit stay
separate. A test-quarantined network env accidentally forwarded to a public
command was removed, not admitted by weakening env validation.

The original 48-row gate remains complete-class/one-PR, not a new slice.
M09 has named partial WIP evidence, not final-head closure. #2525 compensation
is still E-owned and unmerged; L01/L04 need integration and both-store crossed
proof. Other original temporal/multi-context/retained-crash/hosted/qualification
and final-audit obligations remain uncredited until actually executed.

### M09 downstream consumption correction

The existing audited `Conversation.executeToolResponse` consumer converted a
typed uncertain transported call into ordinary tool feedback with a nil error.
That permits a successor model round, despite the call possibly having committed.
This is within M07/M09 and ruling 5978927926's no-replay requirement, not a new
owner or class expansion. `TestMockConversationTransportUncertaintyCannotBecomeToolFeedback`
crosses an actual native host child and HTTP call: the server counts a call then
closes its response socket. The uncertain branch failed 3/3 before correction;
the known observed-tool-failure control passed. After correction, both branches
and existing ordinary failed-emit/model-outcome controls passed race x3 (8.073s).

The Conversation now consumes the existing failure class to stop uncertain mock
transport outcomes immediately, leaving the existing completion continuation
unconsumed. It does not create a successor model frame, deliver a successful tool
result, or authorize another call. Known observed tool failures retain normal
feedback. The canonical HTTP/worker failure owners and original error causes
remain unchanged. This native-HTTP proof is store-neutral; durable recovery and
public both-store paths still require their separate matrix credit.

`TestGatewayTurnContextEffectStoryScopeSelectedStoreParity` now also includes
`native_worker_committed_lost_reply` on SQLite and PostgreSQL. The native host
worker calls the real MCP gateway and existing authored-HTTP effect owner; a
test transport drops the reply only after a successful gateway result. The
same exact occurrence is refused on replay. Durable settled effects and launched
activity remain exactly two (the original success plus the lost-reply call),
and the actual authored-HTTP server receives exactly two dispatches. Original
scope-refusal assertions remain unchanged. This and the native auth/occurrence/
temporal controls passed race x3 (60.724s), with both stores executed rather
than skipped. This is selected-store/real-HTTP integration credit, not public
CLI restart or a complete M07 recovery claim.

The original forced-restart H journey passed both backend subtests but its
aggregate failed removing sealed data projections (51.769s). That red receipt
is retained; it is not a green qualification. The parent-owned golden root
now restores only directory-removal permission after later child cleanups join.
It never changes live projection access or follows an outside symlink. The
new cleanup ordering/noninterference control passes race x3 (1.020s); the
unchanged dual-store forced-restart journey must pass as an aggregate before
receiving credit. No production lifecycle cleanup contract was changed.

### M09/M10 cancellation and response-identity delta

The native result writer checked cancellation before its typed failure owner.
A tools/call that reached a held HTTP endpoint could therefore lose possible-
commit evidence and return bare `canceled`. The corrected, bounded fixture
consumes its request body so it can actually observe the connection closing;
the first fixture timeout is retained and earns no counterexample credit.
The corrected native counterexample fails three times with exactly one call
and no typed uncertainty. The writer now preserves an existing failure envelope
first; bare cancellation keeps the existing closed cancellation field. The
parent join retains its own cancellation cause alongside an observed typed
failure, without turning observed success into failure due to later cancellation.
No result union, lifecycle owner, recovery state or schema is added.

The Conversation's inner result parser also now distinguishes malformed
post-call content/value from pre-dispatch definition mismatch. Both new
malformed-result cases failed three times before correction; possible commit
now stops the conversation, while a valid observed tool error keeps feedback.

| Receipt | Current executed evidence and limits |
| --- | --- |
| `worker-cancel-uncertainty-before-corrected.log` | Native held-call cancellation loses its typed possible-commit result: FAIL 3/3 before repair. The original unbounded/unread-body fixture timeout is retained separately and is not production attribution. |
| `worker-cancel-uncertainty-after-race.log` | Full native protocol/remote framing/launch-ack/cancellation package PASS race x3 (3.501s), preserving typed uncertainty and bare cancellation as distinct outcomes. |
| `worker-m09-docker-final-joined-race-x3.log` | All seven actual Linux Docker leaves PASS race x3 (99.207s), no skips. Adds committed-call deadline uncertainty to the original lost-client/deadline/identity/interpreter/sibling/source/error-retention controls. Ordinary successful joins still require request/process absence before return. The injected observer-failure leaf retains uncertainty and both failures, then separately owns eventual disposal. Its previous aggregate RED is retained: it incorrectly applied successful-join assertions to an intentionally unproven join. Explicit bridge, not hosted default proof. |
| `worker-m09-native-host-final-race.log` | Actual native-host held committed call cancellation, exactly one call, truthful uncertainty/cause, request join and subsequent continuity PASS race x3 (11.139s). |
| `worker-m09-conversation-final-race.log` | Native child + actual HTTP lost/malformed content/malformed value and observed-error controls PASS race x3 (14.041s), one call/token, no parent redispatch or successor call. |
| `worker-m09-settlement-classification-final-race.log` | Existing completion outcome owner preserves never-started/observed pre-model/observed-model/lost-response/cleanup-unproven dispositions PASS race x3 (1.060s); fixture is not durable-store evidence. |
| `mock-committed-tool-reopen-both-stores-race.log` | Real gateway/native child/authored HTTP call, acknowledged commit with deliberately lost reply, replay refusal before and after native-store reopen with a fresh registry/Gateway PASS race x3 on both stores (39.997s). Existing scope negatives and exact effect/activity/HTTP cardinality retained. Native selected-store reopen, not public process crash credit. |
| `worker-m09-forced-restart-both-stores-fixed.log` | Unchanged retained H forced-interruption aggregate now PASS on both stores (37.388s), including the parent-owned cleanup-order/noninterference control. The earlier cleanup RED remains recorded. No public mock serve or paid-provider credit. |
| `mock-context-preparation-native-race.log` | Two actual prepared MockRuntime factories, independent runtime bindings/Gateways/registries and native target-local initialize/list PASS race x3 (22.219s). Foreign registry and boot tokens refuse; closed primary cannot fall back to secondary; secondary remains usable; zero business calls and no leaked registration. The injected target fixture is not workspace-owner isolation or public multi-context serving proof. |
| `worker-doctor-resource-lifetime-final-race.log` | Free host probe/concurrent cleanup, occupied foreign listener preservation, partial Docker construction/cleanup faults and canceled probe PASS race x3 (7.968s). Fault Docker is a cleanup fixture, not actual container network credit. |
| `worker-m09-spec-cancellation.log` | Complete API-spec package PASS after the typed cancellation/spec delta. |

All results still bind audit-only `baadec1e4` plus captured uncommitted WIP,
not a final clean head. The original complete-class one-PR gate, E-owned #2525
dependency, hosted default-Linux proof, remaining original matrix, full/full
qualification, exact-head CI and final proof audit are unchanged. No new ruling
or reduced closure is requested for these existing-owner consumption fixes.

### Exact durable-refusal and retained consumer proof

The replay proof now requires the existing durable effect owner's exact
`external_effect_replay_refused` envelope both before and after store reopen.
Generic `isError` membership cannot earn no-replay credit: a foreign/stale
authority error would be nondiscriminating. Both stores pass this stronger
oracle with a fresh gateway/registry after reopen and unchanged effect,
activity and actual dispatch cardinality. No production path changed for
this proof refinement.

| Receipt | Executed evidence and limits |
| --- | --- |
| `worker-durable-replay-exact-reason-both-stores-race.log` | Real native-child/gateway/authored HTTP acknowledged commit, deliberately lost response, exact durable replay-refusal reason before/after native-store reopen PASS race x3 on SQLite/PostgreSQL (38.006s). Native reopen, not public process-death proof. |
| `worker-selected-fork-retained-matrix.log` | `swarm-test` PASS: `TestSelectedForkPublicChangedTargetExecutionBothStores` through the public-RPC H mock path (serveapp 11.163s), plus selected operation-replacement fault cuts, retained-source selected execution, committed-process-death recovery and selected-input execution-evidence matrices (runforkexecution 24.068s). Both stores execute; no skips. The agent-free retained-source and lower-level fault matrices retain their own credit, not mock transport or complete interrupted-effect closure. |
| `worker-reset-binding-and-context-matrix.log` | `swarm-test` PASS (16.561s): public changed-target fork after reset, successor reconciliation before execution build, retained/clear/historical served-reset controls on both stores, and unchanged multi-context live-Claude refusal. No paid-provider or public multi-context mock-serving claim. |
| `worker-toolgateway-closed-result-final-race.log` | Complete toolgateway package PASS race x3 (1.083s), preserving exact authentication, closed response shape, listener ownership and deadline controls. Store-neutral boundary proof. |

### Independent provider/worker package qualification

The broader first aggregate `worker-provider-independent-package-matrix.log`
was RED. It exposed missing workspace/probe phases in older provider fixtures,
an obsolete disconnected-MCP acceptance assertion and mock constructors lacking
their now-required native workspace. These are G-owned corrections, not the
master-red jobs attributed to #2557. The RED receipt remains retained.

The first retirement-fixture migration also incorrectly fed a workflow executor
into forensic fork chat. That is not the supported fork policy: public fork chat
validates the exact `ConversationForkChatPrepared.ValidateSandboxPolicy` union
before construction. The original selected-workflow retirement assertion now
uses its actual managed catalogue and runless startup authority, and still
requires all three retired names to disappear. No lexical retirement owner,
host-Claude permission or fallback was added to make the fixture pass.

| Receipt | Exact scope and credit |
| --- | --- |
| `worker-independent-fixture-final-race.log` | Claude fresh/resumed native/MCP/mixed/empty, process/child-identity/timeout characterization and startup missing-CLI/auth failures PASS race x3 (203.117s); selected-workflow retirement projection PASS race x3 (1.033s). Simulated Docker process selection runs the native worker and real HTTP probe but earns no real-container credit. Original outcomes, argument ordering and token-lifetime assertions remain. |
| `worker-provider-independent-package-final-matrix.log` | `swarm-test` complete llm/MCP/workspace/toolgateway/runfork packages PASS (58.035s/2.559s/21.829s/0.014s/0.014s). Original seven real-Docker M09 leaves run without skips under explicit bridge; opt-in Claude backing/reset leaves are skipped and uncredited. No whole-suite or hosted default-Linux credit. |
| `worker-native-model-captured-input-bounds-race.log` | Native host captured model bytes/digest, conflicting and changed on-target source, exact interpreter/engine/snapshot identities, wrong digest, denied import, actual output cap and rejected fuel/memory/output/entry overrides PASS race (17.878s); earlier x3 also passes. Empty PATH excludes ambient Python/Claude/Docker. Interpreter fuel/memory exhaustion remain separately owned controls, not inferred from override refusal. |

The complete 48-row gate and all remaining final-head/hosted/dependency
conditions above remain unchanged.

### Rebased source-census and fixture qualification

Implementation is committed locally and rebased onto master dd27e4374,
including C's forward repair. The exact complexity ratchet passes on ddc6e5fd5
after extraction of the existing transport stages: cognitive hotspot counts
564/189 remain 564/189, cyclomatic counts 263/54 remain 263/54. The existing
maximum cyclomatic score moves 184 -> 186 and is not hidden by a baseline edit.

`worker-rebased-owner-expanded-matrix.log` is an independently expanded
seven-package RED receipt. It identified G-owned stale public CLI and proof
inventory assertions, mock fixtures lacking required native/HTTP dependencies,
and a credential fixture borrowing invented fork permissions. No master-red
attribution or green closure is inferred. Corrected fixtures preserve exact
catalogue visibility, credential dispatch cardinality and API-provider behavior;
the mock notify-human branch now uses the native child and real HTTP gateway.
Its injected host target is not workspace-adoption or Docker proof.

The Q05/M13 source-derived ledger explicitly covers the ten added launch/write
primitives. Their consumer partition is:

| Primitive/consumer | Owner consumption and proof |
| --- | --- |
| `MockRuntime.continueSession -> executeMockCompletionWithExecutor -> executeWorkspaceMockModel -> RunWorker` (host Start and Docker exec Start) | Existing managed completion admission: Begin, initial heartbeat and committed MarkLaunched precede the exact model callback. Registration names both launch primitives; the dedicated delegated-source guard checks the chain and rejects missing admission/heartbeat/marker, wrong callback, tool mode and foreign model input. Existing managed-provider launch-boundary refusal and actual native mock frame proofs remain required execution evidence. |
| `RunWorker` identity/list observation and `workerCommand` inspection | Existing workspace dependency owner; no model-attempt authority inferred from shared transport. Target/native identity and zero-model probe controls remain the execution oracle. |
| Docker launch handshake and exact worker-exit observation | Same workspace/worker lifetime owner; protocol/real-Docker M09 proofs require exact invocation, retained failure and request/process join, never whole-container retirement. |
| `toolgateway.HTTPObservation.rpc` | Internal MCP transport, not an authored HTTP tool or external provider effect owner. Real gateway auth/occurrence, both-store exact effect replay-refusal and no redispatch proofs bind actual effect admission to the existing gateway/executor owner. |
| `ResolveForkChatWorkspace` isolated directory writes | Existing exact fork-chat workspace projection; no source/sibling writable authority. Public-H/real-Docker both-store source-continuity and foreign/stale pre-mutation refusal proofs remain binding. |
| `VerifyBuiltWorker` identity-only image check | Existing operator image-build owner; no provider call or model execution. Compiled entry/image identity/ABI controls prove the different concept. |

This is a source-census/proof correction within Q05/M13, not a new semantic
owner, effect bypass or framework. General primitive-order checks remain strict;
the sole shared-worker exception is adapter `mock_python` on the two exact
registered branches, and negative mutation proofs reject other adapters/sites.
Historical RED receipts, the expanded clean-head rerun, remaining 48-row proof,
#2525 integration, hosted default-Linux proof and full/full remain separate.

### Clean-head qualification and host cancellation counterexample

The expanded seven-package rerun at `80d4f8216` is GREEN:
`worker-rebased-owner-expanded-final-matrix.log` passes complete CLI, API-spec,
effects, tools, LLM, MCP and workspace packages. All seven real-Docker worker
leaves execute without skips under explicit bridge. The exact complexity,
source-manifest mutation, native protocol and proof-plan guards also pass.
These are package/owner receipts, not complete-class or whole-suite closure.

The fresh native race x3 aggregate at that same head is RED on the unchanged
host request-retirement oracle: default abrupt child termination can return
before the held HTTP handler retires. All seven real-Docker leaves pass their
three repetitions. M09 therefore is not closed by the earlier WIP host pass.
The bounded correction stays in the same worker owner: host cancellation asks
the native worker to cancel and join its interpreter/HTTP work, retaining the
observed possible-commit envelope, before bounded exact-child disposal. No
retry, independent lifetime owner or request-retirement assertion relaxation
is introduced. The corrected host control passes race x20 locally; a fresh
committed-head owner/public rerun is still required.

`worker-rebased-public-both-store-final-matrix.log` passes both-store public-H
host and actual-Docker fork-chat, real native lost-response/exact durable replay
controls, and activation unit controls. Its compiled host and live boot checks
are RED before execution because G's authored fixture still used the retired
input-pin mapping. The fixture now consumes the current names-only sequence;
the verifier is unchanged and no legacy reader is added. Requalification must
reach the real transport assertions; this admission failure earns no proof.

The original RED receipts remain retained. The complete 48-row class, E-owned
#2525 compensation integration, public hosted default-Linux Docker proof and
full/full exact-head qualification remain open.

### Full-run fixture and separately provisioned proof correction

The first explicit `swarm-test --full` at clean `f8721729b` is RED, retained as
`worker-f872-independent-full-pg16.log`. It planned 65 units and started 11 before
fail-fast cancellation. Neither unfinished units nor opt-in skips earn credit.
The four observed failure families are within existing fixture/proof ownership:

- Release's strict compiled-process boundary correctly rejects G's two observer
  imports of the production worker package. The observers now inspect bounded
  wire bytes and join only their own forwarding descriptors using standard
  library code. They still execute the compiled native child; the boundary guard
  is unchanged and no in-process model or tool execution is introduced.
- The Docker emulator treated the first `--mount` (now the read-only worker bind)
  as Claude's retained provider volume. It must consume its already-parsed
  `providerMount`, separately from the worker bind and disposable tmpfs. Exact
  mount and missing/foreign provider backing assertions stay strict.
- A compiled read test used a plain temporary root for sealed retained data
  projections. It now uses the existing test-owned root helper, which joins its
  children before restoring directory removal permission, never a runtime
  permission change or early unsealing.
- All four canonical Telegram memory/restart cells refused with persisted typed
  `workspace_worker_target_missing`: the test injected a workspace stub with no
  executable target. Remove that stub so the retained H composition uses real
  host workspace execution and gateway HTTP. Keep every receipt, causal identity,
  session continuity and both-recovery-policy assertion.

The full plan also tried to require PASS from opt-in Docker roots in ordinary
shards without an image. Declare these nine exact roots as separately provisioned
through the existing finite deferral owner, matching the existing Docker proof
policy. Their mandatory hosted owner remains the image-building Ubuntu
`sqlite-local-dev` job, with opt-in enabled and both exact worker/release command
families. Add an executable owner guard with missing-provisioning and lost-test
negative controls. This is proof venue correction, not Q01/M09 waiver: ordinary
skips give no transport, remote-join, default-network or class-closure credit.
Public default Linux Docker Q01, E's merged compensation L01/L04, every original
48-row obligation and fresh full/full qualification remain required.

At clean `e83a414a1`, the canonical Telegram memory/restart test passes all four
SQLite/PostgreSQL x recovery-policy cells through the actual host worker and
HTTP gateway (44.590s). The release aggregate retains a further RED: after the
provider-volume fix, resource startup now reaches an exact isolated agent
provider target that the emulator only allowed for runless system probes.
Correct the finite target/workdir validator for that actual activation-probe
variant, preserving ordinary-agent/system live-turn refusal, isolated provider
state, credential/tool/MCP admission and negative target mutations. This is
fixture classification, not a runtime fallback or paid-provider proof.
The compiled read surface, boundary, provider-mount and native-identity leaves
pass in that same RED aggregate. Its failed resource leaf remains retained;
fresh committed-head execution is required before claiming repair.

### Remaining ordinary fixture and A04/A07 proof accounting

The next full plan at clean `1e5362c8c` is also RED, not closure. Its mixed
node/agent composition used the same non-executable workspace stub and refused
startup with `workspace_worker_target_missing` on both stores. Remove the stub
at that existing H entrance; preserve the exact one agent/four node delivery,
one turn, child-versus-parent identity and public-readback assertions. SQLite's
corrected focused journey passes through the native host worker and real HTTP;
committed-head both-store proof and a fresh complete qualification remain due.

The original A04 plan also requires explicit HTTP 5xx and DNS failures. The HTTP
matrix now names both 503 before dispatch (dependency unavailable) and 503 after
a tool call (uncertain, nonretryable, exactly one call). A hermetic failing DNS
resolver proves actual resolution was attempted without timeout or alternate
endpoint execution; a numeric-address real-HTTP control still succeeds. These
store-neutral controls pass race x3, but do not substitute for native target
admission or hosted network proof.

A07's previous injected in-memory outcomes cannot earn selected-store credit.
`TestWorkspaceGatewayRefusalBeforeProviderTurnBothStores` now enters the real
managed mock conversation with exact durable delivery or directive authority,
selected session, real native host probe and real HTTP 503. Its assertions
require observed pre-model/proven-join evidence, typed dependency refusal,
zero provider attempts, turns, spend, tool dispatch or output events, unchanged
session history/turn count and no fabricated delivered/directive-success state.
It does not fabricate a completion operation before the probe or claim the
manager's directive-failure settlement; that enclosing lifecycle proof remains
separate. Safe pre-model dependency retryability is not confused with the
nonretryable possibly committed M09 outcome. The shared fixture preserves all
existing live defaults while accepting an explicit captured mock actor, and
directive admission uses that fixture's exact existing execution posture.

No production or schema/owner change is authorized by these proof corrections.
E's actual #2525 merge and crossed L01/L04, hosted default Q01, complete 48-row
mapping, fresh final-head full/full and CI/audit remain required.

### Selected native interruption and fifth qualification delta

`TestSelectedForkNativeEmitProcessDeathBothStores` now exercises the existing
selected execution owner with an authored native mock, actual host workspace
and authenticated HTTP emit. The child reaches the existing pre-activation
checkpoint only after one exact terminal emit and settled model turn, then the
parent sends SIGKILL. SQLite and PostgreSQL each retain a quiesced, paused,
unactivated execution. Two fresh recovery compositions must preserve the exact
event and business snapshots, one execution generation, one delivered agent
input, one captured call/session and one settled completion, without replay.
Selected terminal completion does not acquire the normal-delivery response
projection; its expected projection count is zero, unlike the earlier selected
nonterminal read's tool-request/post-tool pair. This is internal retained
selected-agent crash proof, not a public serve or paid-provider journey.

The clean `ec93d1898` independent full run is RED and retained. Its first
concrete failure is the served public-mock approval fixture's workspace stub:
activation correctly refuses its missing native execution target, then reaches
E's still-unmerged compensation defect. The fixture now preserves actual host
workspace construction on both stores; its deadline, approval, cardinality,
credential and settlement assertions remain unchanged. The selected Claude
OAuth framing fixture also now executes a native child against its real HTTP
gateway before its fake model calls. Docker selection/address mapping remain
explicitly simulated in that framing fixture and earn no Docker lifetime
credit. Both corrected families pass focused both-store execution.

The real-Docker public fork-chat root is separately provisioned through the
existing finite deferral owner, like the ordinary Docker emit root. Default
suite skips do not count as successful execution; its actual both-store Docker
receipts remain separately required. The conformance partition census excludes
`TestMain`, which owns native-worker entry but is not a test root; the reviewed
164 executable-root count and all real roots remain unchanged.

No runtime owner, authority, schema, compatibility path or assertion waiver is
introduced by this delta. The full 48-row gate is unchanged. Selected-agent
gateway-loss/pre-model refusal, E's integrated durable activation proof, hosted
default-Linux Q01, final qualification and final PR proof audit remain open.

### Diagnostic full and qualification ladder

The clean `77e9de2ac` full run is diagnostic-only under the user's instruction,
not final qualification because #2525 is not integrated. Its provider-alias
agent-consumers and agent-replay fixtures still selected the workspace stub;
both stores correctly refused the missing native execution target. Those two
scenarios now select the existing real host-workspace helper, while agent-free
scenarios retain their original composition. All routing, exact receipts,
authentication, replay, source ownership and cardinality assertions remain.
The focused two-scenario run and complete 14-scenario both-store root pass.

The same diagnostic unit exhausts its cumulative ten-minute package timeout
during `TestServedPublicationDirectRestartBothStores/postgres/static`, which
had run for only four seconds. The direct restart root separately passes all
six SQLite/PostgreSQL root/static/template cases. That isolated success is
not aggregate qualification or a claimed performance fix. Preserve the full
counterexample and inspect accumulated unit cost before changing partitioning;
do not relax a performance assertion or timeout to hide it.

The user's ladder is focused iteration, then core, then one final full on the
head integrating actually merged #2525. No further pre-merge full is allowed.
E's critical-path qualification has priority over G's server2 capacity. Final
L01/L04 refusal/join/new-attempt retry remains explicitly dependent on that
merge; G does not duplicate E's compensation repair.

### Independent static-doctor proof

`TestWorkspaceMCPCompiledStaticDoctorProofCredit` now exercises the actual
compiled, source-free command in text and JSON with no external executable
path. It requires the container path to be explicitly unprobed, credential
validity to remain unprobed, exactly one informational/skipped gateway finding
in JSON, and no private test session or database acquisition. Both modes pass
(4.899s, captured before commit). This closes that wording observation gap,
not the hosted Docker path, paid doctor, final-head qualification or L06.

## Independent selected-fork Docker gateway proof

`TestSelectedForkDockerGatewayTransportBothStores` adds the remaining
L06/A08 selected gateway-loss branch through public `run.fork` on an internal
retained MockOnly H composition, not public live serve. Both stores execute a
real Docker native worker against the isolated selected gateway. The reachable
control consumes the exact selected static resource in two completions and
retains the original source/domain/public-control assertions.

The loss case disconnects only the exact fork target after its activation
probe and before its provider-turn observation. It requires matching activation
and provider-turn run identity, a refused fork, one exact settled dead-letter
delivery, and identical typed `workspace_gateway_unreachable` failure bytes
through public `agent.delivery_diagnostics`. Provider attempts, agent turns and
delivered agent success must remain zero; source domain state remains unchanged.
The aggregate mutation error is not credited as the turn-failure projection.

The focused both-store root passes on server2 before this proof commit
(46.246s). Initial proof-oracle/schema mistakes and local host transport failures
remain retained. Vemew's unchanged emission and held-HTTP controls also fail
gateway reachability, so the server2 result uses explicit bridge and is not
implicit-default Ubuntu Q01 acceptance. Ordinary unprovisioned-suite skips earn
no Docker credit; the finite root deferral requires this separate execution.
No runtime code, timeout, production authority, firewall or tier is changed.
Same-head focused/race qualification still follows this commit; final E-crossed
activation and the one final full remain blocked on #2525 actual merge.

The first clean-head selection census rejected the original `RealDocker`
root spelling: the existing selected-rest regex excludes `R` and the selected
partition admits only Receiver/Required. Rename this new proof to `Docker`
so the existing selected-rest partition owns it exactly once. No plan, tier,
timeout or budget changes; the original d201f640e race receipt retains its
historical name, and new-name focused qualification follows this correction.
The new root is consumed by the existing mandatory release-rest partition;
no planner, timeout, tier, runtime or production assertion changes are made.
